import WebSocket from 'ws';
import redis from '../database/redis';
import { scalperEvents } from '../engine/fastScalper'; // Đã có import

// ─── CONFIG ────────────────────────────────────────────────────────────────────
const BINANCE_FAPI_BASE = 'https://fapi.binance.com';
const WS_BASE = 'wss://fstream.binance.com';
const RECONNECT_DELAY_MS = 3000;
const MAX_RECONNECT_DELAY_MS = 60_000;
const WHALE_THRESHOLD_USD = 100_000;
const WHALE_TTL_SECONDS = 3600;
const RADAR_FLUSH_INTERVAL_MS = 5_000;
const RADAR_TOP_N = 20;

// ─── INTERFACES ────────────────────────────────────────────────────────────────
type KlineRaw = [
  number, // 0: openTime
  string, // 1: open
  string, // 2: high
  string, // 3: low
  string, // 4: close
  string, // 5: volume
  number, // 6: closeTime
  string, // 7: quoteAssetVolume
  number, // 8: numberOfTrades
  string, // 9: takerBuyBaseVolume
  string, // 10: takerBuyQuoteVolume
  string  // 11: ignore
];

export interface KlineData {
  openTime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  closeTime: number;
  quoteVolume: number;
  trades: number;
}

export interface MultiTimeframeResult {
  '4h': KlineData[] | null;
  '1h': KlineData[] | null;
  '15m': KlineData[] | null;
}

export interface WhaleTrade {
  side: 'BUY' | 'SELL';
  volume: number;
  timestamp: number;
}

interface AggTradeMsg {
  e: string;   // event type
  s: string;   // symbol
  p: string;   // price
  q: string;   // quantity
  m: boolean;  // is buyer market maker
  T: number;   // trade time
}

interface MiniTicker {
  s: string;   // symbol
  c: string;   // close price
  o: string;   // open price
  h: string;   // high price
  l: string;   // low price
  v: string;   // total traded base asset volume
  q: string;   // total traded quote asset volume
}

// ─── SMART MONEY FLOW INTERFACES ───────────────────────────────────────────────

export interface MoneyFlowData {
  symbol: string;
  // Global sentiment
  longShortRatio: { longAccount: number; shortAccount: number; longShortRatio: number };
  openInterest: { oi: number; oiChangePercent: number };
  fundingRate: number;
  takerBuySellRatio: { buyVol: number; sellVol: number; ratio: number };
  // Top traders (cá mập lớn)
  topTraderLongShort: { longAccount: number; shortAccount: number; ratio: number };
  topTraderPositions: { longPosition: number; shortPosition: number; ratio: number };
  // Order book depth (sổ lệnh)
  orderBook: { bidTotal: number; askTotal: number; ratio: number; spread: number };
  // 24h stats
  tradeCount24h: number;
  timestamp: number;
}

// ─── HELPERS ───────────────────────────────────────────────────────────────────

function parseKline(raw: KlineRaw): KlineData {
  return {
    openTime: raw[0],
    open: parseFloat(raw[1]),
    high: parseFloat(raw[2]),
    low: parseFloat(raw[3]),
    close: parseFloat(raw[4]),
    volume: parseFloat(raw[5]),
    closeTime: raw[6],
    quoteVolume: parseFloat(raw[7]),
    trades: raw[8],
  };
}

function logError(tag: string, err: unknown): void {
  const message = err instanceof Error ? err.message : String(err);
  console.error(`[MarketData][${tag}] ${message}`);
}

/**
 * Exponential backoff: delay doubles mỗi lần retry, tối đa MAX_RECONNECT_DELAY_MS.
 */
function getBackoffDelay(attempt: number): number {
  const delay = RECONNECT_DELAY_MS * Math.pow(2, attempt);
  return Math.min(delay, MAX_RECONNECT_DELAY_MS);
}

// ─── 1. getMultiTimeframeKlines ────────────────────────────────────────────────

export async function getMultiTimeframeKlines(symbol: string): Promise<MultiTimeframeResult> {
  const tasks: Array<{ interval: '4h' | '1h' | '15m'; limit: number }> = [
    { interval: '4h', limit: 60 },
    { interval: '1h', limit: 20 },
    { interval: '15m', limit: 30 },
  ];

  const fetchers = tasks.map((t) =>
    fetchKlines(symbol.toUpperCase(), t.interval, t.limit),
  );

  const results = await Promise.allSettled(fetchers);

  const output: MultiTimeframeResult = { '4h': null, '1h': null, '15m': null };

  results.forEach((result, idx) => {
    const tf = tasks[idx].interval;
    if (result.status === 'fulfilled') {
      output[tf] = result.value;
      console.log(`[MarketData] ${symbol} ${tf}: ${result.value.length} klines fetched`);
    } else {
      logError(`Kline:${tf}`, result.reason);
    }
  });

  return output;
}

async function fetchKlines(
  symbol: string,
  interval: string,
  limit: number,
): Promise<KlineData[]> {
  const url = `${BINANCE_FAPI_BASE}/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&limit=${limit}`;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10_000);

  try {
    const res = await fetch(url, { signal: controller.signal });

    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status}: ${body}`);
    }

    const raw = (await res.json()) as KlineRaw[];

    return raw.map(parseKline);
  } finally {
    clearTimeout(timeout);
  }
}

// ─── 2. monitorWhaleTrades ─────────────────────────────────────────────────────

export function monitorWhaleTrades(symbols: string[]): () => void {
  let stopped = false;
  const activeConnections = new Map<string, WebSocket>();

  for (const sym of symbols) {
    const symLower = sym.toLowerCase();
    const ws = connectWhaleStream(symLower, 0, activeConnections, () => stopped);
    activeConnections.set(symLower, ws);
  }

  return () => {
    stopped = true;
    for (const [, ws] of activeConnections) {
      ws.removeAllListeners();
      ws.close();
    }
    activeConnections.clear();
    console.log('[MarketData][Whale] All connections closed');
  };
}

function connectWhaleStream(
  symbolLower: string,
  attempt: number = 0,
  activeConnections?: Map<string, WebSocket>,
  isStopped?: () => boolean,
): WebSocket {
  const url = `${WS_BASE}/public/ws/${symbolLower}@aggTrade`;
  console.log(`[MarketData][Whale] Connecting ${symbolLower}...`);

  const ws = new WebSocket(url);

  ws.on('open', () => {
    attempt = 0;
    console.log(`[MarketData][Whale] Connected: ${symbolLower}@aggTrade`);
  });

  ws.on('message', async (raw: WebSocket.RawData) => {
    try {
      const trade: AggTradeMsg = JSON.parse(raw.toString());

      const price = parseFloat(trade.p);
      const qty = parseFloat(trade.q);
      const value = price * qty;

      if (value > WHALE_THRESHOLD_USD) {
        const symbol = trade.s.toUpperCase();
        const side: 'BUY' | 'SELL' = trade.m ? 'SELL' : 'BUY';
        const entry: WhaleTrade = {
          side,
          volume: value,
          timestamp: trade.T,
        };

        const key = `WHALE:${symbol}`;

        const pipeline = redis.pipeline();
        pipeline.lpush(key, JSON.stringify(entry));
        pipeline.expire(key, WHALE_TTL_SECONDS);
        await pipeline.exec();

        console.log(
          `[MarketData][Whale] 🐋 ${symbol} ${side} $${value.toLocaleString('en-US', { maximumFractionDigits: 0 })} @ ${price}`,
        );

        // 🟢 BỔ SUNG: Bắn tín hiệu Fast Scalp bằng Event Emitter
        scalperEvents.emit('WHALE_DETECTED', {
          symbol: symbol,
          side: side,
          usdVolume: value,
          price: price
        });
      }
    } catch (err) {
      logError('Whale:parse', err);
    }
  });

  ws.on('close', (code: number) => {
    if (isStopped?.()) return;
    const delay = getBackoffDelay(attempt);
    console.warn(
      `[MarketData][Whale] ${symbolLower} closed (code: ${code}). Reconnecting in ${(delay / 1000).toFixed(1)}s...`,
    );
    setTimeout(() => {
      if (isStopped?.()) return;
      const newWs = connectWhaleStream(symbolLower, attempt + 1, activeConnections, isStopped);
      activeConnections?.set(symbolLower, newWs);
    }, delay);
  });

  ws.on('error', (err: Error) => {
    logError(`Whale:${symbolLower}`, err);
    ws.close();
  });

  return ws;
}

// ─── 3. getRadarData ───────────────────────────────────────────────────────────

export function getRadarData(): () => void {
  const REDIS_KEY = 'RADAR:HOT_COINS';
  let latestTop: { symbol: string; price: number; quoteVolume: number }[] = [];
  let flushTimer: ReturnType<typeof setInterval> | null = null;
  let stopped = false;
  let currentWs: WebSocket | null = null;

  currentWs = connectRadarStream(
    (tickers: MiniTicker[]) => {
      const usdtPairs = tickers
        .filter((t) => t.s.endsWith('USDT'))
        .map((t) => {
          const close = parseFloat(t.c);
          const open = parseFloat(t.o || t.c);
          const changePercent = open > 0 ? ((close - open) / open) * 100 : 0;
          return {
            symbol: t.s,
            price: close,
            quoteVolume: parseFloat(t.q),
            priceChangePercent: parseFloat(changePercent.toFixed(2)),
            high24h: parseFloat(t.h || t.c),
            low24h: parseFloat(t.l || t.c),
          };
        })
        .sort((a, b) => b.quoteVolume - a.quoteVolume)
        .slice(0, RADAR_TOP_N);

      if (usdtPairs.length > 0) {
        latestTop = usdtPairs;
      }
    },
    0,
    (ws) => { currentWs = ws; },
    () => stopped,
  );

  flushTimer = setInterval(async () => {
    if (latestTop.length === 0) return;
    try {
      await redis.set(REDIS_KEY, JSON.stringify(latestTop));
      console.log(
        `[MarketData][Radar] Top ${RADAR_TOP_N} saved: ${latestTop.map((c) => c.symbol).join(', ')}`,
      );
    } catch (err) {
      logError('Radar:flush', err);
    }
  }, RADAR_FLUSH_INTERVAL_MS);

  return () => {
    stopped = true;
    if (flushTimer) clearInterval(flushTimer);
    if (currentWs) {
      currentWs.removeAllListeners();
      currentWs.close();
    }
    console.log('[MarketData][Radar] Stopped');
  };
}

function connectRadarStream(
  onTickers: (tickers: MiniTicker[]) => void,
  attempt: number = 0,
  onNewWs?: (ws: WebSocket) => void,
  isStopped?: () => boolean,
): WebSocket {
  const url = `${WS_BASE}/market/ws/!miniTicker@arr`;
  console.log('[MarketData][Radar] Connecting...');

  const ws = new WebSocket(url);

  ws.on('open', () => {
    attempt = 0;
    console.log('[MarketData][Radar] Connected to !miniTicker@arr');
  });

  ws.on('message', (raw: WebSocket.RawData) => {
    try {
      const tickers: MiniTicker[] = JSON.parse(raw.toString());
      onTickers(tickers);
    } catch (err) {
      logError('Radar:parse', err);
    }
  });

  ws.on('close', (code: number) => {
    if (isStopped?.()) return;
    const delay = getBackoffDelay(attempt);
    console.warn(
      `[MarketData][Radar] Closed (code: ${code}). Reconnecting in ${(delay / 1000).toFixed(1)}s...`,
    );
    setTimeout(() => {
      if (isStopped?.()) return;
      const newWs = connectRadarStream(onTickers, attempt + 1, onNewWs, isStopped);
      onNewWs?.(newWs);
    }, delay);
  });

  ws.on('error', (err: Error) => {
    logError('Radar:ws', err);
    ws.close();
  });

  return ws;
}

// ─── 4. getMoneyFlowData ──────────────────────────────────────────────────────

export async function getMoneyFlowData(symbol: string): Promise<MoneyFlowData> {
  const result: MoneyFlowData = {
    symbol,
    longShortRatio: { longAccount: 50, shortAccount: 50, longShortRatio: 1 },
    openInterest: { oi: 0, oiChangePercent: 0 },
    fundingRate: 0,
    takerBuySellRatio: { buyVol: 0, sellVol: 0, ratio: 1 },
    topTraderLongShort: { longAccount: 50, shortAccount: 50, ratio: 1 },
    topTraderPositions: { longPosition: 50, shortPosition: 50, ratio: 1 },
    orderBook: { bidTotal: 0, askTotal: 0, ratio: 1, spread: 0 },
    tradeCount24h: 0,
    timestamp: Date.now(),
  };

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10_000);

  try {
    const [lsRes, oiRes, frRes, takerRes, topAccRes, topPosRes, depthRes, tickerRes] = await Promise.allSettled([
      fetch(
        `${BINANCE_FAPI_BASE}/futures/data/globalLongShortAccountRatio?symbol=${encodeURIComponent(symbol)}&period=5m&limit=1`,
        { signal: controller.signal },
      ),
      fetch(
        `${BINANCE_FAPI_BASE}/futures/data/openInterestHist?symbol=${encodeURIComponent(symbol)}&period=5m&limit=2`,
        { signal: controller.signal },
      ),
      fetch(
        `${BINANCE_FAPI_BASE}/fapi/v1/fundingRate?symbol=${encodeURIComponent(symbol)}&limit=1`,
        { signal: controller.signal },
      ),
      fetch(
        `${BINANCE_FAPI_BASE}/futures/data/takerlongshortRatio?symbol=${encodeURIComponent(symbol)}&period=5m&limit=1`,
        { signal: controller.signal },
      ),
      fetch(
        `${BINANCE_FAPI_BASE}/futures/data/topLongShortAccountRatio?symbol=${encodeURIComponent(symbol)}&period=5m&limit=1`,
        { signal: controller.signal },
      ),
      fetch(
        `${BINANCE_FAPI_BASE}/futures/data/topLongShortPositionRatio?symbol=${encodeURIComponent(symbol)}&period=5m&limit=1`,
        { signal: controller.signal },
      ),
      fetch(
        `${BINANCE_FAPI_BASE}/fapi/v1/depth?symbol=${encodeURIComponent(symbol)}&limit=20`,
        { signal: controller.signal },
      ),
      fetch(
        `${BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr?symbol=${encodeURIComponent(symbol)}`,
        { signal: controller.signal },
      ),
    ]);

    if (lsRes.status === 'fulfilled' && lsRes.value.ok) {
      const data = await lsRes.value.json() as Array<{ longAccount: string; shortAccount: string; longShortRatio: string }>;
      if (data.length > 0) {
        result.longShortRatio = {
          longAccount: parseFloat(data[0].longAccount) * 100,
          shortAccount: parseFloat(data[0].shortAccount) * 100,
          longShortRatio: parseFloat(data[0].longShortRatio),
        };
      }
    }

    if (oiRes.status === 'fulfilled' && oiRes.value.ok) {
      const data = await oiRes.value.json() as Array<{ sumOpenInterest: string; sumOpenInterestValue: string }>;
      if (data.length >= 1) {
        const currentOI = parseFloat(data[0].sumOpenInterestValue);
        result.openInterest.oi = currentOI;
        if (data.length >= 2) {
          const prevOI = parseFloat(data[1].sumOpenInterestValue);
          result.openInterest.oiChangePercent = prevOI > 0 ? ((currentOI - prevOI) / prevOI) * 100 : 0;
        }
      }
    }

    if (frRes.status === 'fulfilled' && frRes.value.ok) {
      const data = await frRes.value.json() as Array<{ fundingRate: string }>;
      if (data.length > 0) {
        result.fundingRate = parseFloat(data[0].fundingRate) * 100;
      }
    }

    if (takerRes.status === 'fulfilled' && takerRes.value.ok) {
      const data = await takerRes.value.json() as Array<{ buySellRatio: string; buyVol: string; sellVol: string }>;
      if (data.length > 0) {
        result.takerBuySellRatio = {
          buyVol: parseFloat(data[0].buyVol),
          sellVol: parseFloat(data[0].sellVol),
          ratio: parseFloat(data[0].buySellRatio),
        };
      }
    }

    if (topAccRes.status === 'fulfilled' && topAccRes.value.ok) {
      const data = await topAccRes.value.json() as Array<{ longAccount: string; shortAccount: string; longShortRatio: string }>;
      if (data.length > 0) {
        result.topTraderLongShort = {
          longAccount: parseFloat(data[0].longAccount) * 100,
          shortAccount: parseFloat(data[0].shortAccount) * 100,
          ratio: parseFloat(data[0].longShortRatio),
        };
      }
    }

    if (topPosRes.status === 'fulfilled' && topPosRes.value.ok) {
      const data = await topPosRes.value.json() as Array<{ longAccount: string; shortAccount: string; longShortRatio: string }>;
      if (data.length > 0) {
        result.topTraderPositions = {
          longPosition: parseFloat(data[0].longAccount) * 100,
          shortPosition: parseFloat(data[0].shortAccount) * 100,
          ratio: parseFloat(data[0].longShortRatio),
        };
      }
    }

    if (depthRes.status === 'fulfilled' && depthRes.value.ok) {
      const data = await depthRes.value.json() as { bids: string[][]; asks: string[][] };
      if (data.bids && data.asks) {
        const bidTotal = data.bids.reduce((sum, [price, qty]) => sum + parseFloat(price) * parseFloat(qty), 0);
        const askTotal = data.asks.reduce((sum, [price, qty]) => sum + parseFloat(price) * parseFloat(qty), 0);
        const bestBid = data.bids.length > 0 ? parseFloat(data.bids[0][0]) : 0;
        const bestAsk = data.asks.length > 0 ? parseFloat(data.asks[0][0]) : 0;
        const spread = bestBid > 0 ? ((bestAsk - bestBid) / bestBid) * 100 : 0;
        result.orderBook = {
          bidTotal,
          askTotal,
          ratio: askTotal > 0 ? bidTotal / askTotal : 1,
          spread,
        };
      }
    }

    if (tickerRes.status === 'fulfilled' && tickerRes.value.ok) {
      const data = await tickerRes.value.json() as { count: number };
      if (data.count) {
        result.tradeCount24h = data.count;
      }
    }
  } catch (err) {
    logError('MoneyFlow', err);
  } finally {
    clearTimeout(timeout);
  }

  return result;
}