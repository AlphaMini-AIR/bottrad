import WebSocket from 'ws';
import redis from '../database/redis';

const SCANNER_WS_URL = 'wss://fstream.binance.com/market/ws/!miniTicker@arr';
const RECONNECT_DELAY_MS = 3000;
const REDIS_KEY = 'SCANNER:HOT_COINS';
const FLUSH_INTERVAL_MS = 3000;

interface MiniTicker {
  s: string;  // symbol
  c: string;  // close price
  q: string;  // quote asset volume 24h
}

interface HotCoin {
  symbol: string;
  price: number;
  quoteVolume: number;
}

let latestTop5: HotCoin[] = [];
let scannerWs: WebSocket | null = null;
let stopped = false;
let flushInterval: ReturnType<typeof setInterval> | null = null;

function connectScanner(): void {
  if (stopped) return;
  console.log(`[Scanner] Connecting to ${SCANNER_WS_URL}...`);
  scannerWs = new WebSocket(SCANNER_WS_URL);

  scannerWs.on('open', () => {
    console.log('[Scanner] Connected to All Market Mini Tickers');
  });

  scannerWs.on('message', (raw: WebSocket.RawData) => {
    try {
      const tickers: MiniTicker[] = JSON.parse(raw.toString());

      // Lọc cặp USDT, sắp xếp theo quote volume giảm dần, lấy Top 5
      const usdtPairs = tickers
        .filter((t) => t.s.endsWith('USDT'))
        .map((t) => ({
          symbol: t.s,
          price: parseFloat(t.c),
          quoteVolume: parseFloat(t.q),
        }))
        .sort((a, b) => b.quoteVolume - a.quoteVolume)
        .slice(0, 5);

      if (usdtPairs.length > 0) {
        latestTop5 = usdtPairs;
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      console.error('[Scanner] Parse error:', message);
    }
  });

  scannerWs.on('close', (code: number) => {
    if (stopped) return;
    console.warn(`[Scanner] Connection closed (code: ${code}). Reconnecting in ${RECONNECT_DELAY_MS / 1000}s...`);
    setTimeout(connectScanner, RECONNECT_DELAY_MS);
  });

  scannerWs.on('error', (err: Error) => {
    console.error('[Scanner] Error:', err.message);
    scannerWs?.close();
  });
}

function startRedisFlush(): void {
  flushInterval = setInterval(async () => {
    if (latestTop5.length === 0) return;
    try {
      await redis.set(REDIS_KEY, JSON.stringify(latestTop5));
      console.log(`[Scanner] Top 5 saved to Redis: ${latestTop5.map((c) => c.symbol).join(', ')}`);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      console.error('[Scanner] Redis flush error:', message);
    }
  }, FLUSH_INTERVAL_MS);
}

export function startMarketScanner(): () => void {
  stopped = false;
  connectScanner();
  startRedisFlush();
  console.log('[Scanner] Market Scanner initialized');

  // Cleanup function
  return () => {
    stopped = true;
    if (flushInterval) {
      clearInterval(flushInterval);
      flushInterval = null;
    }
    if (scannerWs) {
      scannerWs.removeAllListeners();
      scannerWs.close();
      scannerWs = null;
    }
    console.log('[Scanner] Market Scanner stopped');
  };
}
