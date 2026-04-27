// ============================================================
// DASHBOARD APP JS V2 - TRADING CONTROL ROOM FRONTEND
// ------------------------------------------------------------
// Mục tiêu V2:
// 1. Không hiển thị coin LOST trong watchlist.
// 2. Đọc candles + markers + tooltip từ DashboardServer V2.
// 3. Chart không fitContent liên tục khi live update để tránh bị giật/cắt.
// 4. Tooltip rõ từng nến: OHLC, volumeProxy, AI long/short, spread, funding...
// 5. Marker IN/OUT ưu tiên payload.markers từ backend.
// 6. Giao diện responsive tốt hơn cho mobile/desktop.
// ============================================================

const socket = io();

const state = {
    selectedSymbol: null,
    watchlist: [],
    symbolSnapshots: new Map(),
    candles: [],
    aiTimeline: [],
    chartMarkers: [],
    openTrades: [],
    closedTrades: [],
    logs: [],
    account: null,
    health: null,
    chart: null,
    candleSeries: null,
    tooltipEl: null,
    lastFitSymbol: null,
    userHasInteractedWithChart: false
};

// ============================================================
// 1. DOM HELPERS
// ============================================================
const $ = (id) => document.getElementById(id);

function fmtNumber(value, digits = 4) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '--';
    if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
    if (Math.abs(n) > 0 && Math.abs(n) < 0.0001) return n.toExponential(2);
    return n.toFixed(digits);
}

function fmtPrice(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '--';
    if (n >= 100) return n.toFixed(2);
    if (n >= 1) return n.toFixed(4);
    if (n >= 0.01) return n.toFixed(6);
    return n.toFixed(8);
}

function fmtPct(value, digits = 2) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '--';
    return `${(n * 100).toFixed(digits)}%`;
}

function fmtPctRaw(value, digits = 2) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '--';
    return `${n.toFixed(digits)}%`;
}

function fmtProb(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '--';
    return `${(n * 100).toFixed(1)}%`;
}

function fmtUsdt(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '--';
    return `${n.toFixed(4)} USDT`;
}

function fmtAge(ms) {
    const n = Number(ms);
    if (!Number.isFinite(n)) return '--';
    if (n < 1000) return `${Math.round(n)}ms`;
    if (n < 60_000) return `${(n / 1000).toFixed(1)}s`;
    return `${Math.floor(n / 60_000)}m${Math.floor((n % 60_000) / 1000)}s`;
}

function fmtTime(value) {
    if (!value) return '--';
    const v = Number(value);
    const d = Number.isFinite(v)
        ? new Date(v < 1e12 ? v * 1000 : v)
        : new Date(value);
    if (Number.isNaN(d.getTime())) return '--';
    return d.toLocaleTimeString('vi-VN', { hour12: false });
}

function fmtDuration(ms) {
    const n = Number(ms);
    if (!Number.isFinite(n) || n < 0) return '--';
    const s = Math.floor(n / 1000);
    const m = Math.floor(s / 60);
    const h = Math.floor(m / 60);
    if (h > 0) return `${h}h ${m % 60}m`;
    if (m > 0) return `${m}m ${s % 60}s`;
    return `${s}s`;
}

function classForStatus(status) {
    const s = String(status || '').toUpperCase();
    if (s === 'LIVE') return 'status-live';
    if (s === 'IN_TRADE') return 'status-trade';
    if (s === 'STALE') return 'status-stale';
    if (s === 'LOST') return 'status-lost';
    return 'status-wait';
}

function classForBias(bias) {
    const b = String(bias || '').toUpperCase();
    if (b === 'LONG') return 'bias-long';
    if (b === 'SHORT') return 'bias-short';
    return 'bias-wait';
}

function setText(id, value) {
    const el = $(id);
    if (el) el.textContent = value;
}

function setWidth(id, ratio) {
    const el = $(id);
    if (!el) return;
    const n = Number(ratio);
    el.style.width = `${Math.max(0, Math.min(1, Number.isFinite(n) ? n : 0)) * 100}%`;
}

function escapeHtml(str) {
    return String(str ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function getTradeTimeMs(t, open = true) {
    const value = open ? (t.openTime || t.openTs) : (t.closeTime || t.closeTs);
    if (!value) return null;
    if (value instanceof Date) return value.getTime();
    const n = Number(value);
    if (Number.isFinite(n)) return n < 1e12 ? n * 1000 : n;
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function getFeatureValue(obj, keys, fallback = 0) {
    if (!obj) return fallback;
    for (const key of keys) {
        if (obj[key] !== undefined && obj[key] !== null) return obj[key];
    }
    return fallback;
}

// ============================================================
// 2. CHART INIT + TOOLTIP
// ============================================================
function initChart() {
    const container = $('chartContainer');
    if (!container || !window.LightweightCharts) return;

    state.chart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: Math.max(container.clientHeight, 360),
        layout: {
            background: { color: '#0f172a' },
            textColor: '#cbd5e1'
        },
        grid: {
            vertLines: { color: 'rgba(148, 163, 184, 0.12)' },
            horzLines: { color: 'rgba(148, 163, 184, 0.12)' }
        },
        timeScale: {
            timeVisible: true,
            secondsVisible: true,
            borderColor: 'rgba(148, 163, 184, 0.2)',
            rightOffset: 8,
            barSpacing: 7,
            minBarSpacing: 3,
            fixLeftEdge: false,
            fixRightEdge: false
        },
        rightPriceScale: {
            borderColor: 'rgba(148, 163, 184, 0.2)',
            scaleMargins: {
                top: 0.12,
                bottom: 0.16
            }
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal
        },
        localization: {
            priceFormatter: price => fmtPrice(price)
        }
    });

    state.candleSeries = state.chart.addCandlestickSeries({
        upColor: '#22c55e',
        downColor: '#ef4444',
        borderUpColor: '#22c55e',
        borderDownColor: '#ef4444',
        wickUpColor: '#22c55e',
        wickDownColor: '#ef4444',
        priceLineVisible: true,
        lastValueVisible: true
    });

    createChartTooltip(container);

    state.chart.subscribeCrosshairMove(param => handleCrosshairMove(param, container));

    container.addEventListener('wheel', () => {
        state.userHasInteractedWithChart = true;
    }, { passive: true });

    container.addEventListener('mousedown', () => {
        state.userHasInteractedWithChart = true;
    });

    window.addEventListener('resize', () => resizeChart());
    resizeChart();
}

function resizeChart() {
    const container = $('chartContainer');
    if (!state.chart || !container) return;
    const rect = container.getBoundingClientRect();
    const width = Math.max(320, Math.floor(rect.width));
    const height = Math.max(320, Math.floor(rect.height || container.clientHeight || 420));
    state.chart.applyOptions({ width, height });
}

function createChartTooltip(container) {
    const existing = $('chartTooltip');
    if (existing) {
        state.tooltipEl = existing;
        return;
    }

    const tooltip = document.createElement('div');
    tooltip.id = 'chartTooltip';
    tooltip.className = 'chart-tooltip hidden';
    container.style.position = 'relative';
    container.appendChild(tooltip);
    state.tooltipEl = tooltip;
}

function candleByTime(time) {
    if (!time) return null;
    const target = typeof time === 'object'
        ? Date.UTC(time.year, time.month - 1, time.day) / 1000
        : Number(time);
    return state.candles.find(c => Number(c.time) === target) || null;
}

function handleCrosshairMove(param, container) {
    const tooltip = state.tooltipEl;
    if (!tooltip || !param || !param.time || !param.point) {
        if (tooltip) tooltip.classList.add('hidden');
        return;
    }

    const candle = candleByTime(param.time);
    if (!candle) {
        tooltip.classList.add('hidden');
        return;
    }

    tooltip.innerHTML = buildCandleTooltipHtml(candle);
    tooltip.classList.remove('hidden');

    const width = 245;
    const height = tooltip.offsetHeight || 150;
    const x = param.point.x + 18;
    const y = param.point.y + 18;

    const left = x + width > container.clientWidth ? param.point.x - width - 18 : x;
    const top = y + height > container.clientHeight ? param.point.y - height - 18 : y;

    tooltip.style.left = `${Math.max(8, left)}px`;
    tooltip.style.top = `${Math.max(8, top)}px`;
}

function buildCandleTooltipHtml(c) {
    const open = Number(c.open);
    const close = Number(c.close);
    const changePct = Number.isFinite(open) && open !== 0
        ? ((close - open) / open) * 100
        : 0;
    const tip = c.tooltip || {};

    return `
        <div class="tooltip-title">
            <strong>${fmtTime(c.time)}</strong>
            <span class="${c.isSynthetic ? 'text-warning' : 'text-long'}">${c.isSynthetic ? 'Synthetic' : 'Live'}</span>
        </div>
        <div class="tooltip-grid">
            <span>O</span><strong>${fmtPrice(c.open)}</strong>
            <span>H</span><strong>${fmtPrice(c.high)}</strong>
            <span>L</span><strong>${fmtPrice(c.low)}</strong>
            <span>C</span><strong>${fmtPrice(c.close)}</strong>
            <span>Change</span><strong class="${changePct >= 0 ? 'text-long' : 'text-short'}">${fmtPctRaw(changePct, 3)}</strong>
            <span>Ticks</span><strong>${fmtNumber(c.tickCount ?? c.volumeProxy, 0)}</strong>
            <span>AI Long</span><strong class="text-long">${fmtProb(tip.aiLong)}</strong>
            <span>AI Short</span><strong class="text-short">${fmtProb(tip.aiShort)}</strong>
            <span>Spread</span><strong>${fmtPct(tip.spread, 3)}</strong>
            <span>Funding</span><strong>${fmtPct(tip.fundingRate, 4)}</strong>
            <span>BTC Rel</span><strong>${fmtPctRaw(tip.btcRelativeStrength, 4)}</strong>
        </div>
        ${c.isSynthetic ? '<div class="tooltip-note">Nến nội suy để giữ chart không bị đứt.</div>' : ''}
    `;
}

function normalizeCandleForChart(c) {
    return {
        time: Number(c.time),
        open: Number(c.open),
        high: Number(c.high),
        low: Number(c.low),
        close: Number(c.close)
    };
}

function setChartCandles(candles = [], markers = []) {
    state.candles = (candles || [])
        .filter(c => c && Number.isFinite(Number(c.time)) && Number(c.open) > 0 && Number(c.high) > 0 && Number(c.low) > 0 && Number(c.close) > 0)
        .sort((a, b) => Number(a.time) - Number(b.time));

    state.chartMarkers = markers || [];

    if (!state.candleSeries) return;

    state.candleSeries.setData(state.candles.map(normalizeCandleForChart));
    updateTradeMarkers();

    if (state.selectedSymbol !== state.lastFitSymbol) {
        state.lastFitSymbol = state.selectedSymbol;
        state.userHasInteractedWithChart = false;
        state.chart?.timeScale().fitContent();
    } else if (!state.userHasInteractedWithChart) {
        state.chart?.timeScale().scrollToRealTime();
    }
}

function updateLiveCandle(candle) {
    if (!candle || !state.candleSeries) return;

    const normalized = normalizeCandleForChart(candle);
    if (!Number.isFinite(normalized.time) || normalized.close <= 0) return;

    state.candleSeries.update(normalized);

    const last = state.candles[state.candles.length - 1];
    if (!last || Number(last.time) !== normalized.time) {
        state.candles.push(candle);
    } else {
        Object.assign(last, candle);
    }

    const cutoff = Math.floor((Date.now() - 60 * 60 * 1000) / 1000);
    state.candles = state.candles.filter(c => Number(c.time) >= cutoff);

    if (!state.userHasInteractedWithChart) {
        state.chart?.timeScale().scrollToRealTime();
    }
}

function markerColor(marker) {
    if (marker.type === 'EXIT') return Number(marker.pnl) >= 0 ? '#38bdf8' : '#f97316';
    if (marker.side === 'LONG') return '#22c55e';
    if (marker.side === 'SHORT') return '#ef4444';
    return '#f59e0b';
}

function updateTradeMarkers() {
    if (!state.candleSeries || !state.selectedSymbol) return;

    let markers = [];

    if (Array.isArray(state.chartMarkers) && state.chartMarkers.length > 0) {
        markers = state.chartMarkers.map(m => ({
            time: Number(m.time),
            position: m.position || (m.side === 'LONG' ? 'belowBar' : 'aboveBar'),
            color: markerColor(m),
            shape: m.shape || (m.type === 'EXIT' ? 'circle' : (m.side === 'LONG' ? 'arrowUp' : 'arrowDown')),
            text: m.text || `${m.type || ''} ${m.side || ''}`.trim()
        }));
    } else {
        markers = buildMarkersFallbackFromTrades(state.selectedSymbol);
    }

    markers = markers
        .filter(m => Number.isFinite(Number(m.time)))
        .sort((a, b) => Number(a.time) - Number(b.time));

    if (typeof state.candleSeries.setMarkers === 'function') {
        state.candleSeries.setMarkers(markers);
    }
}

function buildMarkersFallbackFromTrades(symbol) {
    const markers = [];
    const cutoff = Date.now() - 60 * 60 * 1000;

    for (const t of state.openTrades.filter(x => x.symbol === symbol)) {
        const openMs = getTradeTimeMs(t, true);
        if (!openMs || openMs < cutoff) continue;
        markers.push({
            time: Math.floor(openMs / 1000),
            position: t.type === 'LONG' ? 'belowBar' : 'aboveBar',
            color: t.type === 'LONG' ? '#22c55e' : '#ef4444',
            shape: t.type === 'LONG' ? 'arrowUp' : 'arrowDown',
            text: `IN ${t.type} ${fmtProb(t.prob)}`
        });
    }

    for (const t of state.closedTrades.filter(x => x.symbol === symbol).slice(0, 60)) {
        const openMs = getTradeTimeMs(t, true);
        const closeMs = getTradeTimeMs(t, false);
        if (openMs && openMs >= cutoff) {
            markers.push({
                time: Math.floor(openMs / 1000),
                position: t.type === 'LONG' ? 'belowBar' : 'aboveBar',
                color: t.type === 'LONG' ? '#22c55e' : '#ef4444',
                shape: t.type === 'LONG' ? 'arrowUp' : 'arrowDown',
                text: `IN ${t.type}`
            });
        }
        if (closeMs && closeMs >= cutoff) {
            const pnl = Number(t.pnl ?? t.netPnl ?? 0);
            markers.push({
                time: Math.floor(closeMs / 1000),
                position: t.type === 'LONG' ? 'aboveBar' : 'belowBar',
                color: pnl >= 0 ? '#38bdf8' : '#f97316',
                shape: 'circle',
                text: `OUT ${fmtNumber(pnl, 3)}`
            });
        }
    }

    return markers;
}

// ============================================================
// 3. WATCHLIST
// ============================================================
function renderWatchlist() {
    const body = $('watchlistBody');
    if (!body) return;

    const search = ($('watchlistSearch')?.value || '').toUpperCase().trim();
    const cleanList = (state.watchlist || []).filter(item => String(item.status).toUpperCase() !== 'LOST');
    const items = cleanList.filter(item => !search || item.symbol.includes(search));
    setText('watchlistSubtitle', `${items.length} symbols`);

    if (items.length === 0) {
        body.innerHTML = '<tr><td colspan="6" class="empty-cell">Chưa có dữ liệu live...</td></tr>';
        return;
    }

    body.innerHTML = items.map(item => {
        const selected = item.symbol === state.selectedSymbol ? 'selected-row' : '';
        const score = Math.max(Number(item.aiLong || 0), Number(item.aiShort || 0));
        return `
            <tr class="${selected}" data-symbol="${escapeHtml(item.symbol)}">
                <td>
                    <div class="symbol-cell">
                        <strong>${escapeHtml(item.symbol)}</strong>
                        <small>${fmtAge(item.ageMs)} · ${fmtPrice(item.price)}</small>
                    </div>
                </td>
                <td><span class="bias-pill ${classForBias(item.bias)}">${escapeHtml(item.bias || 'WAIT')}</span></td>
                <td>${fmtProb(item.aiLong)}</td>
                <td>${fmtProb(item.aiShort)}</td>
                <td>${fmtProb(score)}</td>
                <td><span class="status-pill ${classForStatus(item.status)}">${escapeHtml(item.status)}</span></td>
            </tr>
        `;
    }).join('');

    body.querySelectorAll('tr[data-symbol]').forEach(row => {
        row.addEventListener('click', () => selectSymbol(row.dataset.symbol));
    });
}

async function selectSymbol(symbol) {
    const normalized = String(symbol || '').toUpperCase().trim();
    if (!normalized) return;

    state.selectedSymbol = normalized;
    state.lastFitSymbol = null;
    setText('selectedSymbolTitle', normalized);
    socket.emit('symbol:select', normalized);
    renderWatchlist();
}

// ============================================================
// 4. SYMBOL DETAIL / METRICS
// ============================================================
function renderSelectedSymbolFromState() {
    if (!state.selectedSymbol) return;
    const item = state.watchlist.find(x => x.symbol === state.selectedSymbol);
    const snapshot = state.symbolSnapshots.get(state.selectedSymbol);
    const feature = snapshot?.state?.feature || item?.feature || null;
    const prediction = snapshot?.state?.prediction || item || null;

    if (item) {
        const badge = $('selectedStatusBadge');
        setText('selectedStatusBadge', item.status || '--');
        if (badge) badge.className = `pill ${classForStatus(item.status)}`;
        setText('selectedPriceBadge', `Price: ${fmtPrice(item.price)}`);
        setText('selectedMarkBadge', `Mark: ${fmtPrice(item.markPrice)}`);
    }

    if (feature) renderMetrics(feature, snapshot?.openTrade || item?.activeTrade);
    if (prediction) renderAi(prediction.aiLong ?? prediction.long, prediction.aiShort ?? prediction.short);
}

function renderAi(longValue, shortValue) {
    const longProb = Number(longValue);
    const shortProb = Number(shortValue);

    setText('longProbText', fmtProb(longProb));
    setText('shortProbText', fmtProb(shortProb));
    setWidth('longProbBar', Number.isFinite(longProb) ? longProb : 0);
    setWidth('shortProbBar', Number.isFinite(shortProb) ? shortProb : 0);

    const biasBox = $('biasBox');
    if (!biasBox) return;

    let bias = 'WAIT';
    if (Number.isFinite(longProb) && Number.isFinite(shortProb)) {
        bias = longProb >= shortProb ? 'LONG' : 'SHORT';
    }

    biasBox.textContent = bias;
    biasBox.className = `bias-box ${classForBias(bias)}`;
}

function renderMetrics(feature, activeTrade) {
    setText('metricSpread', fmtPct(getFeatureValue(feature, ['spread_close', 'spread'], 0), 3));
    setText('metricFunding', fmtPct(getFeatureValue(feature, ['funding_rate', 'fundingRate'], 0), 4));
    setText('metricAtr', fmtPct(getFeatureValue(feature, ['ATR14', 'atr14'], 0), 3));
    setText('metricMfa', fmtNumber(getFeatureValue(feature, ['MFA', 'mfa'], 0), 4));
    setText('metricOfi', fmtNumber(getFeatureValue(feature, ['OFI', 'ofi'], 0), 4));
    setText('metricVpin', fmtNumber(getFeatureValue(feature, ['VPIN', 'vpin'], 0), 4));
    setText('metricWhale', fmtNumber(getFeatureValue(feature, ['WHALE_NET', 'whaleNet'], 0), 6));
    setText('metricBtcRel', `${fmtNumber(getFeatureValue(feature, ['btc_relative_strength', 'btcRelativeStrength'], 0), 4)}%`);
    setText('metricLiqLong', fmtNumber(getFeatureValue(feature, ['liq_long_vol', 'liqLongVol'], 0), 6));
    setText('metricLiqShort', fmtNumber(getFeatureValue(feature, ['liq_short_vol', 'liqShortVol'], 0), 6));
    setText('metricTakerBuy', fmtPct(getFeatureValue(feature, ['taker_buy_ratio', 'takerBuyRatio'], 0), 2));
    setText('metricAge', `P:${fmtAge(feature.price_age_ms)} D:${fmtAge(feature.depth_age_ms)} M:${fmtAge(feature.mark_price_age_ms)}`);

    renderActiveTradeBox(activeTrade);
}

function renderActiveTradeBox(activeTrade) {
    const box = $('activeTradeBox');
    if (!box) return;

    if (!activeTrade) {
        box.innerHTML = '<h3>Lệnh đang mở</h3><p class="muted">Chưa có lệnh cho symbol này.</p>';
        return;
    }

    const openMs = getTradeTimeMs(activeTrade, true);
    const duration = openMs ? Date.now() - openMs : activeTrade.durationMs;

    box.innerHTML = `
        <h3>Lệnh đang mở</h3>
        <div class="active-trade-grid">
            <div><span>Side</span><strong class="${activeTrade.type === 'LONG' ? 'text-long' : 'text-short'}">${escapeHtml(activeTrade.type)}</strong></div>
            <div><span>Entry</span><strong>${fmtPrice(activeTrade.executedEntryPrice ?? activeTrade.entryPrice)}</strong></div>
            <div><span>Lev</span><strong>${activeTrade.leverage || '--'}x</strong></div>
            <div><span>Prob</span><strong>${fmtProb(activeTrade.prob)}</strong></div>
            <div><span>Margin</span><strong>${fmtNumber(activeTrade.margin, 3)}</strong></div>
            <div><span>Age</span><strong>${fmtDuration(duration)}</strong></div>
        </div>
    `;
}

// ============================================================
// 5. TRADES
// ============================================================
function renderTrades() {
    renderOpenTrades();
    renderClosedTrades();
    updateTradeMarkers();
}

function renderOpenTrades() {
    const body = $('openTradesBody');
    if (!body) return;

    const openTrades = (state.openTrades || []).filter(t => String(t.status || 'OPEN').toUpperCase() === 'OPEN');
    setText('openTradesSubtitle', `${openTrades.length} open trades`);

    if (openTrades.length === 0) {
        body.innerHTML = '<tr><td colspan="8" class="empty-cell">Chưa có lệnh mở.</td></tr>';
        return;
    }

    body.innerHTML = openTrades.map(t => {
        const openMs = getTradeTimeMs(t, true);
        return `
            <tr data-symbol="${escapeHtml(t.symbol)}">
                <td><strong>${escapeHtml(t.symbol)}</strong></td>
                <td class="${t.type === 'LONG' ? 'text-long' : 'text-short'}">${escapeHtml(t.type)}</td>
                <td>${fmtPrice(t.executedEntryPrice ?? t.entryPrice)}</td>
                <td>${t.leverage || '--'}x</td>
                <td>${fmtProb(t.prob)}</td>
                <td>${fmtNumber(t.margin, 3)}</td>
                <td>${fmtDuration(openMs ? Date.now() - openMs : t.durationMs)}</td>
                <td>${escapeHtml(t.mode || '--')}</td>
            </tr>
        `;
    }).join('');

    body.querySelectorAll('tr[data-symbol]').forEach(row => row.addEventListener('click', () => selectSymbol(row.dataset.symbol)));
}

function renderClosedTrades() {
    const body = $('closedTradesBody');
    if (!body) return;
    setText('closedTradesSubtitle', `${state.closedTrades.length} recent closed trades`);

    if (state.closedTrades.length === 0) {
        body.innerHTML = '<tr><td colspan="8" class="empty-cell">Chưa có lịch sử lệnh.</td></tr>';
        return;
    }

    body.innerHTML = state.closedTrades.slice(0, 100).map(t => {
        const pnl = Number(t.pnl ?? t.netPnl ?? 0);
        return `
            <tr data-symbol="${escapeHtml(t.symbol)}">
                <td><strong>${escapeHtml(t.symbol)}</strong></td>
                <td class="${t.type === 'LONG' ? 'text-long' : 'text-short'}">${escapeHtml(t.type)}</td>
                <td>${fmtPrice(t.executedEntryPrice ?? t.entryPrice)}</td>
                <td>${fmtPrice(t.executedClosePrice ?? t.closePrice)}</td>
                <td class="${pnl >= 0 ? 'text-long' : 'text-short'}">${fmtNumber(pnl, 4)}</td>
                <td>${fmtNumber(t.roi, 2)}%</td>
                <td>${fmtDuration(t.durationMs)}</td>
                <td title="${escapeHtml(t.reason || '')}">${escapeHtml(t.reason || '--')}</td>
            </tr>
        `;
    }).join('');

    body.querySelectorAll('tr[data-symbol]').forEach(row => row.addEventListener('click', () => selectSymbol(row.dataset.symbol)));
}

// ============================================================
// 6. ACCOUNT / HEALTH / LOGS
// ============================================================
function renderAccount(account) {
    state.account = account;
    if (!account) return;

    setText('modeValue', account.mode || '--');
    setText('paperWalletValue', account.paperBalance == null ? '--' : fmtUsdt(account.paperBalance));
    setText('todayPnlValue', fmtUsdt(account.todayPnl));
    $('todayPnlValue')?.classList.toggle('text-short', Number(account.todayPnl) < 0);
    $('todayPnlValue')?.classList.toggle('text-long', Number(account.todayPnl) >= 0);
    setText('winrateValue', `${fmtNumber(account.winrateToday, 1)}%`);
}

function renderHealth(health) {
    state.health = health;
    if (!health) return;

    setText('redisStatus', `Redis: ${health.redis || '--'}`);
    setText('mongoStatus', `Mongo: ${health.mongo || '--'}`);
    setText('feedStatus', `Feed: ${health.feed || '--'} ${health.lastFeatureAgeMs != null ? fmtAge(health.lastFeatureAgeMs) : ''}`);
    setText('clientStatus', `Clients: ${health.connectedClients || 0}`);

    const ok = health.redis === 'ready' && health.mongo === 'OK' && health.feed === 'OK';
    setText('systemStatusValue', ok ? 'OK' : 'CHECK');
    $('systemStatusValue')?.classList.toggle('text-long', ok);
    $('systemStatusValue')?.classList.toggle('text-short', !ok);
}

function renderLogs(logs) {
    state.logs = logs || [];
    const list = $('logsList');
    if (!list) return;

    if (state.logs.length === 0) {
        list.innerHTML = '<div class="log-line muted">Đang chờ log hệ thống...</div>';
        return;
    }

    list.innerHTML = state.logs.slice(-120).reverse().map(log => `
        <div class="log-line">
            <span class="log-time">${fmtTime(log.ts)}</span>
            <span class="log-symbol">${escapeHtml(log.symbol || 'SYSTEM')}</span>
            <span class="log-msg">${escapeHtml(log.msg || '')}</span>
        </div>
    `).join('');
}

// ============================================================
// 7. SOCKET EVENTS
// ============================================================
socket.on('connect', () => {
    console.log('Dashboard socket connected', socket.id);
});

socket.on('disconnect', () => {
    console.warn('Dashboard socket disconnected');
});

socket.on('watchlist:update', payload => {
    state.watchlist = (payload?.symbols || []).filter(item => String(item.status || '').toUpperCase() !== 'LOST');
    renderWatchlist();
    renderSelectedSymbolFromState();

    if (!state.selectedSymbol && state.watchlist.length > 0) {
        selectSymbol(state.watchlist[0].symbol);
    }
});

socket.on('symbol:snapshot', payload => {
    if (!payload || !payload.symbol) return;
    state.symbolSnapshots.set(payload.symbol, payload);

    if (payload.symbol === state.selectedSymbol) {
        setChartCandles(payload.candles || [], payload.markers || []);
        state.aiTimeline = payload.aiTimeline || [];
        state.chartMarkers = payload.markers || [];
        renderActiveTradeBox(payload.openTrade);
        if (payload.state?.feature) renderMetrics(payload.state.feature, payload.openTrade);
        if (payload.state?.prediction) renderAi(payload.state.prediction.long, payload.state.prediction.short);
    }
});

socket.on('symbol:update', payload => {
    if (!payload || !payload.symbol) return;

    const existing = state.symbolSnapshots.get(payload.symbol) || { symbol: payload.symbol };
    existing.state = {
        ...(existing.state || {}),
        feature: payload.feature,
        prediction: payload.prediction,
        status: payload.status
    };
    existing.openTrade = payload.activeTrade || null;
    state.symbolSnapshots.set(payload.symbol, existing);

    if (payload.symbol === state.selectedSymbol) {
        updateLiveCandle(payload.candle);
        renderMetrics(payload.feature, payload.activeTrade);
        renderAi(payload.prediction?.long, payload.prediction?.short);
        setText('selectedPriceBadge', `Price: ${fmtPrice(getFeatureValue(payload.feature, ['last_price', 'lastPrice', 'price', 'close'], null))}`);
        setText('selectedMarkBadge', `Mark: ${fmtPrice(getFeatureValue(payload.feature, ['mark_price', 'markPrice'], null))}`);
        setText('selectedStatusBadge', payload.status || '--');
        const badge = $('selectedStatusBadge');
        if (badge) badge.className = `pill ${classForStatus(payload.status)}`;
    }
});

socket.on('prediction:update', payload => {
    if (!payload || !payload.symbol) return;

    const existing = state.symbolSnapshots.get(payload.symbol) || { symbol: payload.symbol };
    existing.state = {
        ...(existing.state || {}),
        prediction: payload.prediction
    };
    state.symbolSnapshots.set(payload.symbol, existing);

    if (payload.symbol === state.selectedSymbol) {
        renderAi(payload.prediction?.long, payload.prediction?.short);
    }
});

socket.on('trades:update', payload => {
    state.openTrades = payload?.open || [];
    state.closedTrades = payload?.closed || [];
    renderTrades();
    renderSelectedSymbolFromState();

    if (state.selectedSymbol) {
        socket.emit('symbol:select', state.selectedSymbol);
    }
});

socket.on('account:update', payload => {
    renderAccount(payload);
});

socket.on('system:health', payload => {
    renderHealth(payload);
});

socket.on('logs:update', payload => {
    renderLogs(payload?.logs || []);
});

// ============================================================
// 8. COMMANDS / UI EVENTS
// ============================================================
function sendCommand(cmd) {
    socket.emit('command', cmd);
}

function bindUiEvents() {
    $('watchlistSearch')?.addEventListener('input', renderWatchlist);

    $('pauseBtn')?.addEventListener('click', () => {
        sendCommand({ action: 'PAUSE_TRADING', reason: 'dashboard' });
    });

    $('resumeBtn')?.addEventListener('click', () => {
        sendCommand({ action: 'RESUME_TRADING' });
    });

    $('closeAllBtn')?.addEventListener('click', () => {
        const ok = confirm('Bạn chắc chắn muốn CLOSE_ALL toàn bộ lệnh đang mở?');
        if (ok) sendCommand({ action: 'CLOSE_ALL', reason: 'dashboard_close_all' });
    });

    $('reloadAiBtn')?.addEventListener('click', () => {
        sendCommand({ action: 'RELOAD_AI' });
    });
}

// ============================================================
// 9. BOOT
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
    initChart();
    bindUiEvents();
});
