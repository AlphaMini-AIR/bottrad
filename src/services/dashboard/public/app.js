// ============================================================
// DASHBOARD APP JS - TRADING CONTROL ROOM FRONTEND
// ------------------------------------------------------------
// Vai trò:
// 1. Kết nối Socket.IO với DashboardServer.
// 2. Render watchlist coin đang theo dõi.
// 3. Vẽ live candle chart bằng Lightweight Charts.
// 4. Hiển thị AI long/short probability theo symbol đang chọn.
// 5. Render thông số coin, lệnh open/closed, account, health, logs.
// 6. Gửi command an toàn: PAUSE, RESUME, CLOSE_ALL, RELOAD_AI.
// ============================================================

const socket = io();

const state = {
    selectedSymbol: null,
    watchlist: [],
    symbolSnapshots: new Map(),
    candles: [],
    aiTimeline: [],
    openTrades: [],
    closedTrades: [],
    logs: [],
    account: null,
    health: null,
    chart: null,
    candleSeries: null,
    markerMap: new Map()
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

function fmtPct(value, digits = 2) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '--';
    return `${(n * 100).toFixed(digits)}%`;
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
    return `${(n / 1000).toFixed(1)}s`;
}

function fmtTime(value) {
    if (!value) return '--';
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return '--';
    return d.toLocaleTimeString('vi-VN', { hour12: false });
}

function classForStatus(status) {
    if (status === 'LIVE') return 'status-live';
    if (status === 'IN_TRADE') return 'status-trade';
    if (status === 'STALE') return 'status-stale';
    if (status === 'LOST') return 'status-lost';
    return 'status-wait';
}

function classForBias(bias) {
    if (bias === 'LONG') return 'bias-long';
    if (bias === 'SHORT') return 'bias-short';
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

// ============================================================
// 2. CHART INIT
// ============================================================
function initChart() {
    const container = $('chartContainer');
    if (!container || !window.LightweightCharts) return;

    state.chart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: container.clientHeight,
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
            borderColor: 'rgba(148, 163, 184, 0.2)'
        },
        rightPriceScale: {
            borderColor: 'rgba(148, 163, 184, 0.2)'
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal
        }
    });

    state.candleSeries = state.chart.addCandlestickSeries({
        upColor: '#22c55e',
        downColor: '#ef4444',
        borderUpColor: '#22c55e',
        borderDownColor: '#ef4444',
        wickUpColor: '#22c55e',
        wickDownColor: '#ef4444'
    });

    window.addEventListener('resize', () => {
        if (!state.chart || !container) return;
        state.chart.applyOptions({ width: container.clientWidth, height: container.clientHeight });
    });
}

function setChartCandles(candles = []) {
    state.candles = candles || [];
    if (!state.candleSeries) return;
    state.candleSeries.setData(state.candles.map(c => ({
        time: c.time,
        open: Number(c.open),
        high: Number(c.high),
        low: Number(c.low),
        close: Number(c.close)
    })));
    updateTradeMarkers();
    state.chart?.timeScale().fitContent();
}

function updateLiveCandle(candle) {
    if (!candle || !state.candleSeries) return;
    const item = {
        time: candle.time,
        open: Number(candle.open),
        high: Number(candle.high),
        low: Number(candle.low),
        close: Number(candle.close)
    };
    state.candleSeries.update(item);

    const last = state.candles[state.candles.length - 1];
    if (!last || last.time !== item.time) state.candles.push(item);
    else Object.assign(last, item);
}

function updateTradeMarkers() {
    if (!state.candleSeries || !state.selectedSymbol) return;

    const markers = [];
    const symbol = state.selectedSymbol;

    for (const t of state.openTrades.filter(x => x.symbol === symbol)) {
        if (t.openTs) {
            markers.push({
                time: Math.floor(Number(t.openTs) / 1000),
                position: t.type === 'LONG' ? 'belowBar' : 'aboveBar',
                color: t.type === 'LONG' ? '#22c55e' : '#ef4444',
                shape: t.type === 'LONG' ? 'arrowUp' : 'arrowDown',
                text: `OPEN ${t.type} ${fmtProb(t.prob)}`
            });
        }
    }

    for (const t of state.closedTrades.filter(x => x.symbol === symbol).slice(0, 30)) {
        if (t.openTs) {
            markers.push({
                time: Math.floor(Number(t.openTs) / 1000),
                position: t.type === 'LONG' ? 'belowBar' : 'aboveBar',
                color: t.type === 'LONG' ? '#22c55e' : '#ef4444',
                shape: t.type === 'LONG' ? 'arrowUp' : 'arrowDown',
                text: `IN ${t.type}`
            });
        }
        if (t.closeTs) {
            const pnl = Number(t.pnl ?? t.netPnl ?? 0);
            markers.push({
                time: Math.floor(Number(t.closeTs) / 1000),
                position: 'aboveBar',
                color: pnl >= 0 ? '#38bdf8' : '#f97316',
                shape: 'circle',
                text: `OUT ${fmtNumber(pnl, 3)}`
            });
        }
    }

    markers.sort((a, b) => a.time - b.time);
    state.candleSeries.setMarkers(markers);
}

// ============================================================
// 3. WATCHLIST
// ============================================================
function renderWatchlist() {
    const body = $('watchlistBody');
    if (!body) return;

    const search = ($('watchlistSearch')?.value || '').toUpperCase().trim();
    const items = state.watchlist.filter(item => !search || item.symbol.includes(search));
    setText('watchlistSubtitle', `${items.length} symbols`);

    if (items.length === 0) {
        body.innerHTML = '<tr><td colspan="6" class="empty-cell">Chưa có dữ liệu feature...</td></tr>';
        return;
    }

    body.innerHTML = items.map(item => {
        const selected = item.symbol === state.selectedSymbol ? 'selected-row' : '';
        return `
            <tr class="${selected}" data-symbol="${escapeHtml(item.symbol)}">
                <td>
                    <div class="symbol-cell">
                        <strong>${escapeHtml(item.symbol)}</strong>
                        <small>${fmtAge(item.ageMs)}</small>
                    </div>
                </td>
                <td><span class="bias-pill ${classForBias(item.bias)}">${escapeHtml(item.bias || 'WAIT')}</span></td>
                <td>${fmtProb(item.aiLong)}</td>
                <td>${fmtProb(item.aiShort)}</td>
                <td>${fmtPct(item.spread, 3)}</td>
                <td><span class="status-pill ${classForStatus(item.status)}">${escapeHtml(item.status)}</span></td>
            </tr>
        `;
    }).join('');

    body.querySelectorAll('tr[data-symbol]').forEach(row => {
        row.addEventListener('click', () => selectSymbol(row.dataset.symbol));
    });
}

async function selectSymbol(symbol) {
    state.selectedSymbol = symbol;
    setText('selectedSymbolTitle', symbol);
    socket.emit('symbol:select', symbol);
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
        setText('selectedStatusBadge', item.status || '--');
        $('selectedStatusBadge').className = `pill ${classForStatus(item.status)}`;
        setText('selectedPriceBadge', `Price: ${fmtNumber(item.price, 8)}`);
        setText('selectedMarkBadge', `Mark: ${fmtNumber(item.markPrice, 8)}`);
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
    setText('metricSpread', fmtPct(feature.spread_close, 3));
    setText('metricFunding', fmtPct(feature.funding_rate, 4));
    setText('metricAtr', fmtPct(feature.ATR14, 3));
    setText('metricMfa', fmtNumber(feature.MFA, 4));
    setText('metricOfi', fmtNumber(feature.OFI, 4));
    setText('metricVpin', fmtNumber(feature.VPIN, 4));
    setText('metricWhale', fmtNumber(feature.WHALE_NET, 6));
    setText('metricBtcRel', `${fmtNumber(feature.btc_relative_strength, 4)}%`);
    setText('metricLiqLong', fmtNumber(feature.liq_long_vol, 6));
    setText('metricLiqShort', fmtNumber(feature.liq_short_vol, 6));
    setText('metricTakerBuy', fmtPct(feature.taker_buy_ratio, 2));
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

    box.innerHTML = `
        <h3>Lệnh đang mở</h3>
        <div class="active-trade-grid">
            <div><span>Side</span><strong class="${activeTrade.type === 'LONG' ? 'text-long' : 'text-short'}">${activeTrade.type}</strong></div>
            <div><span>Entry</span><strong>${fmtNumber(activeTrade.entryPrice, 8)}</strong></div>
            <div><span>Lev</span><strong>${activeTrade.leverage || '--'}x</strong></div>
            <div><span>Prob</span><strong>${fmtProb(activeTrade.prob)}</strong></div>
            <div><span>Margin</span><strong>${fmtNumber(activeTrade.margin, 3)}</strong></div>
            <div><span>Mode</span><strong>${activeTrade.mode || '--'}</strong></div>
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
    setText('openTradesSubtitle', `${state.openTrades.length} open trades`);

    if (state.openTrades.length === 0) {
        body.innerHTML = '<tr><td colspan="7" class="empty-cell">Chưa có lệnh mở.</td></tr>';
        return;
    }

    body.innerHTML = state.openTrades.map(t => `
        <tr data-symbol="${escapeHtml(t.symbol)}">
            <td><strong>${escapeHtml(t.symbol)}</strong></td>
            <td class="${t.type === 'LONG' ? 'text-long' : 'text-short'}">${escapeHtml(t.type)}</td>
            <td>${fmtNumber(t.entryPrice, 8)}</td>
            <td>${t.leverage || '--'}x</td>
            <td>${fmtProb(t.prob)}</td>
            <td>${fmtNumber(t.margin, 3)}</td>
            <td>${escapeHtml(t.mode || '--')}</td>
        </tr>
    `).join('');

    body.querySelectorAll('tr[data-symbol]').forEach(row => row.addEventListener('click', () => selectSymbol(row.dataset.symbol)));
}

function renderClosedTrades() {
    const body = $('closedTradesBody');
    if (!body) return;
    setText('closedTradesSubtitle', `${state.closedTrades.length} recent closed trades`);

    if (state.closedTrades.length === 0) {
        body.innerHTML = '<tr><td colspan="7" class="empty-cell">Chưa có lịch sử lệnh.</td></tr>';
        return;
    }

    body.innerHTML = state.closedTrades.slice(0, 100).map(t => {
        const pnl = Number(t.pnl ?? t.netPnl ?? 0);
        return `
            <tr data-symbol="${escapeHtml(t.symbol)}">
                <td><strong>${escapeHtml(t.symbol)}</strong></td>
                <td class="${t.type === 'LONG' ? 'text-long' : 'text-short'}">${escapeHtml(t.type)}</td>
                <td>${fmtNumber(t.entryPrice, 8)}</td>
                <td>${fmtNumber(t.closePrice, 8)}</td>
                <td class="${pnl >= 0 ? 'text-long' : 'text-short'}">${fmtNumber(pnl, 4)}</td>
                <td>${fmtNumber(t.roi, 2)}%</td>
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
    state.watchlist = payload?.symbols || [];
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
        setChartCandles(payload.candles || []);
        state.aiTimeline = payload.aiTimeline || [];
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
    existing.openTrade = payload.activeTrade || existing.openTrade || null;
    state.symbolSnapshots.set(payload.symbol, existing);

    if (payload.symbol === state.selectedSymbol) {
        updateLiveCandle(payload.candle);
        renderMetrics(payload.feature, payload.activeTrade);
        renderAi(payload.prediction?.long, payload.prediction?.short);
        setText('selectedPriceBadge', `Price: ${fmtNumber(payload.feature?.last_price, 8)}`);
        setText('selectedMarkBadge', `Mark: ${fmtNumber(payload.feature?.mark_price, 8)}`);
        setText('selectedStatusBadge', payload.status || '--');
        $('selectedStatusBadge').className = `pill ${classForStatus(payload.status)}`;
    }
});

socket.on('prediction:update', payload => {
    if (!payload || !payload.symbol) return;
    if (payload.symbol === state.selectedSymbol) {
        renderAi(payload.prediction?.long, payload.prediction?.short);
    }
});

socket.on('trades:update', payload => {
    state.openTrades = payload?.open || [];
    state.closedTrades = payload?.closed || [];
    renderTrades();
    renderSelectedSymbolFromState();
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
