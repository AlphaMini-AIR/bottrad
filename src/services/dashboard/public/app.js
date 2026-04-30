/* public/app.js
 * Bot Trading Dashboard UI V1
 * Compatible with Dual Brain / Redis Archive / Training / Replay / PM2 flow
 */

const API_BASE = "";

const state = {
  health: null,
  brains: null,
  archive: null,
  training: null,
  trades: [],
  errors: [],
  selectedTrade: null,
  autoRefresh: true,
  refreshMs: 5000,
  timer: null,
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

function formatNumber(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "--";
  return n.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatInt(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "--";
  return n.toLocaleString("en-US");
}

function formatTs(ts) {
  if (!ts) return "--";
  const n = Number(ts);
  if (!Number.isFinite(n)) return "--";

  const d = new Date(n);
  if (Number.isNaN(d.getTime())) return "--";

  return d.toLocaleString("vi-VN", {
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function durationSec(entryTs, exitTs) {
  const a = Number(entryTs);
  const b = Number(exitTs);
  if (!Number.isFinite(a) || !Number.isFinite(b) || b <= a) return "--";
  return `${formatNumber((b - a) / 1000, 1)}s`;
}

function badge(text, type = "neutral") {
  return `<span class="badge badge-${type}">${text || "--"}</span>`;
}

function sideBadge(side) {
  const s = String(side || "").toUpperCase();
  if (s === "LONG") return badge("LONG", "long");
  if (s === "SHORT") return badge("SHORT", "short");
  return badge(s || "N/A", "neutral");
}

function statusBadge(value) {
  const s = String(value || "").toLowerCase();

  if (["ok", "online", "running", "active", "ready", "healthy"].includes(s)) {
    return badge(value, "ok");
  }

  if (["warn", "warning", "degraded", "pending"].includes(s)) {
    return badge(value, "warn");
  }

  if (["error", "offline", "stopped", "failed", "dead"].includes(s)) {
    return badge(value, "bad");
  }

  return badge(value || "unknown", "neutral");
}

async function apiGet(path, fallback = null) {
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      method: "GET",
      headers: { "Accept": "application/json" },
      cache: "no-store",
    });

    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }

    return await res.json();
  } catch (err) {
    console.warn(`[API GET FAILED] ${path}`, err);
    return fallback;
  }
}

async function apiPost(path, body = {}) {
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      method: "POST",
      headers: {
        "Accept": "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }

    return await res.json();
  } catch (err) {
    console.warn(`[API POST FAILED] ${path}`, err);
    showToast(`Lỗi gọi API: ${path}`, "bad");
    return null;
  }
}

function showToast(message, type = "neutral") {
  let el = $("#toast");

  if (!el) {
    el = document.createElement("div");
    el.id = "toast";
    el.className = "toast";
    document.body.appendChild(el);
  }

  el.className = `toast toast-${type} toast-show`;
  el.textContent = message;

  clearTimeout(el._timer);
  el._timer = setTimeout(() => {
    el.classList.remove("toast-show");
  }, 3000);
}

function setText(selector, value) {
  const el = $(selector);
  if (el) el.textContent = value;
}

function setHtml(selector, value) {
  const el = $(selector);
  if (el) el.innerHTML = value;
}

async function loadDashboard() {
  setText("#lastRefresh", "Đang tải...");

  const [health, brains, archive, training, trades, errors] = await Promise.all([
    apiGet("/api/system/health", null),
    apiGet("/api/brains/status", null),
    apiGet("/api/archive/status", null),
    apiGet("/api/training/latest-report", null),
    apiGet("/api/trades/history?limit=200", []),
    apiGet("/api/errors/history?limit=100", []),
  ]);

  state.health = health;
  state.brains = brains;
  state.archive = archive;
  state.training = training;
  state.trades = Array.isArray(trades) ? trades : trades?.items || [];
  state.errors = Array.isArray(errors) ? errors : errors?.items || [];

  renderAll();

  setText("#lastRefresh", `Cập nhật: ${formatTs(Date.now())}`);
}

function renderAll() {
  renderHealth();
  renderBrains();
  renderArchive();
  renderTraining();
  renderTrades();
  renderErrors();
}

function renderHealth() {
  const h = state.health || {};

  setHtml("#systemStatus", statusBadge(h.status || h.pm2_status || "unknown"));
  setText("#systemUptime", h.uptime || h.pm2_uptime || "--");
  setText("#redisStatus", h.redis || "--");
  setText("#binanceStatus", h.binance || "--");
  setText("#nodeVersion", h.node_version || h.node || "--");
  setText("#pythonVersion", h.python_version || h.python || "--");

  const services = h.services || h.pm2 || [];

  if (!Array.isArray(services) || services.length === 0) {
    setHtml("#pm2Services", `
      <tr>
        <td colspan="5" class="empty">Chưa có dữ liệu PM2. Cần backend trả về /api/system/health.</td>
      </tr>
    `);
    return;
  }

  setHtml("#pm2Services", services.map((svc) => `
    <tr>
      <td>${svc.name || "--"}</td>
      <td>${statusBadge(svc.status || "--")}</td>
      <td>${svc.restarts ?? "--"}</td>
      <td>${svc.cpu ?? "--"}</td>
      <td>${svc.memory ?? "--"}</td>
    </tr>
  `).join(""));
}

function renderBrains() {
  const b = state.brains || {};
  const entry = b.entry || b.EntryBrain || {};
  const exit = b.exit || b.ExitBrain || {};

  renderBrainCard("entry", "EntryBrain", entry);
  renderBrainCard("exit", "ExitBrain", exit);
}

function renderBrainCard(prefix, title, brain) {
  setText(`#${prefix}BrainName`, title);
  setHtml(`#${prefix}BrainStatus`, statusBadge(brain.status || brain.loaded || "unknown"));
  setText(`#${prefix}BrainVersion`, brain.version || brain.model_version || "--");
  setText(`#${prefix}BrainFeatures`, brain.features_count || brain.expected_input_features || "--");
  setText(`#${prefix}BrainOutput`, brain.output_dim || brain.expected_output_dim || "--");
  setText(`#${prefix}BrainPath`, brain.path || brain.onnx_path || brain.json_path || "--");

  const evalData = brain.evaluation || {};

  setText(`#${prefix}BrainAccuracy`, evalData.accuracy != null ? `${formatNumber(evalData.accuracy * 100, 2)}%` : "--");
  setText(`#${prefix}BrainAvgConf`, evalData.avg_confidence != null ? `${formatNumber(evalData.avg_confidence * 100, 2)}%` : "--");
}

function renderArchive() {
  const a = state.archive || {};

  setHtml("#archiveStatus", statusBadge(a.status || "unknown"));
  setText("#archiveVersion", a.version || "--");
  setText("#archiveFiles", formatInt(a.files || a.files_count));
  setText("#archiveRows", formatInt(a.rows || a.records || a.exported));
  setText("#archiveLastRun", formatTs(a.last_run_ms || a.completed_at_ms));
  setText("#archivePath", a.output_dir || a.path || "data/live_features_archive");
}

function renderTraining() {
  const t = state.training || {};
  const data = t.data || {};
  const replay = t.replay_backtest || {};

  setHtml("#trainingStatus", statusBadge(t.status || "unknown"));
  setText("#trainingVersion", t.version || "--");
  setText("#trainingRows", formatInt(data.rows));
  setText("#trainingSymbols", formatInt(data.symbols));
  setText("#trainingEpisodes", formatInt(data.episodes));
  setText("#trainingRuntime", t.runtime_sec != null ? `${formatNumber(t.runtime_sec, 1)}s` : "--");

  setText("#replayTrades", formatInt(replay.trades));
  setText("#replayProfit", replay.net_profit_usdt != null ? `${formatNumber(replay.net_profit_usdt, 4)} USDT` : "--");
  setText("#replayRoi", replay.roi_on_capital_pct != null ? `${formatNumber(replay.roi_on_capital_pct, 2)}%` : "--");
  setText("#replayWinrate", replay.winrate != null ? `${formatNumber(replay.winrate * 100, 2)}%` : "--");
  setText("#replayPF", replay.profit_factor != null ? formatNumber(replay.profit_factor, 2) : "--");
}

function renderTrades() {
  const trades = state.trades || [];

  setText("#tradeCount", formatInt(trades.length));

  if (!trades.length) {
    setHtml("#tradeRows", `
      <tr>
        <td colspan="10" class="empty">Chưa có lịch sử lệnh.</td>
      </tr>
    `);
    return;
  }

  setHtml("#tradeRows", trades.map((t, index) => {
    const pnl = Number(t.pnl_usdt ?? t.pnl ?? 0);
    const pnlClass = pnl >= 0 ? "text-profit" : "text-loss";

    return `
      <tr class="clickable" data-trade-index="${index}">
        <td>${formatTs(t.entry_ts)}</td>
        <td>${formatTs(t.exit_ts)}</td>
        <td>${t.symbol || "--"}</td>
        <td>${sideBadge(t.side)}</td>
        <td>${formatNumber(t.entry_price, 6)}</td>
        <td>${formatNumber(t.exit_price, 6)}</td>
        <td class="${pnlClass}">${formatNumber(pnl, 4)}</td>
        <td>${t.net_roi != null ? `${formatNumber(t.net_roi, 2)}%` : "--"}</td>
        <td>${durationSec(t.entry_ts, t.exit_ts)}</td>
        <td>${statusBadge(t.reason || t.status || "closed")}</td>
      </tr>
    `;
  }).join(""));

  $$("#tradeRows tr[data-trade-index]").forEach((row) => {
    row.addEventListener("click", () => {
      const index = Number(row.dataset.tradeIndex);
      openTradeModal(trades[index]);
    });
  });
}

function renderErrors() {
  const errors = state.errors || [];

  setText("#errorCount", formatInt(errors.length));

  if (!errors.length) {
    setHtml("#errorRows", `
      <tr>
        <td colspan="5" class="empty">Chưa có lỗi được ghi nhận.</td>
      </tr>
    `);
    return;
  }

  setHtml("#errorRows", errors.map((err) => `
    <tr>
      <td>${formatTs(err.ts || err.time_ms || err.created_at_ms)}</td>
      <td>${badge(err.level || "ERROR", "bad")}</td>
      <td>${err.service || err.module || "--"}</td>
      <td>${err.symbol || "--"}</td>
      <td class="error-message">${escapeHtml(err.message || err.error || JSON.stringify(err))}</td>
    </tr>
  `).join(""));
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function openTradeModal(trade) {
  if (!trade) return;

  state.selectedTrade = trade;

  const pnl = Number(trade.pnl_usdt ?? trade.pnl ?? 0);
  const pnlClass = pnl >= 0 ? "text-profit" : "text-loss";

  setText("#modalSymbol", trade.symbol || "--");
  setHtml("#modalSide", sideBadge(trade.side));
  setText("#modalEntryTime", formatTs(trade.entry_ts));
  setText("#modalExitTime", formatTs(trade.exit_ts));
  setText("#modalHoldTime", durationSec(trade.entry_ts, trade.exit_ts));
  setText("#modalEntryPrice", formatNumber(trade.entry_price, 8));
  setText("#modalExitPrice", formatNumber(trade.exit_price, 8));
  setHtml("#modalPnl", `<span class="${pnlClass}">${formatNumber(pnl, 4)} USDT</span>`);
  setText("#modalRoi", trade.net_roi != null ? `${formatNumber(trade.net_roi, 2)}%` : "--");
  setText("#modalReason", trade.reason || "--");
  setText("#modalEntryProb", trade.entry_prob != null ? `${formatNumber(trade.entry_prob * 100, 2)}%` : "--");
  setText("#modalBestRoi", trade.best_roi != null ? `${formatNumber(trade.best_roi, 2)}%` : "--");
  setText("#modalWorstRoi", trade.worst_roi != null ? `${formatNumber(trade.worst_roi, 2)}%` : "--");

  renderTradeTimeline(trade);

  const modal = $("#tradeModal");
  if (modal) modal.classList.add("modal-open");
}

function renderTradeTimeline(trade) {
  const entryTs = Number(trade.entry_ts);
  const exitTs = Number(trade.exit_ts);
  const hasExit = Number.isFinite(exitTs) && exitTs > entryTs;

  const events = [
    {
      title: "Tín hiệu vào lệnh",
      time: trade.entry_ts,
      desc: `Bot vào ${trade.side || "--"} ${trade.symbol || "--"} tại giá ${formatNumber(trade.entry_price, 8)}. Entry probability: ${
        trade.entry_prob != null ? `${formatNumber(trade.entry_prob * 100, 2)}%` : "--"
      }.`,
      type: "entry",
    },
  ];

  if (trade.brain_snapshot || trade.entry_brain || trade.exit_brain) {
    events.push({
      title: "Snapshot bộ não",
      time: trade.entry_ts,
      desc: `EntryBrain / ExitBrain snapshot được ghi nhận tại thời điểm vào lệnh.`,
      type: "brain",
    });
  }

  if (hasExit) {
    events.push({
      title: "Tín hiệu thoát lệnh",
      time: trade.exit_ts,
      desc: `Thoát lệnh tại giá ${formatNumber(trade.exit_price, 8)}. Lý do: ${trade.reason || "--"}.`,
      type: "exit",
    });
  }

  events.push({
    title: "Kết quả lệnh",
    time: trade.exit_ts || trade.entry_ts,
    desc: `PnL: ${formatNumber(trade.pnl_usdt ?? trade.pnl, 4)} USDT | ROI: ${
      trade.net_roi != null ? `${formatNumber(trade.net_roi, 2)}%` : "--"
    } | Hold: ${durationSec(trade.entry_ts, trade.exit_ts)}.`,
    type: Number(trade.pnl_usdt ?? trade.pnl ?? 0) >= 0 ? "profit" : "loss",
  });

  setHtml("#tradeTimeline", events.map((ev) => `
    <div class="timeline-item timeline-${ev.type}">
      <div class="timeline-dot"></div>
      <div class="timeline-content">
        <div class="timeline-title">${ev.title}</div>
        <div class="timeline-time">${formatTs(ev.time)}</div>
        <div class="timeline-desc">${escapeHtml(ev.desc)}</div>
      </div>
    </div>
  `).join(""));
}

function closeTradeModal() {
  const modal = $("#tradeModal");
  if (modal) modal.classList.remove("modal-open");
}

async function handleAction(action) {
  const confirmMap = {
    start_all: "Chạy toàn bộ luồng PM2?",
    stop_all: "Dừng toàn bộ luồng PM2?",
    restart_all: "Restart toàn bộ luồng PM2?",
    archive_once: "Chạy export Redis archive một lần?",
    train_once: "Chạy train Dual Brain một lần?",
  };

  const message = confirmMap[action] || `Thực hiện action: ${action}?`;

  if (!window.confirm(message)) return;

  showToast(`Đang gửi lệnh: ${action}`, "warn");

  const result = await apiPost("/api/actions/run", { action });

  if (result) {
    showToast(result.message || `Đã gửi lệnh: ${action}`, "ok");
    await loadDashboard();
  }
}

function bindEvents() {
  const refreshBtn = $("#btnRefresh");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", loadDashboard);
  }

  const autoRefreshBtn = $("#btnAutoRefresh");
  if (autoRefreshBtn) {
    autoRefreshBtn.addEventListener("click", () => {
      state.autoRefresh = !state.autoRefresh;
      autoRefreshBtn.textContent = state.autoRefresh ? "Auto Refresh: ON" : "Auto Refresh: OFF";
      autoRefreshBtn.classList.toggle("btn-active", state.autoRefresh);
      setupAutoRefresh();
    });
  }

  $$("[data-action]").forEach((btn) => {
    btn.addEventListener("click", () => {
      handleAction(btn.dataset.action);
    });
  });

  const closeModalBtn = $("#closeTradeModal");
  if (closeModalBtn) {
    closeModalBtn.addEventListener("click", closeTradeModal);
  }

  const modal = $("#tradeModal");
  if (modal) {
    modal.addEventListener("click", (e) => {
      if (e.target === modal) closeTradeModal();
    });
  }

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeTradeModal();
  });
}

function setupAutoRefresh() {
  if (state.timer) {
    clearInterval(state.timer);
    state.timer = null;
  }

  if (!state.autoRefresh) return;

  state.timer = setInterval(() => {
    loadDashboard();
  }, state.refreshMs);
}

function renderBootMessage() {
  setHtml("#pm2Services", `
    <tr>
      <td colspan="5" class="empty">Đang tải trạng thái PM2...</td>
    </tr>
  `);

  setHtml("#tradeRows", `
    <tr>
      <td colspan="10" class="empty">Đang tải lịch sử lệnh...</td>
    </tr>
  `);

  setHtml("#errorRows", `
    <tr>
      <td colspan="5" class="empty">Đang tải lịch sử lỗi...</td>
    </tr>
  `);
}

document.addEventListener("DOMContentLoaded", async () => {
  renderBootMessage();
  bindEvents();
  setupAutoRefresh();
  await loadDashboard();
});