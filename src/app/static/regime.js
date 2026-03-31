(() => {
  const PREFS_KEY = "regime_ui_prefs";
  const COLORS = {
    ink: "#111827",
    muted: "#6b7280",
    border: "#e5e7eb",
    neutral: "#9ca3af",
    bull: "#16a34a",
    bullBg: "#f0fdf4",
    bear: "#dc2626",
    bearBg: "#fef2f2",
    neutralBg: "#f9fafb",
    action: "#ea580c",
    actionBg: "#fff7ed",
    ok: "#16a34a",
    warning: "#d97706",
    danger: "#dc2626",
    warn: "#b45309",
    planPending: "#f59e0b",
    planApproved: "#3b82f6",
    planExecuted: "#10b981",
    planRejected: "#ef4444",
    planSubmitted: "#8b5cf6",
    pnlPositive: "#166534",
    pnlNegative: "#991b1b",
    white: "#fff",
  };

  const state = {
    config: null,
    holdings: [],
    holdingGroups: {},
    portfolioScopes: [],
    selectedHoldings: [],
    themes: [],
    themeHealth: [],
    watchlist: [],
    watchlistStats: {},
    paperPortfolios: [],
    currentPaperPortfolioId: null,
    paperPortfolioDetail: null,
    paperBudget: null,
    paperPlans: [],
    paperPositions: [],
    paperTaxLots: [],
    paperWashSale: [],
    paperTaxEstimates: {},
    paperPerformance: null,
    paperAttribution: null,
    autonomySettings: null,
    autonomyStatus: null,
    taxSettings: null,
    paperAudit: [],
    paperAuditSummary: null,
    paperMonitoring: null,
    systemHealth: null,
    dataValidation: null,
    alertHistory: [],
    unacknowledgedAlerts: [],
    vixStatus: null,
    monitoringTimer: null,
    paperPrecheck: {},
    paperExecutionResults: null,
    paperOrderPollTimer: null,
    discoveryJob: null,
    currentJobId: null,
    pollTimer: null,
    eventSource: null,
    streamRetries: 0,
    lastPayload: null,
    detailChartsRendered: {},
    pickerFocusIndex: -1,
    showAllTableColumns: false,
    ibkrSettings: null,
    ibkrReadiness: null,
    ibkrStatus: null,
    ibkrTestResult: null,
    ibkrRestartRequired: false,
    expandedDiagnosticsTicker: null,
    frontierSettings: null,
    marketDataSettings: null,
    notificationPreferences: null,
    ensembleWeights: null,
  };

  function byId(id) {
    return document.getElementById(id);
  }

  function updateStreamBadge(status) {
    const badge = byId("regimeStreamBadge");
    if (!badge) return;
    badge.className = `regime-stream-badge regime-stream-badge--${status}`;
    const labels = { connected: "Live", reconnecting: "Reconnecting…", idle: "" };
    badge.textContent = labels[status] || "";
    badge.style.display = status === "idle" ? "none" : "";
  }

  function loadPrefs() {
    try {
      return JSON.parse(window.localStorage.getItem(PREFS_KEY) || "{}");
    } catch {
      return {};
    }
  }

  function savePref(key, value) {
    const prefs = loadPrefs();
    prefs[key] = value;
    window.localStorage.setItem(PREFS_KEY, JSON.stringify(prefs));
  }

  function parseJson(id) {
    const el = byId(id);
    if (!el) return null;
    try {
      return JSON.parse(el.textContent || "null");
    } catch {
      return null;
    }
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function numFromCell(td) {
    if (!td) return NaN;
    const raw = td.getAttribute("data-sort-val");
    if (raw != null && String(raw).trim() !== "") {
      const n = Number(raw);
      if (Number.isFinite(n)) return n;
    }
    const t = (td.textContent || "").replace(/[%$,]/g, "").trim();
    const n = Number(t);
    return Number.isFinite(n) ? n : NaN;
  }

  function textFromCell(td) {
    return (td && td.textContent ? td.textContent : "").trim().toLowerCase();
  }

  function signalClass(action) {
    if (action === "Strong Buy" || action === "Buy") return "cell-ok";
    if (action === "Strong Sell" || action === "Sell") return "cell-bad";
    return "";
  }

  function sentimentClass(trend) {
    const normalized = String(trend || "").toLowerCase();
    if (normalized === "improving") return "ui-badge ui-badge--safe";
    if (normalized === "declining") return "ui-badge ui-badge--bad";
    return "ui-badge ui-badge--neutral";
  }

  function badgeClass(value) {
    if (value === "Bull") return "ui-badge ui-badge--safe";
    if (value === "Bear") return "ui-badge ui-badge--bad";
    return "ui-badge ui-badge--neutral";
  }

  function planStatusClass(status) {
    const normalized = String(status || "").toLowerCase();
    if (normalized === "pending") return "regime-plan-card--pending";
    if (normalized === "approved" || normalized === "modified") return "regime-plan-card--approved";
    if (normalized === "executed" || normalized === "filled") return "regime-plan-card--executed";
    if (normalized === "rejected" || normalized === "expired") return "regime-plan-card--rejected";
    if (normalized === "submitted" || normalized === "partially filled") return "regime-plan-card--submitted";
    return "";
  }

  function relativeTime(value) {
    if (!value) return "n/a";
    const ts = new Date(value).getTime();
    if (!Number.isFinite(ts)) return String(value);
    const deltaMs = Date.now() - ts;
    const minutes = Math.round(Math.abs(deltaMs) / 60000);
    if (minutes < 1) return "just now";
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.round(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    const days = Math.round(hours / 24);
    return `${days}d ago`;
  }

  function divergenceBadge(row) {
    if (row.multi_timeframe_aligned) return '<span class="ui-muted">Aligned</span>';
    const info = row.divergence_severity || {};
    const score = Number(info.score || 0);
    const title = escapeHtml(info.interpretation || "Divergent");
    if (score >= 0.7) return `<span class="cell-bad" title="${title}">Divergent ⚠</span>`;
    if (score >= 0.4) return `<span style="color:${COLORS.warn}" title="${title}">Divergent</span>`;
    return `<span class="ui-muted" title="${title}">Divergent</span>`;
  }

  function currentBenchmark() {
    const el = document.querySelector("[data-regime-benchmark]");
    return (el && el.value ? el.value : "SOXX").trim().toUpperCase();
  }

  function currentPeriod() {
    const el = document.querySelector("[data-regime-period]");
    return (el && el.value ? el.value : "3y").trim();
  }

  function showAllEnabled() {
    const el = document.querySelector("[data-regime-show-all]");
    return !!(el && el.checked);
  }

  function currentFrontierEnabled() {
    const el = document.querySelector("[data-regime-frontier-enabled]");
    return !!(el && el.checked);
  }

  function currentFrontierProvider() {
    const el = document.querySelector("[data-regime-frontier-provider]");
    return (el && el.value ? el.value : "auto").trim().toLowerCase();
  }

  function currentFrontierModel() {
    const el = document.querySelector("[data-regime-frontier-model]");
    return (el && el.value ? el.value : "").trim();
  }

  function currentForceRefresh() {
    const el = document.querySelector("[data-regime-force-refresh]");
    return !!(el && el.checked);
  }

  function currentPortfolioScope() {
    const el = document.querySelector("[data-regime-portfolio-scope]");
    return (el && el.value ? el.value : "household").trim().toLowerCase();
  }

  function currentAccountId() {
    const el = document.querySelector("[data-regime-account-id]");
    return (el && el.value ? el.value : "").trim();
  }

  function selectedCustom() {
    const expandTickerToken = (token) => {
      const normalized = String(token || "").trim().toUpperCase();
      if (!normalized) return [];
      const parts = normalized.split(/\s+/).filter(Boolean);
      if (parts.length <= 1) return [normalized];
      if (parts.length === 2 && parts[1].length === 1 && parts[0].length <= 5) return [normalized];
      return parts;
    };
    const input = document.querySelector("[data-regime-custom-input]");
    if (!input) return [];
    const raw = String(input.value || "");
    return raw
      .split(/[,;\n]+/)
      .flatMap((token) => expandTickerToken(token))
      .filter(Boolean);
  }

  function syncFrontierControlVisibility() {
    const providerWrap = document.querySelector("[data-regime-provider-wrap]");
    const modelWrap = document.querySelector("[data-regime-model-wrap]");
    const enabled = currentFrontierEnabled();
    const provider = currentFrontierProvider();
    if (providerWrap) providerWrap.style.display = enabled ? "" : "none";
    if (modelWrap) {
      modelWrap.style.display = enabled && provider && provider !== "auto" && provider !== "best" ? "" : "none";
    }
  }

  async function saveFrontierSettings(provider, model) {
    const response = await fetch("/regime/frontier/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/x-www-form-urlencoded", Accept: "application/json" },
      body: new URLSearchParams({ provider: String(provider || "auto"), model: String(model || "") }),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.detail || "Unable to save frontier settings");
    state.frontierSettings = data;
    return data;
  }

  async function loadFrontierSettings() {
    const response = await fetch("/regime/frontier/settings", { headers: { Accept: "application/json" } });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.detail || "Unable to load frontier settings");
    state.frontierSettings = data;
    return data;
  }

  async function loadFrontierModels(provider, forceRefresh = false) {
    const modelSelect = document.querySelector("[data-regime-frontier-model]");
    const modelWrap = document.querySelector("[data-regime-model-wrap]");
    if (!modelSelect || !modelWrap) return;
    if (!provider || provider === "auto" || provider === "best" || !currentFrontierEnabled()) {
      modelWrap.style.display = "none";
      return;
    }
    modelWrap.style.display = "";
    modelSelect.innerHTML = '<option value="">Loading...</option>';
    modelSelect.disabled = true;
    try {
      const url = `/regime/frontier/models?provider=${encodeURIComponent(provider)}${forceRefresh ? "&refresh=1" : ""}`;
      const response = await fetch(url, { headers: { Accept: "application/json" } });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(data.detail || "Failed to load models");
      const models = Array.isArray(data.models) ? data.models : [];
      modelSelect.innerHTML = '<option value="">Default (env)</option>';
      models.forEach((model) => {
        const opt = document.createElement("option");
        opt.value = model.id;
        opt.textContent = model.name || model.id;
        modelSelect.appendChild(opt);
      });
      const settings = state.frontierSettings || (await loadFrontierSettings());
      if (settings.provider === provider && settings.model) {
        modelSelect.value = settings.model;
      } else {
        modelSelect.value = "";
      }
    } catch (error) {
      modelSelect.innerHTML = '<option value="">Failed to load</option>';
      showToast(`Unable to load ${provider} models: ${error.message || error}`, "error");
    } finally {
      modelSelect.disabled = false;
    }
  }

  async function loadEnsembleWeights() {
    if (!state.config?.endpoints?.ensemble_weights) return null;
    const response = await fetch(state.config.endpoints.ensemble_weights, { headers: { Accept: "application/json" } });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || "Unable to load ensemble weights");
    state.ensembleWeights = payload;
    renderEnsemblePanel(state.lastPayload || state.config.initial_payload || {});
    return payload;
  }

  async function saveEnsembleWeights() {
    if (!state.config?.endpoints?.ensemble_weights) return;
    const mount = byId("regimeEnsembleMount");
    if (!mount) return;
    const analysts = {};
    mount.querySelectorAll("[data-ensemble-row]").forEach((row) => {
      const name = String(row.getAttribute("data-ensemble-row") || "");
      const enabled = !!row.querySelector("[data-ensemble-enabled]")?.checked;
      const weightValue = row.querySelector("[data-ensemble-weight]")?.value;
      analysts[name] = { enabled, weight: Number(weightValue || 1) };
    });
    const aggregation = String(byId("regimeEnsembleAggregation")?.value || "mean");
    try {
      const response = await fetch(state.config.endpoints.ensemble_weights, {
        method: "PUT",
        headers: { Accept: "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({ analysts, aggregation_method: aggregation }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload.detail || "Unable to save ensemble weights");
      state.ensembleWeights = payload;
      renderEnsemblePanel(state.lastPayload || state.config.initial_payload || {});
      showToast("Ensemble weights saved.", "success");
    } catch (error) {
      showToast(`Unable to save ensemble weights: ${error.message || error}`, "error");
    }
  }

  function getSelectedTickers() {
    const merged = [];
    [...state.selectedHoldings, ...selectedCustom()].forEach((ticker) => {
      if (ticker && !merged.includes(ticker)) merged.push(ticker);
    });
    return merged;
  }

  function setPickerMessage(message, bad = false) {
    const warning = byId("regimePickerWarning");
    if (!warning) return;
    if (!message) {
      warning.textContent = "";
      warning.style.display = "none";
      return;
    }
    warning.textContent = message;
    warning.className = bad ? "ui-muted cell-bad" : "ui-muted";
    warning.style.display = "";
  }

  function setThemeMessage(message, bad = false) {
    const el = byId("regimeThemeMessage");
    if (!el) return;
    if (!message) {
      el.textContent = "";
      el.style.display = "none";
      return;
    }
    el.textContent = message;
    el.className = bad ? "ui-muted cell-bad" : "ui-muted";
    el.style.display = "";
  }

  function showToast(message, level = "info") {
    const mount = byId("regimeWatchlistMount");
    if (!mount || !message) return;
    const tone = level === "error" ? "banner--warn" : "banner--ok";
    const toast = document.createElement("div");
    toast.className = tone;
    toast.style.padding = "10px";
    toast.style.borderRadius = "10px";
    toast.style.marginBottom = "8px";
    toast.textContent = String(message);
    mount.prepend(toast);
    window.setTimeout(() => {
      if (toast.parentNode) toast.parentNode.removeChild(toast);
    }, 5000);
  }

  function alertEndpoint(alertId = null) {
    const base = String(state.config?.endpoints?.alert_acknowledge || "");
    return alertId == null ? "" : base.replace("__ALERT_ID__", encodeURIComponent(String(alertId)));
  }

  function alertToneStyle(severity) {
    const normalized = String(severity || "info").toLowerCase();
    if (normalized === "critical") return "background:#fef2f2;border:1px solid #fecaca;color:#991b1b";
    if (normalized === "warning") return "background:#fffbeb;border:1px solid #fde68a;color:#92400e";
    return "background:#eff6ff;border:1px solid #bfdbfe;color:#1d4ed8";
  }

  function renderAlertToasts() {
    const mount = byId("regimeAlertToastMount");
    if (!mount) return;
    const alerts = Array.isArray(state.unacknowledgedAlerts) ? state.unacknowledgedAlerts : [];
    if (!alerts.length) {
      mount.innerHTML = "";
      return;
    }
    mount.innerHTML = `
      <div class="ui-card" style="padding:12px">
        <div class="table-toolbar">
          <div class="ui-section-title">Active Alerts</div>
          <button class="btn btn--secondary" type="button" id="regimeAlertDismissAll">Dismiss all</button>
        </div>
        <div style="display:grid; gap:8px; margin-top:10px">
          ${alerts.slice(0, 5).map((alert) => `
            <div style="${alertToneStyle(alert.severity)}; border-radius:10px; padding:10px; display:flex; justify-content:space-between; gap:10px; align-items:flex-start">
              <div>
                <div style="font-weight:600">${escapeHtml(alert.title || "")}</div>
                <div class="ui-muted" style="margin-top:4px">${escapeHtml(alert.message || "")}</div>
              </div>
              <button class="btn btn--secondary" type="button" data-alert-ack="${escapeHtml(alert.id)}">×</button>
            </div>
          `).join("")}
        </div>
      </div>
    `;
    mount.querySelectorAll("[data-alert-ack]").forEach((button) => {
      button.addEventListener("click", async () => {
        try {
          const response = await fetch(alertEndpoint(button.getAttribute("data-alert-ack")), { method: "POST", headers: { Accept: "application/json" } });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Acknowledge failed (${response.status})`);
          await loadAlerts();
        } catch (error) {
          showToast(`Unable to acknowledge alert: ${error.message || error}`, "error");
        }
      });
    });
    const dismissAll = byId("regimeAlertDismissAll");
    if (dismissAll) {
      dismissAll.addEventListener("click", async () => {
        try {
          const response = await fetch(state.config.endpoints.alerts_acknowledge_all, { method: "POST", headers: { Accept: "application/json" } });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Dismiss all failed (${response.status})`);
          await loadAlerts();
        } catch (error) {
          showToast(`Unable to dismiss alerts: ${error.message || error}`, "error");
        }
      });
    }
  }

  function renderAlertHistory() {
    const mount = byId("regimeAlertHistoryMount");
    if (!mount) return;
    const alerts = Array.isArray(state.alertHistory) ? state.alertHistory : [];
    mount.innerHTML = `
      <details class="ui-card" style="padding:12px" open>
        <summary style="cursor:pointer; font-weight:600">Alert History</summary>
        <div class="table-wrap" style="margin-top:10px">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Type</th>
                <th>Severity</th>
                <th>Title</th>
                <th>Ack</th>
              </tr>
            </thead>
            <tbody>
              ${alerts.length ? alerts.map((alert) => `
                <tr>
                  <td>${escapeHtml(relativeTime(alert.created_at))}</td>
                  <td><span class="ui-badge ui-badge--neutral">${escapeHtml(alert.alert_type || "")}</span></td>
                  <td><span class="${String(alert.severity || "") === "critical" ? "ui-badge ui-badge--bad" : String(alert.severity || "") === "warning" ? "ui-badge ui-badge--warn" : "ui-badge ui-badge--safe"}">${escapeHtml(alert.severity || "")}</span></td>
                  <td>${escapeHtml(alert.title || "")}</td>
                  <td>${Number(alert.acknowledged || 0) ? "✓" : ""}</td>
                </tr>
              `).join("") : '<tr><td colspan="5" class="ui-muted">No alerts yet.</td></tr>'}
            </tbody>
          </table>
        </div>
      </details>
    `;
  }

  function renderVixStatus() {
    const mount = byId("regimeVixStatusMount");
    if (!mount) return;
    const status = state.vixStatus || {};
    if (status.vix == null && !status.frozen) {
      mount.innerHTML = "";
      return;
    }
    const vix = Number(status.vix || 0);
    const freeze = Number(status.freeze_threshold || 35);
    const resume = Number(status.resume_threshold || 30);
    const tone = status.frozen ? "banner--warn" : vix >= freeze ? "banner--warn" : vix >= resume ? "ui-card" : "banner--ok";
    mount.innerHTML = `
      <div class="${tone}" style="padding:10px; border-radius:10px">
        <div class="table-toolbar">
          <div>
            <div style="font-weight:600">VIX: ${escapeHtml(vix.toFixed(1))}</div>
            <div class="ui-muted">${status.frozen ? "VIX FREEZE ACTIVE — Buy entries suspended" : `Freeze ${freeze.toFixed(0)} · Resume ${resume.toFixed(0)}`}</div>
          </div>
          <button class="btn btn--secondary" type="button" id="regimeVixOverride">${status.frozen ? "Manual Unfreeze" : "Manual Freeze"}</button>
        </div>
      </div>
    `;
    const button = byId("regimeVixOverride");
    if (button) {
      button.addEventListener("click", async () => {
        const unfreeze = !!status.frozen;
        if (!window.confirm(unfreeze ? "Lift the VIX freeze manually?" : "Activate the VIX freeze manually?")) return;
        try {
          const response = await fetch(state.config.endpoints.vix_override, {
            method: "POST",
            headers: { Accept: "application/json", "Content-Type": "application/json" },
            body: JSON.stringify({ unfreeze }),
          });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `VIX override failed (${response.status})`);
          state.vixStatus = payload;
          renderVixStatus();
          await loadAlerts();
        } catch (error) {
          showToast(`Unable to update VIX freeze: ${error.message || error}`, "error");
        }
      });
    }
  }

  function syncCustomInputFromTickers(tickers) {
    const input = document.querySelector("[data-regime-custom-input]");
    if (!input) return;
    const holdingsSet = new Set(state.holdings);
    const currentCustom = String(input.value || "")
      .split(",")
      .map((token) => token.trim().toUpperCase())
      .filter(Boolean)
      .filter((ticker, idx, arr) => arr.indexOf(ticker) === idx)
      .filter((ticker) => !holdingsSet.has(ticker));
    const nextCustom = tickers.filter((ticker) => !holdingsSet.has(ticker));
    if (currentCustom.join(",") !== nextCustom.join(",")) {
      input.value = nextCustom.join(", ");
    }
  }

  function themeByTickerMap() {
    const map = new Map();
    (state.themes || []).forEach((theme) => {
      (theme.tickers || []).forEach((item) => {
        const ticker = String(item.ticker || "").toUpperCase();
        if (!ticker || map.has(ticker)) return;
        map.set(ticker, { name: theme.name || "General", conviction: theme.conviction || 0 });
      });
    });
    return map;
  }

  function regimeForTicker(ticker) {
    const rows = Array.isArray((state.lastPayload || {}).rows) ? state.lastPayload.rows : [];
    const match = rows.find((item) => String(item.ticker || "").toUpperCase() === String(ticker || "").toUpperCase());
    return String((match && match.regime) || "Neutral");
  }

  function renderSelectedChips() {
    const mount = byId("regimeSelectedChips");
    if (!mount) return;
    if (!state.selectedHoldings.length) {
      mount.innerHTML = '<div class="regime-empty-state"><div class="regime-empty-state__icon">🏷</div><div class="regime-empty-state__message">No holdings selected.</div></div>';
      return;
    }
    const groups = new Map();
    const themeMap = themeByTickerMap();
    state.selectedHoldings.forEach((ticker) => {
      const theme = themeMap.get(ticker) || { name: "Unassigned", conviction: 0 };
      const key = `${theme.name}::${theme.conviction}`;
      if (!groups.has(key)) groups.set(key, { ...theme, tickers: [] });
      groups.get(key).tickers.push(ticker);
    });
    mount.innerHTML = Array.from(groups.values()).map((group) => `
      <div class="regime-chip-group">
        <div class="regime-chip-group__label">${escapeHtml(group.name)}${group.conviction ? ` (Conviction ${escapeHtml(group.conviction)}/5)` : ""}</div>
        ${group.tickers.map((ticker) => {
          const regime = regimeForTicker(ticker);
          const regimeClass = regime === "Bull" ? "regime-chip--bull" : regime === "Bear" ? "regime-chip--bear" : "regime-chip--neutral";
          return `
            <span class="regime-chip ${regimeClass}">
              ${escapeHtml(ticker)}
              <button type="button" class="regime-chip__remove" data-regime-chip-remove="${escapeHtml(ticker)}" aria-label="Remove ${escapeHtml(ticker)}">&times;</button>
            </span>
          `;
        }).join("")}
      </div>
    `).join("");
    mount.querySelectorAll("[data-regime-chip-remove]").forEach((button) => {
      button.addEventListener("click", () => removeHoldingSelection(String(button.getAttribute("data-regime-chip-remove") || "")));
    });
  }

  function updateSelectionCounter() {
    const counter = byId("regimeSelectionCounter");
    const badge = byId("selected-count");
    const summaryBadge = byId("regimeSelectionBadge");
    if (!counter) return;
    const tickers = getSelectedTickers();
    counter.textContent = `Selected: ${tickers.length}`;
    if (badge) {
      badge.textContent = `${tickers.length} selected`;
      badge.style.display = tickers.length > 0 ? "" : "none";
    }
    if (summaryBadge) {
      summaryBadge.textContent = `${tickers.length} selected`;
    }
    if (tickers.length > state.config.max_tickers) {
      setPickerMessage(`Select no more than ${state.config.max_tickers} tickers per run.`, true);
    } else if (currentFrontierEnabled() && tickers.length > 20) {
      setPickerMessage(`Analyzing ${tickers.length} tickers with Frontier may take several minutes. Cached results will be reused where available.`);
    } else {
      const current = byId("regimePickerWarning");
      if (current && current.textContent && (current.textContent.includes("Select no more than") || current.textContent.includes("may take several minutes"))) {
        setPickerMessage("");
      }
    }
    renderSelectedChips();
    const selectAll = byId("regimeSelectAll");
    if (selectAll) {
      selectAll.textContent = state.selectedHoldings.length === state.holdings.length && state.holdings.length ? "Deselect all" : "Select all";
    }
  }

  function exposureText(payload) {
    const exposure = payload.regime_exposure || {};
    const bull = Number(exposure.Bull || 0) * 100;
    const neutral = Number(exposure.Neutral || 0) * 100;
    const bear = Number(exposure.Bear || 0) * 100;
    return `Bull ${bull.toFixed(0)}% · Neutral ${neutral.toFixed(0)}% · Bear ${bear.toFixed(0)}%`;
  }

  function exposureTone(payload) {
    const exposure = payload.regime_exposure || {};
    const bull = Number(exposure.Bull || 0);
    const neutral = Number(exposure.Neutral || 0);
    const bear = Number(exposure.Bear || 0);
    if (bull >= neutral && bull >= bear) return { color: "#15803d", label: "Bull-heavy" };
    if (bear >= bull && bear >= neutral) return { color: "#b91c1c", label: "Bear-heavy" };
    return { color: "#b45309", label: "Neutral-heavy" };
  }

  function renderKpis(payload) {
    const mount = byId("regimeKpis");
    if (!mount) return;
    const monitoring = state.paperMonitoring || {};
    const account = monitoring.account || {};
    const exposure = payload.regime_exposure || {};
    const bullPct = Math.max(0, Number(exposure.Bull || 0) * 100);
    const neutralPct = Math.max(0, Number(exposure.Neutral || 0) * 100);
    const bearPct = Math.max(0, Number(exposure.Bear || 0) * 100);
    const dominantColor = bullPct >= neutralPct && bullPct >= bearPct
      ? COLORS.bull
      : bearPct >= bullPct && bearPct >= neutralPct
        ? COLORS.bear
        : COLORS.neutral;
    const totalEquity = Number(account.equity ?? account.net_liquidation ?? 0);
    const dailyPnl = Number(account.daily_pnl ?? account.unrealized_pnl ?? 0);
    const exposurePct = Number(account.exposure_pct || 0);
    const avgTransitionRisk = Number(payload.aggregate_transition_risk_pct ?? 0);
    const riskTone = avgTransitionRisk > 50 ? "negative" : avgTransitionRisk >= 25 ? "warning" : "positive";
    const pendingPlans = (state.paperPlans || []).filter((plan) => ["Pending", "Approved", "Submitted", "Partially Filled"].includes(String(plan.status || ""))).length;
    const actionItems = Number(payload.action_items_count || 0) + pendingPlans;
    const mlActive = !!(payload.ensemble_status && payload.ensemble_status.meta_labeler_active);
    const mlVersion = payload.ensemble_status && payload.ensemble_status.meta_labeler_version;
    const actionDetail = pendingPlans
      ? `${pendingPlans} trade plan${pendingPlans === 1 ? "" : "s"} pending review`
      : actionItems
        ? `${actionItems} digest item${actionItems === 1 ? "" : "s"} need attention`
        : "No actions required";
    mount.innerHTML = `
      <div class="ui-card ui-kpi ${totalEquity ? (dailyPnl >= 0 ? "ui-tone-positive" : "ui-tone-negative") : "ui-tone-neutral"}">
        <div class="ui-card__label">Portfolio P&amp;L</div>
        <div class="ui-card__value ui-tabular-nums">${totalEquity ? escapeHtml(formatCurrency(totalEquity, 0)) : "No portfolio"}</div>
        <div class="ui-card__subtext ui-muted">${totalEquity ? `Today: ${escapeHtml(formatCurrency(dailyPnl, 0))}` : "Create or select a trading portfolio"}</div>
        <div class="ui-card__subtext ui-muted" style="margin-top:6px"><span class="ui-badge ${mlActive ? "ui-badge--safe" : "ui-badge--neutral"}">ML ${mlActive ? `Active v${mlVersion || "?"}` : "Inactive"}</span></div>
      </div>
      <div class="ui-card ui-kpi regime-kpi--exposure">
        <div class="ui-card__label">Exposure</div>
        <div class="ui-card__value ui-tabular-nums" id="regimeKpiExposure">${escapeHtml(formatFixed(exposurePct, 1))}%</div>
        <div class="regime-kpi__gauge" id="regimeKpiExposureGauge">
          <div class="regime-kpi__gauge-fill" style="width:${Math.max(0, Math.min(100, exposurePct)).toFixed(1)}%; background:${dominantColor}"></div>
        </div>
        <div class="ui-card__subtext ui-muted">${escapeHtml(exposureText(payload))}</div>
      </div>
      <div class="ui-card ui-kpi ui-tone-${riskTone}">
        <div class="ui-card__label">Risk Score</div>
        <div class="ui-card__value ui-tabular-nums">${escapeHtml(formatFixed(avgTransitionRisk, 1))}</div>
        <div class="ui-card__subtext ui-muted">Transition risk: ${escapeHtml(formatFixed(avgTransitionRisk, 1))}%</div>
      </div>
      <button class="ui-card ui-kpi regime-kpi--alerts ${actionItems > 0 ? "has-actions" : ""}" type="button" id="regimeActionItemsKpi">
        <div class="ui-card__label">Action Items</div>
        <div class="ui-card__value ui-tabular-nums" id="regimeKpiAlerts">${escapeHtml(actionItems)}</div>
        <div class="ui-card__subtext ui-muted" id="regimeKpiAlertsDetail">${escapeHtml(actionDetail)}</div>
      </button>
    `;
    const actionKpi = byId("regimeActionItemsKpi");
    if (actionKpi) {
      actionKpi.addEventListener("click", () => switchTab("trading"));
    }
    const alertsBadge = byId("regimeAlertsBadge");
    if (alertsBadge) {
      const count = Number(payload.unread_alert_count || 0);
      alertsBadge.textContent = count > 0 ? `Alerts ${count}` : "Alerts";
      alertsBadge.style.display = count > 0 ? "" : "none";
    }
  }

  function ensembleLabel(name) {
    return String(name || "")
      .split("_")
      .map((part) => part ? `${part.slice(0, 1).toUpperCase()}${part.slice(1)}` : "")
      .join(" ");
  }

  function ensureEnsembleMount() {
    const analysisPanel = document.querySelector('[data-regime-panel="analysis"]');
    if (!analysisPanel) return null;
    let mount = byId("regimeEnsembleMount");
    if (mount) return mount;
    mount = document.createElement("div");
    mount.id = "regimeEnsembleMount";
    mount.style.marginTop = "14px";
    const anchor = byId("regimeDiffMount");
    analysisPanel.insertBefore(mount, anchor || null);
    return mount;
  }

  function renderEnsemblePanel(payload) {
    const mount = ensureEnsembleMount();
    if (!mount) return;
    if (!payload.ensemble_status && !state.ensembleWeights) {
      mount.innerHTML = "";
      return;
    }
    const weightsPayload = state.ensembleWeights || { analysts: [], aggregation_method: "mean" };
    const analysts = Array.isArray(weightsPayload.analysts)
      ? weightsPayload.analysts.filter((analyst) => String(analyst.name || "") !== "passthrough")
      : [];
    mount.innerHTML = `
      <section class="ui-card">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">Ensemble Analysts</div>
            <div class="ui-muted" style="margin-top:4px">Configure analyst participation and weighting for future ensemble aggregation.</div>
          </div>
          <div class="ui-muted">ML ${payload.ensemble_status && payload.ensemble_status.meta_labeler_active ? `Active v${payload.ensemble_status.meta_labeler_version || "?"}` : "Inactive"}</div>
        </div>
        <div style="display:grid; gap:12px; margin-top:12px">
          <label style="display:grid; gap:6px; max-width:220px">
            Aggregation
            <select id="regimeEnsembleAggregation">
              ${["mean", "min", "weighted"].map((method) => `<option value="${method}" ${weightsPayload.aggregation_method === method ? "selected" : ""}>${method}</option>`).join("")}
            </select>
          </label>
          <div style="display:grid; gap:10px">
            ${analysts.length ? analysts.map((analyst) => `
              <div class="ui-card" data-ensemble-row="${escapeHtml(analyst.name)}" style="padding:10px; border:1px solid #e5e7eb">
                <div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap">
                  <label style="display:flex; gap:8px; align-items:center; min-width:220px">
                    <input type="checkbox" data-ensemble-enabled ${analyst.enabled ? "checked" : ""} />
                    <strong>${escapeHtml(ensembleLabel(analyst.name))}</strong>
                  </label>
                  <span class="ui-badge ${analyst.ready ? "ui-badge--safe" : "ui-badge--neutral"}">${analyst.ready ? "Ready" : "Stub"}</span>
                  <div style="display:flex; gap:10px; align-items:center; flex:1; min-width:240px">
                    <input type="range" min="0" max="5" step="0.1" value="${escapeHtml(formatFixed(analyst.weight, 1))}" data-ensemble-weight ${analyst.enabled ? "" : "disabled"} style="flex:1" />
                    <span class="ui-tabular-nums" data-ensemble-weight-value>${escapeHtml(formatFixed(analyst.weight, 1))}</span>
                  </div>
                </div>
              </div>
            `).join("") : '<div class="ui-muted">No analysts registered.</div>'}
          </div>
          <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap">
            <button class="btn btn--secondary" id="regimeSaveEnsembleWeights" type="button">Save Weights</button>
            <span class="ui-muted">Stubs are visible now but have no live effect until implemented.</span>
          </div>
        </div>
      </section>
    `;
    mount.querySelectorAll("[data-ensemble-row]").forEach((row) => {
      const checkbox = row.querySelector("[data-ensemble-enabled]");
      const slider = row.querySelector("[data-ensemble-weight]");
      const value = row.querySelector("[data-ensemble-weight-value]");
      if (slider && value) {
        slider.addEventListener("input", () => {
          value.textContent = formatFixed(slider.value, 1);
        });
      }
      if (checkbox && slider) {
        checkbox.addEventListener("change", () => {
          slider.disabled = !checkbox.checked;
        });
      }
    });
    const saveButton = byId("regimeSaveEnsembleWeights");
    if (saveButton) {
      saveButton.addEventListener("click", saveEnsembleWeights);
    }
  }

  function renderHeroStrip(payload) {
    const strip = byId("regimeHeroStrip");
    if (!strip) return;
    const exposure = payload.regime_exposure || {};
    const bullPct = Math.max(0, Number(exposure.Bull || 0) * 100);
    const neutralPct = Math.max(0, Number(exposure.Neutral || 0) * 100);
    const bearPct = Math.max(0, Number(exposure.Bear || 0) * 100);
    const tone = exposureTone(payload);
    strip.className = `regime-hero-strip ${tone.label.includes("Bull") ? "regime-hero-strip--bull" : tone.label.includes("Bear") ? "regime-hero-strip--bear" : "regime-hero-strip--neutral"}`;
    const bullSeg = strip.querySelector(".regime-hero-strip__segment--bull");
    const neutralSeg = strip.querySelector(".regime-hero-strip__segment--neutral");
    const bearSeg = strip.querySelector(".regime-hero-strip__segment--bear");
    if (bullSeg) bullSeg.style.width = `${bullPct.toFixed(1)}%`;
    if (neutralSeg) neutralSeg.style.width = `${neutralPct.toFixed(1)}%`;
    if (bearSeg) bearSeg.style.width = `${bearPct.toFixed(1)}%`;
    const value = byId("regimeHeroValue");
    const stats = byId("regimeHeroStats");
    if (value) value.textContent = tone.label;
    if (stats) stats.textContent = `${exposureText(payload)} · Transition Risk: ${formatFixed(payload.aggregate_transition_risk_pct || 0, 1)}% · ${formatCurrency(payload.total_market_value || 0, 0)}`;
  }

  function renderPortfolioSummary(payload) {
    const mount = byId("regimePortfolioSummaryMount");
    if (!mount) return;
    const summary = payload.portfolio_summary || null;
    if (!summary) {
      mount.innerHTML = "";
      return;
    }
    const exposure = summary.regime_exposure || {};
    const bullPct = Number(exposure.bull_pct || 0) * 100;
    const neutralPct = Number(exposure.neutral_pct || 0) * 100;
    const bearPct = Number(exposure.bear_pct || 0) * 100;
    const diversification = Number(summary.diversification_score || 0);
    const diversificationTone = diversification > 0.7 ? COLORS.bull : diversification >= 0.4 ? COLORS.warn : COLORS.bear;
    const sectors = Array.isArray(summary.sector_concentration) ? summary.sector_concentration : [];
    const flags = Array.isArray(summary.risk_flags) ? summary.risk_flags.filter(Boolean) : [];
    const clusters = Array.isArray((summary.correlation_risk || {}).clusters) ? (summary.correlation_risk || {}).clusters : [];
    mount.innerHTML = `
      <section class="ui-card">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">Portfolio Summary</div>
            <div class="ui-muted" style="margin-top:4px">Aggregate exposure, diversification, sector concentration, and transition risk.</div>
          </div>
          <div class="ui-muted">Portfolio signal: ${escapeHtml(summary.portfolio_composite_signal || "Hold")}</div>
        </div>
        <div class="grid2" style="margin-top:12px">
          <div>
            <div class="ui-muted">Portfolio Regime Exposure</div>
            <div style="margin-top:8px; display:flex; width:100%; height:14px; border-radius:999px; overflow:hidden; background:${COLORS.border}">
              <div style="width:${bullPct.toFixed(1)}%; background:${COLORS.bull}" title="Bull ${bullPct.toFixed(1)}%"></div>
              <div style="width:${neutralPct.toFixed(1)}%; background:${COLORS.neutral}" title="Neutral ${neutralPct.toFixed(1)}%"></div>
              <div style="width:${bearPct.toFixed(1)}%; background:${COLORS.bear}" title="Bear ${bearPct.toFixed(1)}%"></div>
            </div>
            <div class="ui-muted" style="margin-top:6px">Bull ${bullPct.toFixed(0)}% · Neutral ${neutralPct.toFixed(0)}% · Bear ${bearPct.toFixed(0)}%</div>
            <div class="ui-muted" style="margin-top:6px">Portfolio Transition Risk: ${escapeHtml(formatSignedPct(summary.aggregate_transition_risk, 1))}</div>
          </div>
          <div>
            <div class="ui-muted">Portfolio Diversification</div>
            <div style="font-size:1.4rem; font-weight:700; color:${diversificationTone}; margin-top:8px">${diversification.toFixed(2)}</div>
            <div class="ui-muted" style="margin-top:4px">${escapeHtml((summary.correlation_risk || {}).warning || "Regime mix is acceptable.")}</div>
            ${flags.length ? `<div class="ui-muted" style="margin-top:8px">${escapeHtml(flags.join(" · "))}</div>` : ""}
            ${clusters.length ? `<div style="display:grid; gap:6px; margin-top:10px">${clusters.filter((row) => Number(row.pct_of_portfolio || 0) >= 0.25).map((row) => `<div class="${Number(row.pct_of_portfolio || 0) >= 0.4 ? "banner--warn" : "ui-card"}" style="padding:8px; border-radius:10px">${escapeHtml(`${row.tickers.length} ${row.sector} holdings (${row.tickers.join(", ")}) share ${row.regime} regimes — ${(Number(row.pct_of_portfolio || 0) * 100).toFixed(0)}% of portfolio value.`)}</div>`).join("")}</div>` : ""}
          </div>
        </div>
        ${sectors.length ? `
          <div class="table-wrap" style="margin-top:12px">
            <table>
              <thead>
                <tr>
                  <th scope="col">Sector</th>
                  <th scope="col" class="num">Value</th>
                  <th scope="col" class="num">Bull %</th>
                  <th scope="col" class="num">Neutral %</th>
                  <th scope="col" class="num">Bear %</th>
                  <th scope="col">Flag</th>
                </tr>
              </thead>
              <tbody>
                ${sectors.map((row) => `
                  <tr>
                    <td>${escapeHtml(row.sector || "Unknown")}</td>
                    <td class="num ui-tabular-nums">${escapeHtml(formatCurrency(row.value || 0, 0))}</td>
                    <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(row.bull_pct, 0))}</td>
                    <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(row.neutral_pct, 0))}</td>
                    <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(row.bear_pct, 0))}</td>
                    <td class="${row.flag ? "cell-bad" : ""}">${escapeHtml(row.flag || "—")}</td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
          </div>
        ` : ""}
      </section>
    `;
  }

  function renderHeatmap(payload) {
    const mount = byId("regimeHeatmapMount");
    if (!mount) return;
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    if (!rows.length) {
      mount.innerHTML = "";
      return;
    }
    mount.innerHTML = `
      <section class="ui-card">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">Regime Heatmap</div>
            <div class="ui-muted" style="margin-top:4px">Primary summary of current regime state, confidence, and relative strength.</div>
          </div>
        </div>
        <div class="table-wrap" style="margin-top:12px">
          <table>
            <thead>
              <tr>
                <th scope="col">Ticker</th>
                <th scope="col">Regime</th>
                <th scope="col" class="num">Probability</th>
                <th scope="col">Relative Strength</th>
              </tr>
            </thead>
            <tbody>
              ${rows.map((row) => `
                <tr>
                  <td class="nowrap">${escapeHtml(row.ticker)}</td>
                  <td class="nowrap ${escapeHtml(row.regime_class || "")}">${escapeHtml(row.regime)}</td>
                  <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(Number(row.probability || 0), 1))}</td>
                  <td class="${escapeHtml(row.relative_strength_class || "")}">${escapeHtml(row.relative_strength || "In-line")}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      </section>
    `;
  }

  function renderWarnings(warnings) {
    const existing = document.querySelector(".holdings-header details.alert--warn");
    if (!existing) return;
    if (!warnings || !warnings.length) {
      existing.open = false;
      existing.style.display = "none";
      return;
    }
    existing.style.display = "";
    existing.open = true;
    existing.querySelector("summary").textContent = `Notes (${warnings.length})`;
    const list = existing.querySelector("ul");
    if (!list) return;
    list.innerHTML = warnings.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
  }

  function renderCachedNote(payload) {
    const note = byId("regimeCachedNote");
    if (!note) return;
    if (payload.job_status === "cached" && payload.cached_note) {
      note.textContent = payload.cached_note;
      note.style.display = "";
      return;
    }
    note.textContent = "";
    note.style.display = "none";
  }

  function openTickerDetail(ticker, focusFrontier = false) {
    const detail = document.querySelector(`[data-regime-detail="${CSS.escape(String(ticker || ""))}"]`);
    if (!detail) return;
    detail.open = true;
    renderChartsForTicker(String(ticker || ""));
    detail.scrollIntoView({ block: "start", behavior: "smooth" });
    if (focusFrontier) {
      window.setTimeout(() => {
        const frontier = detail.querySelector("[data-regime-frontier-panel]");
        if (frontier) frontier.scrollIntoView({ block: "start", behavior: "smooth" });
      }, 120);
    }
  }

  function renderDiffPanel(payload) {
    const mount = byId("regimeDiffMount");
    if (!mount) return;
    const diff = payload.run_diff || {};
    const changes = Array.isArray(diff.changes) ? diff.changes : [];
    if (!diff.has_previous && !changes.length) {
      mount.innerHTML = "";
      return;
    }
    mount.innerHTML = `
      <div class="ui-card" style="padding:12px">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">What Changed</div>
            <div class="ui-muted" style="margin-top:4px">${escapeHtml(diff.summary || "No material changes vs previous run.")}</div>
          </div>
          ${payload.snapshots_saved ? `<div class="ui-muted">Snapshots saved: ${escapeHtml(payload.snapshots_saved)}</div>` : ""}
        </div>
        ${changes.length ? `
          <ul class="ui-muted" style="margin:10px 0 0 18px">
            ${changes.map((change) => `<li>${escapeHtml(change.message || "")}</li>`).join("")}
          </ul>
        ` : '<div class="ui-muted" style="margin-top:10px">No material changes vs previous run.</div>'}
      </div>
    `;
  }

  function renderTable(payload) {
    const mount = byId("regimeTableMount");
    if (!mount) return;
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    if (!rows.length) {
      mount.innerHTML = '<div class="ui-muted">No regime rows available for this run.</div>';
      return;
    }
    mount.innerHTML = `
      <div class="table-wrap regime-table" role="region" aria-label="Regime table" data-regime-sort-table>
        <table class="regime-table">
          <thead>
            <tr>
              <th scope="col" data-sort="text">Ticker</th>
              <th scope="col" data-sort="text">Regime</th>
              <th scope="col" data-sort="text">Weekly Regime</th>
              <th scope="col" class="num" data-sort="num">Probability</th>
              <th scope="col" data-sort="text">ML</th>
              <th scope="col" data-sort="text">Composite Signal</th>
              <th scope="col" data-sort="text">Forward Signal</th>
              <th scope="col" data-sort="text">Technical Signal</th>
              <th scope="col" data-sort="text">Sentiment</th>
              <th scope="col" data-sort="text">Action</th>
              <th scope="col" data-sort="text" data-secondary-col="1">Tax Status</th>
              <th scope="col" class="num" data-sort="num" data-secondary-col="1">Days in Regime</th>
              <th scope="col" data-sort="text">Rel. Strength</th>
              <th scope="col" data-sort="text">AI Verdict</th>
              <th scope="col" data-sort="num">Stop</th>
              <th scope="col" data-sort="text" data-secondary-col="1">Next Earnings</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((row) => `
              <tr class="regime-holding-row" data-regime-diagnostics-toggle="${escapeHtml(row.ticker)}" style="cursor:pointer">
                <td class="nowrap">${escapeHtml(row.ticker)}</td>
                <td class="nowrap regime-table__regime-cell ${escapeHtml(row.regime_class || "")}" style="${row.regime === "Bull" ? `background:${COLORS.bullBg}; color:${COLORS.pnlPositive};` : row.regime === "Bear" ? `background:${COLORS.bearBg}; color:${COLORS.pnlNegative};` : `background:${COLORS.neutralBg}; color:${COLORS.muted};`}">${escapeHtml(row.regime)}</td>
                <td class="nowrap">${escapeHtml(row.weekly_regime || "—")} ${divergenceBadge(row)}</td>
                <td class="num ui-tabular-nums" data-sort-val="${escapeHtml(row.probability_pct)}">${Number(row.probability_pct || 0).toFixed(1)}%</td>
                <td class="nowrap">
                  ${row.meta_labeler_probability != null
                    ? `<span class="${badgeClass(String(row.meta_labeler_signal || "").toLowerCase() === "confirm" ? "Bull" : String(row.meta_labeler_signal || "").toLowerCase() === "veto" ? "Bear" : "Neutral")}">ML ${(Number(row.meta_labeler_probability) * 100).toFixed(0)}%</span>`
                    : '<span class="ui-muted">Inactive</span>'}
                </td>
                <td class="nowrap ${escapeHtml(row.composite_signal_class || signalClass(row.composite_signal))}">${escapeHtml(row.composite_signal)}</td>
                <td class="nowrap ${escapeHtml(row.forward_signal_class || signalClass(row.forward_signal))}">${escapeHtml(row.forward_signal)}</td>
                <td>${escapeHtml(row.technical_signal)}</td>
                <td class="${escapeHtml(row.sentiment_class || "")}">${escapeHtml(row.sentiment_trend || "not available")}</td>
                <td class="nowrap ${escapeHtml(row.action_class || row.tax_action_class || signalClass(row.action || row.tax_action))}">${escapeHtml(row.action || row.tax_action || "—")}</td>
                <td class="nowrap" data-secondary-col="1">${escapeHtml(row.tax_status || "—")}</td>
                <td class="num ui-tabular-nums" data-secondary-col="1" data-sort-val="${escapeHtml(row.days_in_regime)}">${escapeHtml(row.days_in_regime)}</td>
                <td class="nowrap ${row.relative_strength === "Outperforming" ? "cell-ok" : row.relative_strength === "Lagging" ? "cell-bad" : ""}">${row.relative_strength === "Outperforming" ? "↑ " : row.relative_strength === "Lagging" ? "↓ " : "→ "}${escapeHtml(row.relative_strength || "In-line")}</td>
                <td class="nowrap">
                  ${row.ai_verdict ? `<button type="button" class="btn btn--secondary" data-regime-focus-frontier="${escapeHtml(row.ticker)}" style="padding:2px 8px">${escapeHtml(row.ai_verdict)}</button>${row.frontier && row.frontier.llm_override ? `<span class="ui-badge ui-badge--bad" style="font-size:10px; margin-left:4px" title="${escapeHtml(row.frontier.llm_override_reason || "ML confidence below threshold")}">ML Override</span>` : ""}` : '<span class="ui-muted">—</span>'}
                </td>
                <td class="nowrap ${row.stop_proximity && row.stop_proximity.level === "critical" ? "cell-bad regime-table__stop--critical" : row.stop_proximity && row.stop_proximity.level === "warning" ? "regime-table__stop--warning" : "cell-ok regime-table__stop--safe"}" data-sort-val="${escapeHtml(row.stop_proximity ? row.stop_proximity.distance_pct : 999)}">
                  ${row.stop_proximity ? escapeHtml(row.stop_proximity.label) : "—"}
                </td>
                <td class="nowrap ui-tabular-nums" data-secondary-col="1">${escapeHtml(row.earnings_date ? String(row.earnings_date).slice(0, 10) : "N/A")}</td>
              </tr>
              ${state.expandedDiagnosticsTicker === row.ticker ? renderDiagnosticsRow(row) : ""}
            `).join("")}
          </tbody>
        </table>
      </div>
    `;
    wireSortTables();
    applyTableColumnVisibility();
    mount.querySelectorAll("[data-regime-focus-frontier]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.stopPropagation();
        openTickerDetail(String(button.getAttribute("data-regime-focus-frontier") || ""), true);
      });
    });
    mount.querySelectorAll("[data-regime-diagnostics-toggle]").forEach((rowEl) => {
      rowEl.addEventListener("click", (event) => {
        if (event.target.closest("button, a, input, select, textarea, label")) return;
        const ticker = String(rowEl.getAttribute("data-regime-diagnostics-toggle") || "");
        state.expandedDiagnosticsTicker = state.expandedDiagnosticsTicker === ticker ? null : ticker;
        renderTable(state.lastPayload || payload);
      });
    });
  }

  function renderDiagnosticsRow(row) {
    const diag = row.signal_diagnostics || {};
    return `
      <tr class="regime-diagnostics-row">
        <td colspan="16">
          <div class="ui-card" style="padding:12px; margin:6px 0; background:${COLORS.neutralBg}">
            <div class="ui-section-title">Signal Diagnostics · ${escapeHtml(row.ticker || "")}</div>
            <div class="ui-muted" style="margin-top:4px">Forward signal → Technical signal → Composite signal</div>
            <div style="display:grid; grid-template-columns:repeat(4, minmax(0,1fr)); gap:10px; margin-top:10px">
              <div class="ui-card" style="padding:10px; border:1px solid ${COLORS.border}">
                <div style="font-weight:600">Forward Signal</div>
                <div class="${signalClass(diag.forward_action)}" style="margin-top:6px">${escapeHtml(diag.forward_action || "—")}</div>
                <div class="ui-muted" style="margin-top:6px">Strength ${escapeHtml(formatFixed(diag.forward_strength, 3))}</div>
                <div class="ui-muted">Transition risk ${escapeHtml(formatFixed(diag.forward_transition_risk, 3))}</div>
                <div class="ui-muted">Expected duration ${escapeHtml(formatFixed(diag.forward_expected_duration, 1))}d</div>
              </div>
              <div class="ui-card" style="padding:10px; border:1px solid ${COLORS.border}">
                <div style="font-weight:600">Technical Signal</div>
                <div style="margin-top:6px">${escapeHtml(diag.technical_signal || "—")}</div>
              </div>
              <div class="ui-card" style="padding:10px; border:1px solid ${COLORS.border}">
                <div style="font-weight:600">Composite Signal</div>
                <div class="${signalClass(diag.composite_action)}" style="margin-top:6px">${escapeHtml(diag.composite_action || "—")}</div>
                <div class="ui-muted" style="margin-top:6px">Strength ${escapeHtml(formatFixed(diag.composite_strength, 3))}</div>
              </div>
              <div class="ui-card" style="padding:10px; border:1px solid ${COLORS.border}">
                <div style="font-weight:600">Meta-Labeler</div>
                <div class="${signalClass(String(diag.meta_labeler_signal || "").toLowerCase() === "confirm" ? "Buy" : String(diag.meta_labeler_signal || "").toLowerCase() === "veto" ? "Sell" : "Hold")}" style="margin-top:6px">${escapeHtml(diag.meta_labeler_signal || "Inactive")}</div>
                <div class="ui-muted" style="margin-top:6px">Probability ${diag.meta_labeler_probability != null ? escapeHtml(formatSignedPct(Number(diag.meta_labeler_probability || 0), 1)) : "—"}</div>
              </div>
            </div>
            <div class="ui-card" style="padding:10px; margin-top:10px; border:1px solid ${COLORS.border}">
              <div style="font-weight:600">Threshold Path</div>
              <div class="ui-muted" style="margin-top:6px">${escapeHtml(diag.thresholds_applied || "Unavailable")}</div>
            </div>
            <div class="ui-muted" style="display:flex; gap:12px; flex-wrap:wrap; margin-top:10px">
              <span>Regime ${escapeHtml(diag.regime || "—")}</span>
              <span>Probability ${escapeHtml(formatSignedPct(Number(diag.probability || 0), 1))}</span>
              <span>Regime days ${escapeHtml(diag.regime_days || "—")}</span>
              <span>Weekly ${escapeHtml(diag.weekly_regime || "—")}</span>
              <span>${escapeHtml(diag.multi_timeframe_note || "—")}</span>
            </div>
          </div>
        </td>
      </tr>
    `;
  }

  function applyTableColumnVisibility() {
    document.querySelectorAll("[data-secondary-col]").forEach((cell) => {
      cell.style.display = state.showAllTableColumns ? "" : "none";
    });
    const toggle = byId("regimeToggleColumns");
    if (toggle) {
      toggle.textContent = state.showAllTableColumns ? "Hide secondary columns" : "Show all columns";
    }
  }

  function formatSignedPct(value, digits = 2) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "—";
    return `${(num * 100).toFixed(digits)}%`;
  }

  function formatFixed(value, digits = 2) {
    const num = Number(value);
    return Number.isFinite(num) ? num.toFixed(digits) : "—";
  }

  function formatCurrency(value, digits = 2) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "—";
    return num.toLocaleString(undefined, { style: "currency", currency: "USD", minimumFractionDigits: digits, maximumFractionDigits: digits });
  }

  function renderRelativeStrength(payload) {
    const mount = byId("regimeRelativeStrengthMount");
    if (!mount) return;
    const relative = payload.relative_strength || {};
    if (!relative.summary) {
      mount.innerHTML = "";
      return;
    }
    const tone = Array.isArray(relative.outperforming) && relative.outperforming.length ? "banner--ok" : "banner--warn";
    mount.innerHTML = `<div class="${tone}" style="padding:10px; border-radius:10px">${escapeHtml(relative.summary)}</div>`;
  }

  function renderActionItemsBar(payload) {
    const mount = byId("regimeActionItemsBar");
    if (!mount) return;
    const digest = payload.digest || {};
    const items = Array.isArray(digest.action_items) ? digest.action_items : [];
    if (!items.length) {
      mount.innerHTML = "";
      return;
    }
    mount.innerHTML = `
      <div class="regime-action-bar">
        <div class="regime-action-bar__icon">⚠</div>
        <div class="regime-action-bar__content">
          <div class="regime-action-bar__title">${items.length} Action${items.length > 1 ? "s" : ""} Required</div>
          <div class="regime-action-bar__items">
            ${items.map((item) => `<div class="regime-action-bar__item"><span class="regime-action-bar__detail">${escapeHtml(item)}</span></div>`).join("")}
          </div>
        </div>
        <button class="btn btn--secondary regime-action-bar__dismiss" id="regimeActionBarDismiss" type="button">Dismiss</button>
      </div>
    `;
    const dismiss = byId("regimeActionBarDismiss");
    if (dismiss) dismiss.addEventListener("click", () => { mount.innerHTML = ""; });
  }

  function renderThemeHealth(payload) {
    const mount = byId("regimeThemeHealthMount");
    if (!mount) return;
    const themes = Array.isArray(payload.theme_health) ? payload.theme_health : [];
    if (!themes.length) {
      mount.innerHTML = "";
      return;
    }
    mount.innerHTML = `
      <div class="ui-card" style="padding:12px">
        <div class="ui-section-title">Theme Health</div>
        <div style="display:grid; gap:10px; margin-top:10px">
          ${themes.map((theme) => {
            const summary = theme.regime_summary || {};
            const bull = Number((summary.Bull || {}).weight_pct || 0);
            const neutral = Number((summary.Neutral || {}).weight_pct || 0);
            const bear = Number((summary.Bear || {}).weight_pct || 0);
            return `
              <div style="border:1px solid ${COLORS.border}; border-radius:12px; padding:10px">
                <div class="table-toolbar">
                  <div>
                    <div style="font-weight:700">${escapeHtml(theme.name || "")}</div>
                    <div class="ui-muted">Conviction ${escapeHtml(theme.conviction || 0)}/5 · Core ${escapeHtml((theme.role_counts || {}).Core || 0)} · Critical-Path ${escapeHtml((theme.role_counts || {})["Critical-Path"] || 0)} · Speculative ${escapeHtml((theme.role_counts || {}).Speculative || 0)}</div>
                  </div>
                  <div class="ui-muted">${escapeHtml(theme.ticker_count || 0)} ticker(s)</div>
                </div>
                <div style="margin-top:8px; display:flex; width:100%; height:10px; border-radius:999px; overflow:hidden; background:#e5e7eb">
                  <div style="width:${bull.toFixed(1)}%; background:${COLORS.bull}"></div>
                  <div style="width:${neutral.toFixed(1)}%; background:${COLORS.neutral}"></div>
                  <div style="width:${bear.toFixed(1)}%; background:${COLORS.bear}"></div>
                </div>
                ${theme.health_warning ? `<div class="banner--warn" style="padding:8px; border-radius:10px; margin-top:8px">${escapeHtml(theme.health_warning)}</div>` : ""}
              </div>
            `;
          }).join("")}
        </div>
      </div>
    `;
  }

  function renderMathPanel(row) {
    const math = row.math || {};
    const targets = row.price_targets || {};
    const duration = row.duration_accuracy || {};
    const stateRows = Array.isArray(math.state_statistics) ? math.state_statistics : [];
    const currentPriceTone = String(targets.price_position || "").toLowerCase().includes("below stop") ? COLORS.bear : String(targets.price_position || "").toLowerCase().includes("above exit") || String(targets.price_position || "").toLowerCase().includes("below entry") ? COLORS.warn : COLORS.bull;
    const stopDistance = Number(targets.current_price) > 0 && Number(targets.stop_price) > 0
      ? Math.abs(Number(targets.current_price) - Number(targets.stop_price)) / Math.abs(Number(targets.stop_price))
      : NaN;
    return `
      <div class="ui-card" style="padding:14px">
        <div class="ui-section-title">The Math</div>
        ${math.regime_inconsistency_warning ? `<div class="banner--warn" style="padding:8px; border-radius:10px; margin-top:8px">${escapeHtml(math.regime_inconsistency_warning)}</div>` : ""}
        ${stateRows.length ? `
          <div class="table-wrap" style="margin-top:10px">
            <table>
              <thead>
                <tr>
                  <th scope="col">State</th>
                  <th scope="col" class="num">Mean Return</th>
                  <th scope="col" class="num">Exp. Vol</th>
                  <th scope="col" class="num">Volume Z</th>
                </tr>
              </thead>
              <tbody>
                ${stateRows.map((item) => `
                  <tr${Number(item.state_id) === Number(row.state_id) ? ' style="font-weight:700"' : ""}>
                    <td class="nowrap">${escapeHtml(item.state_id)}${item.canonical_label ? ` · ${escapeHtml(item.canonical_label)}` : ""}</td>
                    <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(item.mean_return, 2))}</td>
                    <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(item.expected_volatility, 2))}</td>
                    <td class="num ui-tabular-nums">${escapeHtml(formatFixed(item.volume_zscore, 2))}</td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
          </div>
        ` : '<div class="ui-muted" style="margin-top:8px">State statistics unavailable.</div>'}
        <div style="display:grid; gap:6px; margin-top:10px">
          <div class="ui-muted">Mean Return: ${escapeHtml(formatSignedPct(math.mean_return, 2))}</div>
          <div class="ui-muted">Expected Volatility: ${escapeHtml(formatSignedPct(math.expected_volatility, 2))}</div>
          <div class="ui-muted">Volume Z-Score: ${escapeHtml(formatFixed(math.volume_zscore, 2))}</div>
          <div class="ui-muted">Recent 20-observation mean return: ${escapeHtml(formatSignedPct(math.recent_state_mean_return, 2))}</div>
          <div class="ui-muted">Regime entry date: ${escapeHtml(math.regime_entry_date || "—")}</div>
          <div class="ui-muted">Regime streak: ${escapeHtml(math.regime_streak_days || row.days_in_regime)} day(s)</div>
          ${duration.historical_avg != null ? `<div class="ui-muted" style="color:${Math.abs((Number(duration.expected || 0) - Number(duration.historical_avg || 0)) / Math.max(Number(duration.historical_avg || 1), 1)) <= 0.2 ? COLORS.bull : COLORS.warn}">Historical average: ${escapeHtml(formatFixed(duration.historical_avg, 1))} day(s)</div>` : ""}
          ${duration.accuracy_note ? `<div class="ui-muted">${escapeHtml(duration.accuracy_note)}</div>` : ""}
        </div>
        ${row.earnings_warning ? `<div class="banner--warn" style="padding:8px; border-radius:10px; margin-top:12px">${escapeHtml(row.earnings_warning)}</div>` : ""}
        <div style="display:grid; gap:6px; margin-top:12px; padding-top:10px; border-top:1px solid #e5e7eb">
          <div class="ui-section-title" style="font-size:0.95rem">Price Targets</div>
          ${row.price_targets_error ? `<div class="banner--warn" style="padding:8px; border-radius:10px; margin-bottom:4px">Price targets unavailable for this run: ${escapeHtml(row.price_targets_error)}</div>` : ""}
          <div style="font-weight:700; color:${currentPriceTone}">Current Price: ${escapeHtml(formatCurrency(targets.current_price ?? row.current_price, 2))}</div>
          <div class="ui-muted">${escapeHtml(targets.price_position || "Monitoring")}</div>
          <div class="ui-muted">Entry: ${escapeHtml(formatCurrency(targets.entry_price, 2))}</div>
          <div class="ui-muted">Exit: ${escapeHtml(formatCurrency(targets.exit_price, 2))}</div>
          <div class="ui-muted" style="${Number.isFinite(stopDistance) && stopDistance <= 0.05 ? `color:${COLORS.bear}; font-weight:700;` : ""}">Stop: ${escapeHtml(formatCurrency(targets.stop_price, 2))}</div>
          <div class="ui-muted">Risk / Reward: ${escapeHtml(formatFixed(targets.risk_reward_ratio, 2))}</div>
          <div class="ui-muted">Timeframe: ${escapeHtml(targets.timeframe_days || "—")} day(s)</div>
          ${Number.isFinite(stopDistance) && stopDistance <= 0.05 ? `<div class="banner--warn" style="padding:8px; border-radius:10px; margin-top:4px">Price is ${(stopDistance * 100).toFixed(1)}% from stop level.</div>` : ""}
        </div>
        ${row.risk_reward_conflict ? `<div class="banner--warn" style="padding:8px; border-radius:10px; margin-top:12px">${escapeHtml(row.risk_reward_warning || "Signal and risk/reward are in conflict.")}</div>` : ""}
        ${row.concentration_warning ? `<div class="banner--warn" style="padding:8px; border-radius:10px; margin-top:12px">${escapeHtml(row.concentration_warning)}</div>` : ""}
      </div>
    `;
  }

  function renderFrontierPanel(row) {
    const frontier = row.frontier;
    if (!frontier) {
      return `
        <div class="ui-card" style="padding:14px" data-regime-frontier-panel="${escapeHtml(row.ticker)}">
          <div class="ui-section-title">The Frontier Analysis</div>
          <div class="ui-muted" style="margin-top:8px">Frontier analysis disabled for this run.</div>
        </div>
      `;
    }
    const thesis = frontier.thesis_check || null;
    const institutional = frontier.institutional_report || {};
    const catalysts = Array.isArray(frontier.catalysts) ? frontier.catalysts : [];
    return `
      <div class="ui-card" style="padding:14px" data-regime-frontier-panel="${escapeHtml(row.ticker)}">
        <div class="ui-section-title">The Frontier Analysis</div>
        <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-top:4px">
          ${frontier.model_name ? `<div class="ui-muted">${escapeHtml(frontier.model_name)}</div>` : ""}
          <span class="${frontier.source === "meta_labeler_override" ? "ui-badge ui-badge--bad" : frontier.source === "vader_fallback" ? "ui-badge ui-badge--warn" : "ui-badge ui-badge--safe"}">${frontier.source === "meta_labeler_override" ? "ML Override" : frontier.source === "vader_fallback" ? "Heuristic Estimate" : "AI Analysis"}</span>
          ${frontier.llm_override ? `<span class="ui-badge ui-badge--bad" title="${escapeHtml(frontier.llm_override_reason || "ML confidence below threshold")}">Bypassed</span>` : ""}
        </div>
        <div class="${frontier.verdict_overridden ? "banner--warn" : "banner--ok"}" style="padding:8px; border-radius:10px; margin-top:8px">
          ${escapeHtml(thesis && thesis.answer ? thesis.answer : "Thesis check not triggered; current regime remains the working assumption.")}
        </div>
        <div style="margin-top:12px">
          <div class="ui-section-title" style="font-size:0.95rem">Catalyst Summary</div>
          ${catalysts.length ? `
            <ul class="ui-muted" style="margin:8px 0 0 18px">
              ${catalysts.slice(0, 5).map((item) => `<li>${item.link ? `<a href="${escapeHtml(item.link)}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.title || "Untitled")}</a>` : escapeHtml(item.title || "Untitled")}</li>`).join("")}
            </ul>
          ` : '<div class="ui-muted" style="margin-top:8px">No catalysts available.</div>'}
        </div>
        <div style="display:grid; gap:6px; margin-top:12px">
          <div><strong>Regime Validation:</strong> <span class="ui-muted">${escapeHtml(institutional.regime_validation || "Unavailable")}</span></div>
          <div><strong>Divergence Check:</strong> <span class="ui-muted">${escapeHtml(institutional.divergence_check || "None")}</span></div>
          <div><strong>Actionable Verdict:</strong> <span class="${frontier.verdict_overridden ? "cell-bad" : ""}">${escapeHtml(frontier.display_verdict || "Hold")}</span></div>
          <div><strong>Risk Trigger:</strong> <span class="ui-muted">${escapeHtml(institutional.risk_trigger || "Unavailable")}</span></div>
          <div><strong>Thesis Alignment:</strong> <span class="ui-muted">${escapeHtml(institutional.thesis_alignment || "Unavailable")}</span></div>
        </div>
        <div style="margin-top:12px">
          <div class="ui-section-title" style="font-size:0.95rem">Confidence</div>
          <div style="margin-top:8px; background:#e5e7eb; border-radius:999px; height:10px; overflow:hidden">
            <div style="height:100%; width:${Math.max(0, Math.min(100, Number((row.unified_confidence || {}).value || frontier.confidence_pct || 0)))}%; background:#111827"></div>
          </div>
          <div class="ui-muted" style="margin-top:6px">Confidence: ${escapeHtml(formatFixed((row.unified_confidence || {}).value, 1))}/100 ${row.unified_confidence && row.unified_confidence.label ? `· ${escapeHtml(row.unified_confidence.label)}` : ""}</div>
          <div style="margin-top:6px; color:${escapeHtml((frontier.sizing_guidance || {}).color || COLORS.ink)}; font-weight:700">${escapeHtml((frontier.sizing_guidance || {}).text || "")}</div>
        </div>
      </div>
    `;
  }

  function detailBody(row) {
    const taxSignals = Array.isArray(row.account_tax_signals) ? row.account_tax_signals : [];
    const lots = Array.isArray(row.lot_details) ? row.lot_details : [];
    const actionText = row.action || row.tax_action || "—";
    return `
      <div style="display:grid; gap:12px; margin-top:12px">
        <div>${renderMathPanel(row)}</div>
        <div>${renderFrontierPanel(row)}</div>
      </div>
      ${row.position_size ? `
        <div class="ui-card" style="padding:14px; margin-top:12px">
          <div class="ui-section-title">Position Sizing</div>
          <div class="ui-muted" style="margin-top:6px">Suggested allocation: ${escapeHtml(formatFixed(row.position_size.suggested_pct, 1))}%${row.position_size.suggested_dollars != null ? ` (${escapeHtml(formatCurrency(row.position_size.suggested_dollars, 0))})` : ""}</div>
          <div class="ui-muted">Max risk: ${escapeHtml(formatCurrency(row.position_size.max_loss_dollars, 0))}</div>
          <div class="ui-muted">Kelly fraction: ${row.position_size.kelly_fraction != null ? escapeHtml(formatSignedPct(row.position_size.kelly_fraction, 1)) : "—"}</div>
          <div class="ui-muted">ML confidence: ${row.position_size.meta_labeler_probability != null ? escapeHtml(formatSignedPct(row.position_size.meta_labeler_probability, 1)) : "Inactive"}</div>
          ${Number(row.position_size.portfolio_adjustment || 1) < 0.999 ? `<div class="banner--warn" style="padding:8px; border-radius:10px; margin-top:8px">Position reduced ${escapeHtml(((1 - Number(row.position_size.portfolio_adjustment || 1)) * 100).toFixed(0))}% for portfolio concentration.${row.position_size.adjustment_rationale ? ` ${escapeHtml(row.position_size.adjustment_rationale)}` : ""}</div>` : ""}
          <div class="ui-muted">${escapeHtml(row.position_size.sizing_rationale || "")}</div>
        </div>
      ` : ""}
      ${(Array.isArray(row.theme_membership) && row.theme_membership.length) ? `
        <div class="ui-card" style="padding:14px; margin-top:12px">
          <div class="ui-section-title">Theme Membership</div>
          <div style="display:grid; gap:8px; margin-top:8px">
            ${row.theme_membership.map((theme) => `
              <div style="border:1px solid ${COLORS.border}; border-radius:10px; padding:10px">
                <div style="font-weight:700">${escapeHtml(theme.theme_name || "")}</div>
                <div class="ui-muted">${escapeHtml(theme.role || "Core")} · ${escapeHtml(theme.time_horizon || "strategic")} · conviction ${escapeHtml(theme.conviction || 0)}/5</div>
                ${theme.rationale ? `<div class="ui-muted" style="margin-top:4px">${escapeHtml(theme.rationale)}</div>` : ""}
                <div class="ui-muted" style="margin-top:4px">ATR Target: ${escapeHtml(formatCurrency((row.price_targets || {}).exit_price, 2))}${theme.target_price != null ? ` · Thesis Target: ${escapeHtml(formatCurrency(theme.target_price, 2))}` : ""}${theme.stop_price != null ? ` · Theme Stop: ${escapeHtml(formatCurrency(theme.stop_price, 2))}` : ""}</div>
              </div>
            `).join("")}
          </div>
        </div>
      ` : ""}
      <div class="ui-card" style="padding:14px; margin-top:12px">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">Backtest</div>
            <div class="ui-muted" style="margin-top:4px">${escapeHtml(row.multi_timeframe_note || "Multi-timeframe note unavailable.")}</div>
            ${row.divergence_severity && row.divergence_severity.interpretation ? `<div class="ui-muted" style="margin-top:4px">${escapeHtml(row.divergence_severity.interpretation)}</div>` : ""}
          </div>
          <button class="btn btn--secondary" type="button" data-regime-backtest="${escapeHtml(row.ticker)}">Run Backtest</button>
        </div>
        <div id="regimeBacktestMount_${escapeHtml(row.ticker)}" class="ui-muted" style="margin-top:8px">Backtest results load on demand.</div>
      </div>
      ${(row.charts && (row.charts.price || row.charts.confidence || row.charts.transition)) ? `
        <div class="grid2" style="margin-top:12px">
          <div class="ui-card" style="padding:14px">
            <div class="ui-section-title">Interactive price chart</div>
            <div id="regimePlotlyPrice_${escapeHtml(row.ticker)}" style="min-height:440px"></div>
          </div>
          <div class="ui-card" style="padding:14px">
            <div class="ui-section-title">Transition heatmap</div>
            <div id="regimePlotlyTransition_${escapeHtml(row.ticker)}" style="min-height:280px"></div>
          </div>
        </div>
        <div class="ui-card" style="padding:14px; margin-top:12px">
          <div class="ui-section-title">Interactive confidence timeline</div>
          <div id="regimePlotlyConfidence_${escapeHtml(row.ticker)}" style="min-height:240px"></div>
        </div>
      ` : ""}
      <div class="grid2" style="margin-top:12px">
        <div class="ui-card" style="padding:14px">
          <div class="ui-section-title">Forward probability curve</div>
          <div class="ui-muted" style="margin-bottom:6px">Bull / Neutral / Bear over the next 21 trading days.</div>
          <div id="regimeForwardChart_${escapeHtml(row.ticker)}" data-regime-forward-chart="${escapeHtml(row.ticker)}" style="position:relative"></div>
          <script type="application/json" id="regimeChartData_${escapeHtml(row.ticker)}">${row.forward_curve_json}</script>
        </div>
        <div class="ui-card" style="padding:14px">
          <div class="ui-section-title">Confidence trajectory</div>
          <div class="ui-muted" style="margin-bottom:6px">Recent regime-probability trend.</div>
          <div id="regimeConfidenceChart_${escapeHtml(row.ticker)}" data-regime-confidence-chart="${escapeHtml(row.ticker)}" style="position:relative"></div>
          <script type="application/json" id="regimeConfidenceData_${escapeHtml(row.ticker)}">${row.confidence_curve_json}</script>
        </div>
      </div>
      <div class="grid2" style="margin-top:12px">
        <div class="ui-card" style="padding:14px">
          <div class="ui-section-title">Sentiment momentum</div>
          <div class="ui-muted" style="margin-bottom:6px">Recorded sentiment score history for this ticker.</div>
          <div id="regimeSentimentChart_${escapeHtml(row.ticker)}" data-regime-sentiment-chart="${escapeHtml(row.ticker)}" style="position:relative"></div>
          <script type="application/json" id="regimeSentimentData_${escapeHtml(row.ticker)}">${row.sentiment_history_json}</script>
        </div>
        <div class="ui-card" style="padding:14px">
          <div class="ui-section-title">Tax breakdown</div>
          <div class="ui-muted" style="margin-top:4px">Showing ${escapeHtml(row.open_lot_count || 0)} open lot(s)</div>
          <div class="ui-muted" style="margin-top:8px">Action: <span class="${escapeHtml(row.action_class || row.tax_action_class || signalClass(actionText))}">${escapeHtml(actionText)}</span> · Tax status: ${escapeHtml(row.tax_status || "—")}</div>
          ${taxSignals.length ? `
            <div style="display:grid; gap:8px; margin-top:8px">
              ${taxSignals.map((signal) => `
                <div style="border:1px solid #e5e7eb;border-radius:10px;padding:10px">
                  <div style="font-weight:600">${escapeHtml(signal.account_name || "Unknown")} / ${escapeHtml(signal.account_type || "Unknown")}</div>
                  <div class="ui-muted">${escapeHtml(signal.adjusted_action || "—")}${signal.tax_note ? ` · ${escapeHtml(signal.tax_note)}` : ""}</div>
                  ${signal.ltcg_threshold_date ? `<div class="ui-muted">LTCG threshold: ${escapeHtml(signal.ltcg_threshold_date)}</div>` : ""}
                  ${signal.wash_sale_warning ? `<div class="ui-muted">${escapeHtml(signal.wash_sale_warning)}</div>` : ""}
                </div>
              `).join("")}
            </div>
          ` : '<div class="ui-muted" style="margin-top:8px">—</div>'}
          ${lots.length ? `
            <div class="table-wrap" style="margin-top:12px">
              <table>
                <thead>
                  <tr>
                    <th scope="col">Acct</th>
                    <th scope="col">Acquired</th>
                    <th scope="col" class="num">Qty</th>
                    <th scope="col" class="num">Cost Basis</th>
                    <th scope="col">Term</th>
                    <th scope="col" class="num">Days to LTCG</th>
                  </tr>
                </thead>
                <tbody>
                  ${lots.map((lot) => `
                    <tr${lot.near_ltcg ? ' style="background:#fef2f2"' : ""}>
                      <td class="nowrap">${escapeHtml(lot.account_name || "—")}</td>
                      <td class="nowrap ui-tabular-nums">${escapeHtml(lot.acquisition_date || "—")}</td>
                      <td class="num ui-tabular-nums">${escapeHtml(formatFixed(lot.qty, 3))}</td>
                      <td class="num ui-tabular-nums">${escapeHtml(formatCurrency(lot.cost_basis, 2))}</td>
                      <td class="${lot.term === "LT" ? "cell-ok" : lot.term === "ST" ? "cell-bad" : ""}">${escapeHtml(lot.term || "—")}</td>
                      <td class="num ui-tabular-nums ${lot.near_ltcg ? "cell-bad" : ""}">${escapeHtml(lot.days_to_ltcg == null ? "—" : lot.days_to_ltcg)}</td>
                    </tr>
                  `).join("")}
                </tbody>
              </table>
            </div>
          ` : row.is_portfolio_holding ? '<div class="ui-muted" style="margin-top:12px">No lot-level data available for this position</div>' : ""}
        </div>
      </div>
    `;
  }

  function renderDetails(payload) {
    const mount = byId("regimeDetailsMount");
    if (!mount) return;
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    const prefs = loadPrefs();
    state.detailChartsRendered = {};
    if (!rows.length) {
      mount.innerHTML = "";
      return;
    }
    mount.innerHTML = rows.map((row) => `
      <details class="ui-card regime-ticker-detail" data-regime-detail="${escapeHtml(row.ticker)}" style="margin-top:14px" ${prefs[`detail_${row.ticker}`] ? "open" : ""}>
        <summary style="list-style:none; cursor:pointer">
        <div class="regime-ticker-detail__header">
          <div>
            <div class="regime-ticker-detail__title">${escapeHtml(row.ticker)}</div>
            <div class="regime-ticker-detail__subtitle">${escapeHtml(row.regime)} regime · transition risk ${Number(row.transition_risk_pct || 0).toFixed(1)}% · expected duration ${Number(row.expected_duration || 0) >= 999 ? "999+ days" : `${Number(row.expected_duration || 0).toFixed(1)} days`}${row.stop_proximity ? ` · ${escapeHtml(row.stop_proximity.label)}` : ""}</div>
            <div class="regime-ticker-detail__metrics">
              <div class="regime-ticker-detail__metric">
                <div class="regime-ticker-detail__metric-label">Probability</div>
                <div class="regime-ticker-detail__metric-value">${escapeHtml(formatFixed(row.probability_pct, 1))}%</div>
              </div>
              <div class="regime-ticker-detail__metric">
                <div class="regime-ticker-detail__metric-label">Signal</div>
                <div class="regime-ticker-detail__metric-value">${escapeHtml(row.composite_signal || "—")}</div>
              </div>
              <div class="regime-ticker-detail__metric">
                <div class="regime-ticker-detail__metric-label">Confidence</div>
                <div class="regime-ticker-detail__metric-value">${escapeHtml(formatFixed(((row.unified_confidence || {}).value), 1))}/100</div>
              </div>
              <div class="regime-ticker-detail__metric">
                <div class="regime-ticker-detail__metric-label">ML Confidence</div>
                <div class="regime-ticker-detail__metric-value">${row.meta_labeler_probability != null ? `${escapeHtml(formatFixed(Number(row.meta_labeler_probability || 0) * 100, 1))}%` : "Inactive"}</div>
              </div>
              <div class="regime-ticker-detail__metric">
                <div class="regime-ticker-detail__metric-label">Relative Strength</div>
                <div class="regime-ticker-detail__metric-value">${escapeHtml(row.relative_strength || "In-line")}</div>
              </div>
              <div class="regime-ticker-detail__metric">
                <div class="regime-ticker-detail__metric-label">Tax Status</div>
                <div class="regime-ticker-detail__metric-value">${escapeHtml(row.tax_status || "—")}</div>
              </div>
              <div class="regime-ticker-detail__metric">
                <div class="regime-ticker-detail__metric-label">Next Earnings</div>
                <div class="regime-ticker-detail__metric-value">${escapeHtml(row.earnings_date ? String(row.earnings_date).slice(0, 10) : "N/A")}</div>
              </div>
            </div>
          </div>
          <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-right:20px">
            <span class="${badgeClass(row.regime)}">${escapeHtml(row.regime)}</span>
            <span class="ui-badge ui-badge--outline">${escapeHtml(row.composite_signal)}</span>
            <span class="ui-badge ${row.meta_labeler_probability != null ? (String(row.meta_labeler_signal || "").toLowerCase() === "confirm" ? "ui-badge--safe" : String(row.meta_labeler_signal || "").toLowerCase() === "veto" ? "ui-badge--bad" : "ui-badge--neutral") : "ui-badge--neutral"}">${row.meta_labeler_probability != null ? `ML ${(Number(row.meta_labeler_probability || 0) * 100).toFixed(0)}%` : "ML inactive"}</span>
            <span class="${sentimentClass(row.sentiment_trend)}">${escapeHtml(row.sentiment_trend || "not available")}</span>
            <span class="ui-badge ui-badge--outline">${escapeHtml(formatFixed(((row.unified_confidence || {}).value), 1))}/100</span>
          </div>
        </div>
        </summary>
        <div data-regime-detail-body="${escapeHtml(row.ticker)}">
          ${detailBody(row)}
        </div>
      </details>
    `).join("");
    wireDetailCards();
    mount.querySelectorAll("[data-regime-backtest]").forEach((button) => {
      button.addEventListener("click", async () => {
        const ticker = String(button.getAttribute("data-regime-backtest") || "");
        const target = byId(`regimeBacktestMount_${ticker}`);
        if (!target) return;
        target.textContent = "Loading backtest…";
        try {
          const endpoint = state.config.endpoints.backtest.replace("__TICKER__", encodeURIComponent(ticker));
          const response = await fetch(`${endpoint}?period=5y`, { headers: { Accept: "application/json" } });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Backtest failed (${response.status})`);
          const result = payload.result || {};
          target.innerHTML = `
            <div class="ui-muted">Total return ${escapeHtml(formatSignedPct(result.total_return, 1))} · Sharpe ${escapeHtml(formatFixed(result.sharpe_ratio, 2))} · Max drawdown ${escapeHtml(formatSignedPct(result.max_drawdown, 1))}</div>
            <div class="ui-muted" style="margin-top:4px">Buy & hold ${escapeHtml(formatSignedPct(result.buy_and_hold_return, 1))} · Trades ${escapeHtml((result.trades || []).length)}</div>
            ${Array.isArray(result.regime_conditional) && result.regime_conditional.length ? `
              <div class="table-wrap" style="margin-top:10px">
                <table>
                  <thead>
                    <tr>
                      <th scope="col">Regime</th>
                      <th scope="col">Confidence</th>
                      <th scope="col" class="num">Entries</th>
                      <th scope="col" class="num">Avg 5d</th>
                      <th scope="col" class="num">Avg 21d</th>
                      <th scope="col" class="num">21d Win Rate</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${result.regime_conditional.map((item) => {
                      const currentBucket = Number(row.probability || 0) >= 0.8 ? "high" : Number(row.probability || 0) >= 0.5 ? "medium" : "low";
                      const highlight = item.regime === row.regime && item.probability_bucket === currentBucket;
                      return `
                        <tr${highlight ? ' style="font-weight:700; background:#f8fafc"' : ""}>
                          <td>${escapeHtml(item.regime)}</td>
                          <td>${escapeHtml(item.probability_bucket)}</td>
                          <td class="num ui-tabular-nums">${escapeHtml(item.entry_count)}</td>
                          <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(item.avg_return_5d, 1))}</td>
                          <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(item.avg_return_21d, 1))}</td>
                          <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(item.win_rate_21d, 1))}</td>
                        </tr>
                      `;
                    }).join("")}
                  </tbody>
                </table>
              </div>
            ` : ""}
          `;
        } catch (error) {
          target.textContent = `Unable to load backtest: ${error.message || error}`;
        }
      });
    });
  }

  function renderHistory(payload) {
    const mount = byId("regimeHistoryMount");
    if (!mount) return;
    const rows = Array.isArray(payload.regime_history) ? payload.regime_history : [];
    if (!rows.length) {
      mount.innerHTML = '<div class="ui-muted">No regime changes recorded in the last 90 days.</div>';
      return;
    }
    const matureRows = rows.filter((row) => Number.isFinite(Number(row.return_5d)) || Number.isFinite(Number(row.return_10d)) || Number.isFinite(Number(row.return_21d)));
    const avg = (key) => {
      const nums = matureRows.map((row) => Number(row[key])).filter((value) => Number.isFinite(value));
      if (!nums.length) return "—";
      return formatSignedPct(nums.reduce((sum, value) => sum + value, 0) / nums.length, 1);
    };
    mount.innerHTML = `
      <div class="table-wrap" role="region" aria-label="Regime history">
        <table>
          <thead>
            <tr>
              <th scope="col">Date</th>
              <th scope="col">Ticker</th>
              <th scope="col">Previous → New Regime</th>
              <th scope="col" class="num">Price at Change</th>
              <th scope="col" class="num">5d Return</th>
              <th scope="col" class="num">10d Return</th>
              <th scope="col" class="num">21d Return</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((row) => `
              <tr>
                <td class="nowrap ui-tabular-nums">${escapeHtml(String(row.changed_at || "").slice(0, 10))}</td>
                <td class="nowrap">${escapeHtml(row.ticker)}</td>
                <td class="nowrap">
                  <span class="${escapeHtml(signalClass(row.previous_label === "Bull" ? "Buy" : row.previous_label === "Bear" ? "Sell" : "Hold"))}">${escapeHtml(row.previous_label || "—")}</span>
                  <span class="ui-muted">→</span>
                  <span class="${escapeHtml(signalClass(row.current_label === "Bull" ? "Buy" : row.current_label === "Bear" ? "Sell" : "Hold"))}">${escapeHtml(row.current_label || "—")}</span>
                </td>
                <td class="num ui-tabular-nums">${escapeHtml(formatCurrency(row.price_at_change, 2))}</td>
                <td class="num ui-tabular-nums ${Number(row.return_5d) > 0 ? "cell-ok" : Number(row.return_5d) < 0 ? "cell-bad" : ""}">${escapeHtml(formatSignedPct(row.return_5d, 1))}</td>
                <td class="num ui-tabular-nums ${Number(row.return_10d) > 0 ? "cell-ok" : Number(row.return_10d) < 0 ? "cell-bad" : ""}">${escapeHtml(formatSignedPct(row.return_10d, 1))}</td>
                <td class="num ui-tabular-nums ${Number(row.return_21d) > 0 ? "cell-ok" : Number(row.return_21d) < 0 ? "cell-bad" : ""}">${escapeHtml(formatSignedPct(row.return_21d, 1))}</td>
              </tr>
            `).join("")}
            <tr>
              <td colspan="4" class="ui-muted">Average return after tracked transitions</td>
              <td class="num ui-tabular-nums">${escapeHtml(avg("return_5d"))}</td>
              <td class="num ui-tabular-nums">${escapeHtml(avg("return_10d"))}</td>
              <td class="num ui-tabular-nums">${escapeHtml(avg("return_21d"))}</td>
            </tr>
          </tbody>
        </table>
      </div>
    `;
  }

  function renderDigest(payload) {
    const mount = byId("regimeDigestMount");
    if (!mount) return;
    const digest = payload.digest || {};
    const entries = Array.isArray(digest.entries) ? digest.entries : [];
    const sections = [];

    if (entries.length) {
      sections.push(entries.map((entry) => `
        <div class="ui-card" style="padding:10px">
          <div style="display:flex; justify-content:space-between; gap:12px; flex-wrap:wrap">
            <div style="font-weight:600">${escapeHtml(entry.ticker)}</div>
            <div class="${entry.priority === "ACTION REQUIRED" ? "regime-badge--action" : entry.priority === "NO CHANGE" ? "cell-ok" : ""}" style="padding:2px 8px;border-radius:999px">
              ${escapeHtml(entry.priority)}
            </div>
          </div>
          <div class="ui-muted" style="margin-top:4px">${escapeHtml(entry.current_regime)} · ${escapeHtml(entry.composite_action)} · ${escapeHtml(entry.sentiment_trend)}</div>
          ${entry.tax_note ? `<div class="ui-muted" style="margin-top:4px">${escapeHtml(entry.tax_note)}</div>` : ""}
        </div>
      `).join(""));
    } else {
      sections.push('<div class="ui-muted">No digest entries available.</div>');
    }

    [["Regime changes this week", digest.regime_changes], ["Sentiment divergences", digest.sentiment_divergences], ["Tax alerts", digest.tax_alerts], ["Action items", digest.action_items]].forEach(([label, items]) => {
      if (!Array.isArray(items) || !items.length) return;
      sections.push(`
        <div>
          <div class="ui-section-title">${escapeHtml(label)}</div>
          <ul class="ui-muted" style="margin:6px 0 0 18px">
            ${items.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
          </ul>
        </div>
      `);
    });

    mount.innerHTML = sections.join("");
  }

  function renderEffectiveness(payload) {
    const mount = byId("regimeEffectivenessMount");
    if (!mount) return;
    const effectiveness = payload.signal_effectiveness || {};
    const summary = effectiveness.summary || {};
    const byAction = effectiveness.by_action || {};
    const intervals = ["1w", "1m", "3m"];
    if (!intervals.some((key) => summary[key] && Number(summary[key].count || 0) > 0)) {
      mount.innerHTML = '<div class="ui-muted">No completed signal outcomes are available yet.</div>';
      return;
    }
    mount.innerHTML = intervals.map((key) => {
      const item = summary[key] || {};
      const actions = byAction[key] || {};
      return `
        <div class="ui-card" style="padding:12px">
          <div class="ui-section-title">${escapeHtml(key.toUpperCase())} effectiveness</div>
          <div class="ui-muted" style="margin-top:6px">Hit rate: ${escapeHtml(formatSignedPct(item.hit_rate, 1))} · Avg return: ${escapeHtml(formatSignedPct(item.avg_return, 2))} · Signals: ${escapeHtml(item.count || 0)}</div>
          ${Object.keys(actions).length ? `
            <div class="table-wrap" style="margin-top:10px">
              <table>
                <thead>
                  <tr>
                    <th scope="col">Action</th>
                    <th scope="col" class="num">Signals</th>
                    <th scope="col" class="num">Hit Rate</th>
                    <th scope="col" class="num">Avg Return</th>
                  </tr>
                </thead>
                <tbody>
                  ${Object.entries(actions).map(([action, actionItem]) => `
                    <tr>
                      <td>${escapeHtml(action)}</td>
                      <td class="num ui-tabular-nums">${escapeHtml(actionItem.count || 0)}</td>
                      <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(actionItem.hit_rate, 1))}</td>
                      <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(actionItem.avg_return, 2))}</td>
                    </tr>
                  `).join("")}
                </tbody>
              </table>
            </div>
          ` : ""}
        </div>
      `;
    }).join("");
  }

  function renderDiagnostics(payload) {
    const mount = byId("regimeDiagnosticsMount");
    if (!mount) return;
    const diagnostics = payload.model_diagnostics || null;
    if (!diagnostics || !diagnostics.calibration) {
      mount.innerHTML = '<div class="ui-muted">Not enough completed signal history for diagnostics yet.</div>';
      return;
    }
    const calibration = diagnostics.calibration || {};
    const bins = Array.isArray(calibration.bins) ? calibration.bins : [];
    const sharpness = diagnostics.sharpness || {};
    mount.innerHTML = `
      <div class="ui-card" style="padding:12px">
        <div class="ui-section-title">Calibration</div>
        <div class="ui-muted" style="margin-top:6px">Brier score: ${escapeHtml(formatFixed(calibration.brier_score, 3))} (lower is better)</div>
        ${bins.length ? `
          <div class="table-wrap" style="margin-top:10px">
            <table>
              <thead><tr><th scope="col">Bin</th><th scope="col" class="num">Predicted</th><th scope="col" class="num">Observed</th><th scope="col" class="num">Count</th></tr></thead>
              <tbody>
                ${bins.map((row) => `<tr><td>${escapeHtml(row.bin)}</td><td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(row.predicted, 1))}</td><td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(row.observed, 1))}</td><td class="num ui-tabular-nums">${escapeHtml(row.count)}</td></tr>`).join("")}
              </tbody>
            </table>
          </div>
        ` : '<div class="ui-muted" style="margin-top:8px">No calibration bins available.</div>'}
        <div class="ui-muted" style="margin-top:10px">Sharpness histogram: ${(Array.isArray(sharpness.histogram) ? sharpness.histogram : []).join(", ")}</div>
      </div>
    `;
  }

  function updateExportPdfButton(payload) {
    const exportBtn = byId("regimeExportPdf");
    if (!exportBtn) return;
    if (payload && Array.isArray(payload.rows) && payload.rows.length) {
      exportBtn.style.pointerEvents = "auto";
      exportBtn.style.opacity = "1";
    } else {
      exportBtn.style.pointerEvents = "none";
      exportBtn.style.opacity = "0.5";
    }
  }

  function renderPayload(payload) {
    state.lastPayload = payload;
    if (Array.isArray(payload.themes) && payload.themes.length) state.themes = payload.themes;
    if (Array.isArray(payload.theme_health) && payload.theme_health.length) state.themeHealth = payload.theme_health;
    renderWarnings(payload.warnings || []);
    renderCachedNote(payload);
    renderKpis(payload);
    renderEnsemblePanel(payload);
    renderHeroStrip(payload);
    renderDiffPanel(payload);
    renderHeatmap(payload);
    renderPortfolioSummary(payload);
    renderThemeHealth(payload);
    renderTable(payload);
    renderRelativeStrength(payload);
    renderDetails(payload);
    renderActionItemsBar(payload);
    renderHistory(payload);
    renderEffectiveness(payload);
    renderDiagnostics(payload);
    renderDigest(payload);
    renderThemes();
    loadWatchlist();
    updateTabBadges(payload);
    updateExportPdfButton(payload);
  }

  function switchTab(target) {
    document.querySelectorAll("[data-regime-tab]").forEach((tab) => {
      const active = tab.getAttribute("data-regime-tab") === target;
      tab.classList.toggle("regime-tab--active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
    });
    document.querySelectorAll("[data-regime-panel]").forEach((panel) => {
      panel.classList.toggle("regime-tab-panel--active", panel.getAttribute("data-regime-panel") === target);
    });
    savePref("activeTab", target);
    window.history.replaceState(null, "", `#${target}`);
    if (target === "trading") loadIBKRSettings();
    if (target === "analysis") loadEnsembleWeights().catch?.(() => {});
  }

  function initTabs() {
    document.querySelectorAll("[data-regime-tab]").forEach((tab) => {
      tab.addEventListener("click", () => switchTab(String(tab.getAttribute("data-regime-tab") || "analysis")));
    });
    const prefs = loadPrefs();
    const hash = window.location.hash.replace("#", "");
    const target = hash || prefs.activeTab || "analysis";
    if (document.querySelector(`[data-regime-tab="${CSS.escape(target)}"]`)) {
      switchTab(target);
    }
  }

  function updateTabBadges(payload) {
    const tradingBadge = byId("regimeTradingBadge");
    const researchBadge = byId("regimeResearchBadge");
    const pendingPlans = (state.paperPlans || []).filter((plan) => ["Pending", "Approved", "Submitted", "Partially Filled"].includes(String(plan.status || ""))).length;
    const tradingCount = pendingPlans + Number(payload.action_items_count || 0);
    if (tradingBadge) {
      tradingBadge.textContent = String(tradingCount);
      tradingBadge.style.display = tradingCount > 0 ? "" : "none";
    }
    const researchCount = (state.watchlist || []).filter((item) => String(item.status || "") === "Entry Signal").length;
    if (researchBadge) {
      researchBadge.textContent = String(researchCount);
      researchBadge.style.display = researchCount > 0 ? "" : "none";
    }
  }

  function wireSortTables() {
    document.querySelectorAll("[data-regime-sort-table]").forEach((wrap) => {
      const table = wrap.querySelector("table");
      const thead = table && table.querySelector("thead");
      const tbody = table && table.querySelector("tbody");
      if (!table || !thead || !tbody) return;
      let stateSort = { col: -1, dir: "desc" };
      Array.from(thead.querySelectorAll("th")).forEach((th, idx) => {
        const typ = th.getAttribute("data-sort");
        if (!typ || th.getAttribute("data-wired") === "true") return;
        th.setAttribute("data-wired", "true");
        th.style.cursor = "pointer";
        th.addEventListener("click", () => {
          const dir = stateSort.col === idx && stateSort.dir === "desc" ? "asc" : "desc";
          stateSort = { col: idx, dir };
          const rows = Array.from(tbody.querySelectorAll("tr"));
          rows.sort((a, b) => {
            if (typ === "num") {
              const av = numFromCell(a.children[idx]);
              const bv = numFromCell(b.children[idx]);
              const aa = Number.isFinite(av) ? av : -Infinity;
              const bb = Number.isFinite(bv) ? bv : -Infinity;
              if (aa === bb) return textFromCell(a.children[0]).localeCompare(textFromCell(b.children[0]));
              return aa < bb ? -1 : 1;
            }
            return textFromCell(a.children[idx]).localeCompare(textFromCell(b.children[idx]));
          });
          if (dir === "desc") rows.reverse();
          rows.forEach((row) => tbody.appendChild(row));
        });
      });
    });
  }

  function svgTooltipHost(mount) {
    let tooltip = mount.querySelector("[data-regime-tooltip]");
    if (tooltip) return tooltip;
    mount.style.position = "relative";
    tooltip = document.createElement("div");
    tooltip.setAttribute("data-regime-tooltip", "1");
    tooltip.style.position = "absolute";
    tooltip.style.pointerEvents = "none";
    tooltip.style.background = "#111827";
    tooltip.style.color = "#fff";
    tooltip.style.padding = "4px 8px";
    tooltip.style.borderRadius = "8px";
    tooltip.style.fontSize = "12px";
    tooltip.style.display = "none";
    tooltip.style.zIndex = "5";
    mount.appendChild(tooltip);
    return tooltip;
  }

  function wireTooltip(mount, svg, data, formatter) {
    const tooltip = svgTooltipHost(mount);
    svg.addEventListener("mousemove", (event) => {
      const rect = svg.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const idx = Math.max(0, Math.min(data.length - 1, Math.round((x / Math.max(1, rect.width)) * (data.length - 1))));
      tooltip.textContent = formatter(data[idx], idx);
      tooltip.style.display = "";
      tooltip.style.left = `${Math.min(rect.width - 120, Math.max(0, x + 10))}px`;
      tooltip.style.top = `${Math.max(0, event.clientY - rect.top - 36)}px`;
    });
    svg.addEventListener("mouseleave", () => {
      tooltip.style.display = "none";
    });
  }

  function linePath(points, xScale, yScale, valueKey) {
    let d = "";
    let started = false;
    points.forEach((point, idx) => {
      const value = Number(point[valueKey]);
      if (!Number.isFinite(value)) {
        started = false;
        return;
      }
      const x = xScale(idx);
      const y = yScale(value);
      d += `${started ? "L" : "M"}${x.toFixed(2)} ${y.toFixed(2)} `;
      started = true;
    });
    return d.trim();
  }

  function renderForwardChart(ticker) {
    const mount = byId(`regimeForwardChart_${ticker}`);
    if (!mount) return;
    const data = parseJson(`regimeChartData_${ticker}`);
    if (!Array.isArray(data) || data.length < 2) {
      mount.innerHTML = '<div class="ui-muted">Chart unavailable.</div>';
      return;
    }
    const W = 520;
    const H = 240;
    const pad = { l: 42, r: 12, t: 10, b: 28 };
    const iw = W - pad.l - pad.r;
    const ih = H - pad.t - pad.b;
    const xScale = (idx) => pad.l + (idx / Math.max(1, data.length - 1)) * iw;
    const yScale = (value) => pad.t + (1 - value) * ih;
    mount.innerHTML = `
      <svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${escapeHtml(ticker)} forward regime probabilities">
        <rect x="0" y="0" width="${W}" height="${H}" fill="#fff"></rect>
        ${[0, 0.25, 0.5, 0.75, 1].map((tick) => `<text x="4" y="${yScale(tick) + 4}" font-size="11" fill="#6b7280">${Math.round(tick * 100)}%</text><line x1="${pad.l}" y1="${yScale(tick)}" x2="${pad.l + iw}" y2="${yScale(tick)}" stroke="${tick === 0.5 ? "#9ca3af" : "#e5e7eb"}" ${tick === 0.5 ? 'stroke-dasharray="4 4"' : ""}></line>`).join("")}
        ${[1, 7, 14, 21].map((day) => {
          const idx = Math.min(data.length - 1, Math.max(0, day - 1));
          return `<text x="${xScale(idx)}" y="${H - 6}" text-anchor="middle" font-size="11" fill="#6b7280">${day}</text>`;
        }).join("")}
        <path d="${linePath(data, xScale, yScale, "p_bull")}" fill="none" stroke="#16a34a" stroke-width="2.5"></path>
        <path d="${linePath(data, xScale, yScale, "p_neutral")}" fill="none" stroke="#6b7280" stroke-width="2.5"></path>
        <path d="${linePath(data, xScale, yScale, "p_bear")}" fill="none" stroke="#dc2626" stroke-width="2.5"></path>
      </svg>
      <div class="ui-muted" style="margin-top:6px">Bull <span style="color:#16a34a">■</span> · Neutral <span style="color:#6b7280">■</span> · Bear <span style="color:#dc2626">■</span></div>
    `;
    const svg = mount.querySelector("svg");
    if (svg) {
      wireTooltip(mount, svg, data, (point) => `Day ${point.day || "?"}: Bull ${Math.round(Number(point.p_bull || 0) * 100)}%, Neutral ${Math.round(Number(point.p_neutral || 0) * 100)}%, Bear ${Math.round(Number(point.p_bear || 0) * 100)}%`);
    }
  }

  function renderConfidenceChart(ticker) {
    const mount = byId(`regimeConfidenceChart_${ticker}`);
    if (!mount) return;
    const data = parseJson(`regimeConfidenceData_${ticker}`);
    if (!Array.isArray(data) || data.length < 2) {
      mount.innerHTML = '<div class="ui-muted">Confidence chart unavailable.</div>';
      return;
    }
    const W = 520;
    const H = 140;
    const pad = { l: 28, r: 10, t: 14, b: 18 };
    const iw = W - pad.l - pad.r;
    const ih = H - pad.t - pad.b;
    const values = data.map((point) => Number(point.probability)).filter((v) => Number.isFinite(v));
    const minY = Math.max(0, Math.min(...values) - 0.05);
    const maxY = Math.min(1, Math.max(...values) + 0.05);
    const xScale = (idx) => pad.l + (idx / Math.max(1, data.length - 1)) * iw;
    const yScale = (value) => pad.t + (1 - (value - minY) / Math.max(1e-9, maxY - minY)) * ih;
    const path = data.map((point, idx) => `${idx === 0 ? "M" : "L"}${xScale(idx).toFixed(2)} ${yScale(Number(point.probability)).toFixed(2)}`).join(" ");
    mount.innerHTML = `
      <svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${escapeHtml(ticker)} confidence trajectory">
        <rect x="0" y="0" width="${W}" height="${H}" fill="#fff"></rect>
        <text x="4" y="${pad.t + 4}" font-size="11" fill="#6b7280">${maxY.toFixed(2)}</text>
        <text x="4" y="${pad.t + ih}" font-size="11" fill="#6b7280">${minY.toFixed(2)}</text>
        <text x="${pad.l}" y="${H - 4}" font-size="11" fill="#6b7280">${Number(data[0].probability).toFixed(2)}</text>
        <text x="${W - 42}" y="${H - 4}" font-size="11" fill="#6b7280">${Number(data[data.length - 1].probability).toFixed(2)}</text>
        <line x1="${pad.l}" y1="${pad.t + ih}" x2="${pad.l + iw}" y2="${pad.t + ih}" stroke="#e5e7eb"></line>
        <path d="${path}" fill="none" stroke="#111827" stroke-width="2.5"></path>
      </svg>
    `;
    const svg = mount.querySelector("svg");
    if (svg) {
      wireTooltip(mount, svg, data, (point) => `Day ${point.day || "?"}: confidence ${Number(point.probability || 0).toFixed(2)}`);
    }
  }

  function renderSentimentChart(ticker) {
    const mount = byId(`regimeSentimentChart_${ticker}`);
    if (!mount) return;
    const data = parseJson(`regimeSentimentData_${ticker}`);
    if (!Array.isArray(data) || data.length < 1) {
      mount.innerHTML = '<div class="ui-muted">Sentiment not available.</div>';
      return;
    }
    if (data.length === 1) {
      const point = data[0] || {};
      const score = Number(point.score || 0);
      mount.innerHTML = `<div class="ui-muted">Single sentiment observation: ${score.toFixed(1)} on ${escapeHtml(String(point.recorded_at || "").slice(0, 10) || "unknown date")}.</div>`;
      return;
    }
    const W = 520;
    const H = 140;
    const pad = { l: 28, r: 10, t: 14, b: 24 };
    const iw = W - pad.l - pad.r;
    const ih = H - pad.t - pad.b;
    const values = data.map((point) => Number(point.score)).filter((v) => Number.isFinite(v));
    const minY = Math.min(-1, ...values);
    const maxY = Math.max(1, ...values);
    const xScale = (idx) => pad.l + (idx / Math.max(1, data.length - 1)) * iw;
    const yScale = (value) => pad.t + (1 - (value - minY) / Math.max(1e-9, maxY - minY)) * ih;
    const path = data.map((point, idx) => `${idx === 0 ? "M" : "L"}${xScale(idx).toFixed(2)} ${yScale(Number(point.score)).toFixed(2)}`).join(" ");
    const lastScore = Number(data[data.length - 1].score || 0);
    const color = lastScore > 0 ? "#16a34a" : lastScore < 0 ? "#dc2626" : "#6b7280";
    mount.innerHTML = `
      <svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${escapeHtml(ticker)} sentiment history">
        <rect x="0" y="0" width="${W}" height="${H}" fill="#fff"></rect>
        <line x1="${pad.l}" y1="${yScale(0)}" x2="${pad.l + iw}" y2="${yScale(0)}" stroke="#9ca3af" stroke-dasharray="4 4"></line>
        <text x="${pad.l}" y="${H - 6}" font-size="11" fill="#6b7280">${escapeHtml(String(data[0].recorded_at || "").slice(0, 10))}</text>
        <text x="${W - 72}" y="${H - 6}" font-size="11" fill="#6b7280">${escapeHtml(String(data[data.length - 1].recorded_at || "").slice(0, 10))}</text>
        <path d="${path}" fill="none" stroke="${color}" stroke-width="2.5"></path>
      </svg>
    `;
    const svg = mount.querySelector("svg");
    if (svg) {
      wireTooltip(mount, svg, data, (point) => `${String(point.recorded_at || "").slice(0, 10)}: score ${Number(point.score || 0).toFixed(1)}`);
    }
  }

  function renderChartsForTicker(ticker) {
    if (!ticker || state.detailChartsRendered[ticker]) return;
    const row = (state.lastPayload && Array.isArray(state.lastPayload.rows) ? state.lastPayload.rows : []).find((item) => String(item.ticker || "") === String(ticker));
    if (row && row.charts) {
      const priceMount = byId(`regimePlotlyPrice_${ticker}`);
      const transitionMount = byId(`regimePlotlyTransition_${ticker}`);
      const confidenceMount = byId(`regimePlotlyConfidence_${ticker}`);
      if (window.Plotly) {
        if (priceMount && row.charts.price) window.Plotly.newPlot(priceMount, row.charts.price.data || [], row.charts.price.layout || {}, { responsive: true });
        if (transitionMount && row.charts.transition) window.Plotly.newPlot(transitionMount, row.charts.transition.data || [], row.charts.transition.layout || {}, { responsive: true });
        if (confidenceMount && row.charts.confidence) window.Plotly.newPlot(confidenceMount, row.charts.confidence.data || [], row.charts.confidence.layout || {}, { responsive: true });
      } else {
        const notice = '<div class="ui-muted" style="padding:24px;text-align:center;color:#6b7280">Plotly library unavailable — interactive chart cannot render.<br><span style="font-size:12px">Check that <code>static/vendor/plotly-2.35.0.min.js</code> exists.</span></div>';
        if (priceMount && row.charts.price && !priceMount.innerHTML) priceMount.innerHTML = notice;
        if (transitionMount && row.charts.transition && !transitionMount.innerHTML) transitionMount.innerHTML = notice;
        if (confidenceMount && row.charts.confidence && !confidenceMount.innerHTML) confidenceMount.innerHTML = notice;
      }
    }
    renderForwardChart(ticker);
    renderConfidenceChart(ticker);
    renderSentimentChart(ticker);
    state.detailChartsRendered[ticker] = true;
  }

  function wireDetailCards() {
    document.querySelectorAll("[data-regime-detail]").forEach((detail) => {
      const ticker = String(detail.getAttribute("data-regime-detail") || "");
      if (detail.getAttribute("data-regime-wired") === "true") return;
      detail.setAttribute("data-regime-wired", "true");
      detail.addEventListener("toggle", () => {
        savePref(`detail_${ticker}`, detail.open);
        if (detail.open) renderChartsForTicker(ticker);
      });
    });
  }

  function setProgress(status, progress, total, ticker, error) {
    const wrap = byId("regimeProgress");
    const text = byId("regimeProgressText");
    const bar = byId("regimeProgressBar");
    if (!wrap || !text || !bar) return;
    if (status === "idle") {
      wrap.style.display = "none";
      bar.style.width = "0%";
      text.textContent = "Waiting to start.";
      updateStreamBadge("idle");
      return;
    }
    wrap.style.display = "";
    const pct = total > 0 ? Math.max(0, Math.min(100, (progress / total) * 100)) : 0;
    bar.style.width = `${pct}%`;
    if (status === "pending") {
      text.textContent = `Queued for analysis (${total} ticker${total === 1 ? "" : "s"}).`;
      return;
    }
    if (status === "running") {
      const next = Math.min(total, progress + 1);
      text.textContent = `Analyzing ticker ${next} of ${total}${ticker ? `: ${ticker}` : ""}`;
      return;
    }
    if (status === "done") {
      text.textContent = `Analysis complete for ${total} ticker${total === 1 ? "" : "s"}.`;
      bar.style.width = "100%";
      return;
    }
    if (status === "error") {
      text.textContent = error || "Analysis failed.";
    }
  }

  function setProgressFromPayload(job) {
    const wrap = byId("regimeProgress");
    const text = byId("regimeProgressText");
    const bar = byId("regimeProgressBar");
    if (!wrap || !text || !bar) return;
    if ((job.status || "idle") === "idle") {
      setProgress("idle", 0, 0, "", "");
      return;
    }
    wrap.style.display = "";
    const total = Number(job.total || 0);
    const progress = Number(job.progress || 0);
    const pct = total > 0 ? Math.max(0, Math.min(100, (progress / total) * 100)) : 0;
    bar.style.width = `${pct}%`;
    const parts = [];
    if (job.progress_text) parts.push(job.progress_text);
    if (Number.isFinite(Number(job.eta_seconds)) && Number(job.eta_seconds) > 0 && job.status === "running") {
      parts.push(`ETA ${Math.ceil(Number(job.eta_seconds))}s`);
    }
    if (Number(job.cache_hits || 0) || Number(job.cache_misses || 0)) {
      parts.push(`Cache hits ${Number(job.cache_hits || 0)} · misses ${Number(job.cache_misses || 0)}`);
    }
    text.textContent = parts.join(" · ") || `Analyzing ${job.current_ticker || ""}`.trim();
    if (job.status === "done") {
      bar.style.width = "100%";
    } else if (job.status === "error") {
      text.textContent = job.error || text.textContent || "Analysis failed.";
    }
  }

  function addHoldingSelection(ticker) {
    const normalized = String(ticker || "").trim().toUpperCase();
    if (!normalized) return;
    if (state.selectedHoldings.includes(normalized)) {
      setPickerMessage("");
      return;
    }
    state.selectedHoldings = [...state.selectedHoldings, normalized];
    renderPickerOptions();
    updateSelectionCounter();
    const search = document.querySelector("[data-regime-search-input]");
    if (search) search.focus();
  }

  function removeHoldingSelection(ticker) {
    state.selectedHoldings = state.selectedHoldings.filter((item) => item !== String(ticker || "").toUpperCase());
    renderPickerOptions();
    updateSelectionCounter();
  }

  function selectAllHoldings() {
    state.selectedHoldings = [...state.holdings];
    renderPickerOptions();
    updateSelectionCounter();
  }

  function deselectAllHoldings() {
    state.selectedHoldings = [];
    renderPickerOptions();
    updateSelectionCounter();
  }

  function pickerItems() {
    const query = String((document.querySelector("[data-regime-search-input]") || {}).value || "").trim().toUpperCase();
    const selected = new Set(state.selectedHoldings);
    const groups = [];
    Object.entries(state.holdingGroups || {}).forEach(([label, tickers]) => {
      const items = (Array.isArray(tickers) ? tickers : [])
        .filter((ticker) => !selected.has(ticker))
        .filter((ticker) => !query || ticker.includes(query));
      if (items.length) groups.push([label, items]);
    });
    return groups;
  }

  function renderPickerOptions() {
    const mount = byId("regimeHoldingsPicker");
    if (!mount) return;
    const groups = pickerItems();
    if (!groups.length) {
      mount.innerHTML = '<div class="ui-muted">No matching holdings.</div>';
      state.pickerFocusIndex = -1;
      return;
    }
    let itemIndex = -1;
    mount.innerHTML = groups.map(([label, tickers]) => `
      <section>
        <div class="ui-muted" style="font-size:0.8rem; margin-bottom:6px">${escapeHtml(label)}</div>
        <div style="display:grid; gap:6px">
          ${tickers.map((ticker) => {
            itemIndex += 1;
            return `
              <button
                type="button"
                role="option"
                data-regime-picker-item="${escapeHtml(ticker)}"
                data-regime-picker-index="${itemIndex}"
                class="btn btn--secondary"
                style="justify-content:flex-start; text-align:left"
              >
                ${escapeHtml(ticker)}
              </button>
            `;
          }).join("")}
        </div>
      </section>
    `).join("");
    mount.querySelectorAll("[data-regime-picker-item]").forEach((button) => {
      button.addEventListener("click", () => addHoldingSelection(String(button.getAttribute("data-regime-picker-item") || "")));
    });
    if (state.pickerFocusIndex >= mount.querySelectorAll("[data-regime-picker-item]").length) {
      state.pickerFocusIndex = -1;
    }
    highlightPickerFocus();
  }

  function highlightPickerFocus() {
    document.querySelectorAll("[data-regime-picker-item]").forEach((button, idx) => {
      button.classList.toggle("highlighted", idx === state.pickerFocusIndex);
      if (idx === state.pickerFocusIndex) button.setAttribute("aria-selected", "true");
      else button.removeAttribute("aria-selected");
    });
  }

  function mergePartialResults(partialResults) {
    if (!partialResults || typeof partialResults !== "object") return;
    const base = state.lastPayload || state.config.initial_payload || {};
    const rows = Array.isArray(base.rows) ? [...base.rows] : [];
    const byTicker = new Map(rows.map((row) => [String(row.ticker || "").toUpperCase(), row]));
    Object.entries(partialResults).forEach(([ticker, row]) => {
      if (!ticker || !row) return;
        byTicker.set(String(ticker).toUpperCase(), row);
    });
    const merged = { ...base, rows: Array.from(byTicker.values()) };
    state.lastPayload = merged;
    renderKpis(merged);
    renderHeroStrip(merged);
    renderTable(merged);
    renderHeatmap(merged);
    renderDiffPanel(merged);
    renderActionItemsBar(merged);
    renderDetails(merged);
    wireDetailCards();
  }

  function stopRunTracking() {
    if (state.pollTimer) {
      window.clearInterval(state.pollTimer);
      state.pollTimer = null;
    }
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
    state.streamRetries = 0;
    updateStreamBadge("idle");
  }

  async function loadHoldings() {
    const mount = byId("regimeHoldingsPicker");
    if (!mount) return;
    mount.innerHTML = '<div class="ui-muted">Loading holdings…</div>';
    const params = new URLSearchParams();
    if (showAllEnabled()) params.set("show_all", "true");
    params.set("portfolio_scope", currentPortfolioScope());
    if (currentAccountId()) params.set("account_id", currentAccountId());
    const url = `${state.config.endpoints.holdings}${params.toString() ? `?${params.toString()}` : ""}`;
    try {
      const response = await fetch(url, { headers: { Accept: "application/json" } });
      const payload = await response.json();
      state.holdings = Array.isArray(payload.tickers) ? payload.tickers : [];
      state.holdingGroups = payload.groups || (state.holdings.length ? { "Current Holdings": state.holdings } : {});
      const priorSelection = state.selectedHoldings.filter((ticker) => state.holdings.includes(ticker));
      state.selectedHoldings = priorSelection.length ? priorSelection : [...state.holdings];
      renderPickerOptions();
      if (payload.warning) setPickerMessage(payload.warning, true);
    } catch (error) {
      mount.innerHTML = `<div class="ui-muted">Unable to load holdings: ${escapeHtml(error.message || error)}</div>`;
    }
    updateSelectionCounter();
  }

  function renderAccountOptions(scopeValue, resetSelection = false) {
    const wrap = document.querySelector("[data-regime-account-wrap]");
    const select = document.querySelector("[data-regime-account-id]");
    if (!wrap || !select) return;
    const scope = (state.portfolioScopes || []).find((item) => item && item.value === scopeValue) || null;
    const accounts = Array.isArray(scope && scope.accounts) ? scope.accounts.filter((item) => item && item.has_holdings) : [];
    const previous = resetSelection ? "" : (currentAccountId() || String((state.config && state.config.account_id) || ""));
    select.innerHTML = '<option value="">Combined (all accounts)</option>' + accounts.map((account) => (
      `<option value="${escapeHtml(account.id)}">${escapeHtml(account.name)}</option>`
    )).join("");
    if (!resetSelection && previous && accounts.some((account) => String(account.id) === previous)) {
      select.value = previous;
    } else {
      select.value = "";
    }
    wrap.style.display = accounts.length > 1 ? "grid" : "none";
  }

  function renderSupplyChain(themeId, layers) {
    const mount = document.querySelector(`[data-supply-chain-list="${CSS.escape(String(themeId))}"]`);
    if (!mount) return;
    const items = Array.isArray(layers) ? layers : [];
    if (!items.length) {
      mount.innerHTML = '<div class="ui-muted">No supply-chain map yet.</div>';
      return;
    }
    mount.innerHTML = items.map((item) => `
      <div style="border:1px solid ${COLORS.border}; border-radius:10px; padding:8px; margin-top:8px">
        <div style="font-weight:600">${escapeHtml(item.layer || "")}</div>
        <div class="ui-muted" style="margin-top:4px">${escapeHtml(item.description || "")}</div>
        ${item.example_companies ? `<div class="ui-muted" style="margin-top:4px">Examples: ${escapeHtml(item.example_companies)}</div>` : ""}
      </div>
    `).join("");
  }

  async function generateSupplyChain(themeId) {
    const body = new URLSearchParams();
    body.set("frontier_provider", currentFrontierProvider());
    const response = await fetch(state.config.endpoints.supply_chain.replace("__THEME_ID__", encodeURIComponent(String(themeId))), {
      method: "POST",
      body,
      headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `Supply-chain generation failed (${response.status})`);
    renderSupplyChain(themeId, payload.layers || []);
    return payload.layers || [];
  }

  function renderCrowdScore(score) {
    const value = Number(score || 0);
    const color = value <= 20 ? "#22c55e"
      : value <= 40 ? "#84cc16"
      : value <= 60 ? "#eab308"
      : value <= 80 ? "#f97316"
      : "#ef4444";
    return `<div style="display:flex; align-items:center; gap:6px">
      <div style="width:60px; height:8px; background:#e5e7eb; border-radius:4px; overflow:hidden">
        <div style="width:${Math.max(0, Math.min(100, value))}%; height:100%; background:${color}; border-radius:4px"></div>
      </div>
      <span class="ui-muted" style="font-size:0.85rem">${value}</span>
    </div>`;
  }

  function refreshWatchlistThemeFilter() {
    const select = byId("regimeWatchlistThemeFilter");
    if (!select) return;
    const current = select.value;
    select.innerHTML = '<option value="">All Themes</option>' + (Array.isArray(state.themes) ? state.themes : []).map((theme) => (
      `<option value="${escapeHtml(theme.id)}">${escapeHtml(theme.name || "")}</option>`
    )).join("");
    if (current && Array.from(select.options).some((option) => option.value === current)) {
      select.value = current;
    }
  }

  function renderWatchlist() {
    const mount = byId("regimeWatchlistMount");
    const countBadge = byId("regimeWatchlistCount");
    const signalsBadge = byId("regimeWatchlistSignals");
    if (!mount) return;
    const rows = Array.isArray(state.watchlist) ? [...state.watchlist] : [];
    const signals = rows.filter((item) => String(item.status || "") === "Entry Signal");
    if (countBadge) countBadge.textContent = String(rows.length);
    if (signalsBadge) {
      signalsBadge.textContent = `${signals.length} signals`;
      signalsBadge.style.display = signals.length ? "" : "none";
    }
    if (!rows.length) {
      mount.innerHTML = '<div class="regime-empty-state"><div class="regime-empty-state__title">No research candidates yet</div><div class="ui-muted">Run a discovery scan to find opportunities.</div></div>';
      return;
    }
    const ordered = [...signals, ...rows.filter((item) => String(item.status || "") !== "Entry Signal")];
    mount.innerHTML = ordered.map((item) => `
      <div class="ui-card" style="padding:12px; ${String(item.status || "") === "Entry Signal" ? `border-color:${COLORS.bull}; box-shadow:0 0 0 1px ${COLORS.bull} inset;` : ""}">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">${escapeHtml(item.ticker || "")} <span class="${badgeClass(item.regime_label || "Neutral")}" style="margin-left:6px">${escapeHtml(item.regime_label || "N/A")}</span> <span class="${badgeClass(item.status === "Entry Signal" ? "Bull" : "Neutral")}" style="margin-left:6px">${escapeHtml(item.status || "Watching")}</span></div>
            <div class="ui-muted">${escapeHtml(item.company_name || "")} · ${escapeHtml(item.theme_name || "")} · ${escapeHtml(item.supply_chain_layer || "Unassigned layer")}</div>
          </div>
          <div>${renderCrowdScore(item.crowd_score)}</div>
        </div>
        <div class="ui-muted" style="margin-top:8px">${escapeHtml(item.discovery_rationale || "")}</div>
        <div class="ui-muted" style="margin-top:8px">Entry ${escapeHtml(formatCurrency(item.suggested_entry_price, 2))} · Stop ${escapeHtml(formatCurrency(item.suggested_stop_price, 2))} · Prob ${(Number(item.regime_probability || 0) * 100).toFixed(0)}%</div>
        <div class="regime-strength-bar" style="margin-top:8px">
          <div class="regime-strength-bar__fill" style="width:${Math.max(0, Math.min(100, Number(item.regime_probability || 0) * 100)).toFixed(1)}%"></div>
        </div>
        <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:10px">
          <button class="btn btn--secondary" type="button" data-watchlist-promote="${escapeHtml(item.id)}">Promote to Theme</button>
          <button class="btn btn--secondary" type="button" data-watchlist-pass="${escapeHtml(item.id)}">Pass</button>
          <button class="btn btn--secondary" type="button" data-watchlist-delete="${escapeHtml(item.id)}">Delete</button>
        </div>
      </div>
    `).join("");
    mount.querySelectorAll("[data-watchlist-promote]").forEach((button) => {
      button.addEventListener("click", () => promoteCandidate(String(button.getAttribute("data-watchlist-promote") || "")));
    });
    mount.querySelectorAll("[data-watchlist-pass]").forEach((button) => {
      button.addEventListener("click", () => passCandidate(String(button.getAttribute("data-watchlist-pass") || "")));
    });
    mount.querySelectorAll("[data-watchlist-delete]").forEach((button) => {
      button.addEventListener("click", () => deleteWatchlistCandidate(String(button.getAttribute("data-watchlist-delete") || "")));
    });
  }

  async function loadWatchlist(themeId = null, status = null) {
    if (!state.config || !state.config.endpoints || !state.config.endpoints.watchlist) return;
    const params = new URLSearchParams();
    const themeSelect = byId("regimeWatchlistThemeFilter");
    const statusSelect = byId("regimeWatchlistStatusFilter");
    const resolvedTheme = themeId != null ? String(themeId) : (themeSelect && themeSelect.value ? themeSelect.value : "");
    const resolvedStatus = status != null ? String(status) : (statusSelect && statusSelect.value ? statusSelect.value : "");
    if (resolvedTheme) params.set("theme_id", resolvedTheme);
    if (resolvedStatus) params.set("status", resolvedStatus);
    const url = `${state.config.endpoints.watchlist}${params.toString() ? `?${params.toString()}` : ""}`;
    try {
      const response = await fetch(url, { headers: { Accept: "application/json" } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Unable to load watchlist (${response.status})`);
      state.watchlist = Array.isArray(payload.watchlist) ? payload.watchlist : [];
      state.watchlistStats = payload.stats || {};
      refreshWatchlistThemeFilter();
      renderWatchlist();
    } catch (error) {
      const mount = byId("regimeWatchlistMount");
      if (mount) mount.innerHTML = `<div class="ui-muted">Unable to load watchlist: ${escapeHtml(error.message || error)}</div>`;
    }
  }

  async function promoteCandidate(watchlistId) {
    try {
      const response = await fetch(state.config.endpoints.watchlist_promote.replace("__WATCHLIST_ID__", encodeURIComponent(String(watchlistId))), {
        method: "POST",
        headers: { Accept: "application/json" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Promote failed (${response.status})`);
      await loadThemes();
      await loadWatchlist();
    } catch (error) {
      showToast(`Unable to promote candidate: ${error.message || error}`, "error");
    }
  }

  async function passCandidate(watchlistId) {
    try {
      const response = await fetch(state.config.endpoints.watchlist_pass.replace("__WATCHLIST_ID__", encodeURIComponent(String(watchlistId))), {
        method: "POST",
        headers: { Accept: "application/json" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Pass failed (${response.status})`);
      await loadWatchlist();
    } catch (error) {
      showToast(`Unable to pass candidate: ${error.message || error}`, "error");
    }
  }

  async function deleteWatchlistCandidate(watchlistId) {
    try {
      const response = await fetch(state.config.endpoints.watchlist_entry.replace("__WATCHLIST_ID__", encodeURIComponent(String(watchlistId))), {
        method: "DELETE",
        headers: { Accept: "application/json" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Delete failed (${response.status})`);
      await loadWatchlist();
    } catch (error) {
      showToast(`Unable to delete candidate: ${error.message || error}`, "error");
    }
  }

  function currentPaperPortfolioId() {
    return state.currentPaperPortfolioId ? String(state.currentPaperPortfolioId) : "";
  }

  function currentPaperPortfolio() {
    if (state.paperPortfolioDetail && state.paperPortfolioDetail.portfolio && String(state.paperPortfolioDetail.portfolio.id) === currentPaperPortfolioId()) {
      return state.paperPortfolioDetail.portfolio;
    }
    const id = currentPaperPortfolioId();
    return (state.paperPortfolios || []).find((row) => String(row.id) === id) || null;
  }

  function planPrecheck(planId) {
    return state.paperPrecheck[String(planId)] || null;
  }

  function renderGuardrailBadge(plan) {
    const result = planPrecheck(plan.id);
    if (!result) return '<span class="ui-badge ui-badge--neutral">Guardrails unchecked</span>';
    if (result.guardrail_passed) return '<span class="ui-badge ui-badge--safe">✓ All guardrails pass</span>';
    const failed = (result.guardrail_checks || []).filter((check) => !check.passed).map((check) => check.message).filter(Boolean);
    return `<span class="ui-badge ui-badge--bad" title="${escapeHtml(failed.join(" | "))}">⚠ Blocked</span>`;
  }

  function renderTaxImpactBadge(plan) {
    if (String(plan.action || "") !== "Sell") return "";
    const preview = (state.paperTaxEstimates || {})[String(plan.id)];
    if (!preview || !preview.tax_impact) return '<span class="ui-badge ui-badge--neutral">Tax preview pending</span>';
    const impact = preview.tax_impact;
    const pnl = Number(impact.estimated_pnl || 0);
    const term = Math.abs(Number(impact.long_term_gain || 0)) + Math.abs(Number(impact.long_term_loss || 0)) > 0 ? "LT" : "ST";
    const klass = pnl >= 0 ? "ui-badge ui-badge--safe" : "ui-badge ui-badge--bad";
    return `<span class="${klass}" title="${escapeHtml(`ST gain ${formatCurrency(impact.short_term_gain, 2)} · ST loss ${formatCurrency(impact.short_term_loss, 2)} · LT gain ${formatCurrency(impact.long_term_gain, 2)} · LT loss ${formatCurrency(impact.long_term_loss, 2)}`)}">${escapeHtml(formatCurrency(pnl, 2))} ${term}${impact.wash_sale_warning ? " ⚠" : ""}</span>`;
  }

  function renderAuditTrail() {
    const mount = byId("regimeAuditTrailMount");
    if (!mount) return;
    if (!currentPaperPortfolioId()) {
      mount.innerHTML = '<div class="ui-muted">Audit trail will appear after you select a paper portfolio.</div>';
      return;
    }
    const summary = state.paperAuditSummary || {};
    const rows = Array.isArray(state.paperAudit) ? state.paperAudit : [];
    mount.innerHTML = `
      <div class="ui-card" style="padding:12px">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">Order Audit Trail</div>
            <div class="ui-muted">Today: ${escapeHtml(summary.trades_today || summary.filled_count || 0)} trades, ${escapeHtml(summary.guardrail_blocks || summary.blocked_count || 0)} blocked, ${escapeHtml(summary.rejected_count || 0)} rejected.</div>
          </div>
          <div style="display:flex; gap:8px; flex-wrap:wrap">
            <select id="regimeAuditEventFilter">
              <option value="">All events</option>
              <option value="filled">filled</option>
              <option value="rejected">rejected</option>
              <option value="guardrail_blocked">guardrail_blocked</option>
              <option value="cancelled">cancelled</option>
              <option value="error">error</option>
            </select>
            <input id="regimeAuditTickerFilter" type="text" placeholder="Ticker" style="max-width:120px" />
            <select id="regimeAuditDaysFilter">
              <option value="7">Last 7 days</option>
              <option value="30" selected>Last 30 days</option>
              <option value="90">Last 90 days</option>
            </select>
            <button class="btn btn--secondary" type="button" id="regimeAuditApply">Apply</button>
          </div>
        </div>
        <div class="table-wrap" style="margin-top:10px">
          <table>
            <thead>
              <tr>
                <th scope="col">Time</th>
                <th scope="col">Order ID</th>
                <th scope="col">Ticker</th>
                <th scope="col">Action</th>
                <th scope="col">Event</th>
                <th scope="col">Details</th>
              </tr>
            </thead>
            <tbody>
              ${rows.length ? rows.map((row) => {
                const event = String(row.event_type || "");
                const tone = ["filled"].includes(event) ? "cell-ok" : ["rejected", "guardrail_blocked", "error"].includes(event) ? "cell-bad" : "";
                return `<tr>
                  <td class="nowrap">${escapeHtml(relativeTime(row.created_at))}</td>
                  <td><button class="btn btn--secondary" type="button" data-audit-order-id="${escapeHtml(String(row.order_id || "").slice(0, 8))}" data-audit-order-full="${escapeHtml(row.order_id || "")}">${escapeHtml(String(row.order_id || "").slice(0, 8))}</button></td>
                  <td>${escapeHtml(row.ticker || "")}</td>
                  <td class="${String(row.action || "").toLowerCase() === "buy" ? "cell-ok" : "cell-bad"}">${escapeHtml(row.action || "")}</td>
                  <td class="${tone}">${escapeHtml(event)}</td>
                  <td>${escapeHtml(row.details || "")}</td>
                </tr>`;
              }).join("") : '<tr><td colspan="6" class="ui-muted">No audit events.</td></tr>'}
            </tbody>
          </table>
        </div>
        <div id="regimeOrderTimelineMount" style="margin-top:12px"></div>
      </div>
    `;
    const applyBtn = byId("regimeAuditApply");
    if (applyBtn) applyBtn.addEventListener("click", () => loadAuditTrail());
    mount.querySelectorAll("[data-audit-order-full]").forEach((button) => {
      button.addEventListener("click", () => renderOrderTimeline(button.getAttribute("data-audit-order-full") || ""));
    });
  }

  function renderPnlSparkline(value) {
    const pnl = Number(value || 0);
    const gain = pnl >= 0;
    const width = 44;
    const height = 14;
    const baseline = gain ? height - 2 : 2;
    const endY = gain ? 2 : height - 2;
    const color = gain ? COLORS.bull : COLORS.bear;
    return `
      <svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" aria-hidden="true">
        <line x1="0" y1="${baseline}" x2="${width}" y2="${baseline}" stroke="${COLORS.border}" stroke-width="1"></line>
        <path d="M2 ${baseline} C 16 ${baseline}, 24 ${endY}, ${width - 2} ${endY}" fill="none" stroke="${color}" stroke-width="2"></path>
      </svg>
    `;
  }

  function renderMonitoringDashboard(payload) {
    const mount = byId("regimeMonitoringMount");
    const badge = byId("regimeMonitoringStatus");
    if (!mount) return;
    if (!payload || !payload.account) {
      mount.innerHTML = '<div class="regime-empty-state"><div class="regime-empty-state__title">Monitoring unavailable</div><div class="ui-muted">Monitoring becomes available after you select a paper portfolio.</div></div>';
      if (badge) {
        badge.textContent = "--";
        badge.className = "ui-badge ui-badge--neutral";
      }
      updateStatusBar();
      return;
    }
    const account = payload.account || {};
    const positions = Array.isArray(payload.positions) ? payload.positions : [];
    const pendingOrders = Array.isArray(payload.pending_orders) ? payload.pending_orders : [];
    const guardrails = payload.guardrails || {};
    const connection = payload.connection || {};
    const readiness = payload.readiness || {};
    const health = state.systemHealth || {};
    const backup = health.backup || {};
    const validation = state.dataValidation || {};
    const model = health.model || {};
    const eventBus = health.event_bus || {};
    const agentData = health.agents || {};
    const heartbeatAge = Number(health.heartbeat_age_seconds || 0);
    const heartbeatBadge = heartbeatAge <= 60
      ? "ui-badge ui-badge--safe"
      : heartbeatAge <= 300
        ? "ui-badge ui-badge--warn"
        : "ui-badge ui-badge--bad";
    const connectionText = connection.connected === true
      ? "Connected"
      : connection.connected === false
        ? "Disconnected"
        : (connection.connection || "n/a");
    const agentList = Array.isArray(agentData.agents) ? agentData.agents : [];
    const enabledAgents = agentList.filter((agent) => agent && agent.enabled).length;
    if (badge) {
      badge.textContent = connection.market_hours || connectionText || "ready";
      badge.className = `ui-badge ${connection.connected === true || String(connection.market_hours || "").toLowerCase() === "regular" ? "ui-badge--safe" : "ui-badge--neutral"}`;
    }
    mount.innerHTML = `
      ${health.status === "error" ? `<div class="ui-card" style="margin-bottom:12px; border-color:${COLORS.bear}; background:rgba(170,54,54,0.08)"><strong>System Error</strong><div class="ui-muted" style="margin-top:6px">Check system health. Out-of-band watchdog may trigger emergency liquidation.</div></div>` : ""}
      <div class="regime-monitor-grid">
        <div class="ui-card">
          <div class="ui-section-title">Account Overview</div>
          <div class="ui-muted" style="margin-top:8px">Equity ${escapeHtml(formatCurrency(account.equity ?? account.net_liquidation, 0))} · Cash ${escapeHtml(formatCurrency(account.cash ?? account.total_cash, 0))} · Buying Power ${escapeHtml(formatCurrency(account.buying_power, 0))}</div>
          <div class="ui-muted" style="margin-top:6px">Exposure ${escapeHtml(formatSignedPct((account.exposure_pct || 0) / 100, 1))} · Margin ${escapeHtml(formatCurrency(account.maintenance_margin, 0))} · Unrealized ${escapeHtml(formatCurrency(account.unrealized_pnl, 0))}</div>
        </div>
        <div class="ui-card">
          <div class="ui-section-title">Connection Health</div>
          <div class="ui-muted" style="margin-top:8px">Connection ${escapeHtml(connectionText)} · Market ${escapeHtml(connection.market_hours || "n/a")}</div>
          <div class="ui-muted" style="margin-top:6px">Pending orders ${escapeHtml(pendingOrders.length)} · Positions ${escapeHtml(positions.length)}</div>
          ${connection.next_open ? `<div class="ui-muted" style="margin-top:6px">Next open ${escapeHtml(connection.next_open)}</div>` : ""}
          ${Object.keys(readiness).length ? `<div class="ui-muted" style="margin-top:6px">Readiness ${escapeHtml(readiness.all_clear ? "all clear" : "check config")}</div>` : ""}
          <div class="ui-muted" style="margin-top:6px">Emergency liquidation ${escapeHtml(connection.connected === true ? "available" : "offline")}</div>
        </div>
      </div>
      <div class="ui-card" style="margin-top:12px">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">System Health</div>
            <div class="ui-muted" style="margin-top:6px">
              <span class="${health.status === "ok" ? "ui-badge ui-badge--safe" : health.status === "degraded" ? "ui-badge ui-badge--warn" : "ui-badge ui-badge--bad"}">${escapeHtml(health.status || "unknown")}</span>
              Uptime ${escapeHtml(formatFixed((Number(health.uptime_seconds || 0) / 60), 1))} min · Last backup ${escapeHtml(String(backup.last_backup_at || "never").slice(0, 19).replace("T", " "))}
            </div>
          </div>
          <div style="display:flex; gap:8px; flex-wrap:wrap">
            <button class="btn btn--secondary btn--sm" type="button" id="regimeBackupNow">Backup Now</button>
            <button class="btn btn--secondary btn--sm" type="button" id="regimeRecoveryRun" ${Number(health.stuck_orders || 0) ? "" : "disabled"}>Run Recovery</button>
          </div>
        </div>
        <div class="ui-muted" style="margin-top:8px">DB ${escapeHtml((health.db || {}).integrity || "unknown")} · Watchdog ${escapeHtml((health.watchdog || {}).running ? "running" : "stopped")} · Active alerts ${escapeHtml(health.active_alerts || 0)} · Stuck orders ${escapeHtml(health.stuck_orders || 0)}</div>
        <div class="ui-muted" style="margin-top:6px">Database size ${escapeHtml(formatFixed(Number(health.db_size_bytes || 0) / 1024 / 1024, 2))} MB · Model ${escapeHtml(model.active_version ? `ML Active v${model.active_version}` : "No model")}${model.last_trained_at ? ` · Trained ${escapeHtml(relativeTime(model.last_trained_at))}` : ""}</div>
        <div class="ui-muted" style="margin-top:6px">Last regime check ${escapeHtml(relativeTime(health.last_regime_check) || "never")} · Last plan generation ${escapeHtml(relativeTime(health.last_paper_plans) || "never")} · Heartbeat <span class="${heartbeatBadge}">${escapeHtml(health.heartbeat ? relativeTime(health.heartbeat) : "missing")}</span></div>
        <div class="ui-muted" style="margin-top:6px">Event Bus <span class="${eventBus.running ? "ui-badge ui-badge--safe" : "ui-badge ui-badge--bad"}">${escapeHtml(eventBus.running ? `Active (${eventBus.subscriber_count || 0} subscribers)` : "Stopped")}</span> · History ${escapeHtml(eventBus.history_size || 0)}</div>
        <div class="ui-muted" style="margin-top:6px">Agents <span class="${agentList.length && enabledAgents === agentList.length ? "ui-badge ui-badge--safe" : agentList.length ? "ui-badge ui-badge--warn" : "ui-badge ui-badge--neutral"}">${escapeHtml(`${agentData.count || 0} registered · ${enabledAgents} enabled`)}</span>${agentList.length ? ` · ${escapeHtml(agentList.map((agent) => `${agent.name}${agent.enabled ? "" : " (disabled)"}`).join(", "))}` : ""}</div>
        <div class="ui-muted" style="margin-top:6px">Out-of-band watchdog: configure manually</div>
        ${Array.isArray((validation || {}).issues) && validation.issues.length ? `<div class="ui-muted" style="margin-top:8px">${escapeHtml(validation.issues.join(" | "))}</div>` : ""}
        <details style="margin-top:10px">
          <summary style="cursor:pointer; font-weight:600">Backups (${escapeHtml((backup.backup_count || 0))})</summary>
          <div class="ui-muted" style="margin-top:8px">Directory ${escapeHtml(backup.backup_dir || "")} · Total size ${escapeHtml(formatCurrency((Number(backup.total_size_bytes || 0) / 1024 / 1024), 2))} MB</div>
        </details>
      </div>
      <div class="regime-monitor-grid" style="margin-top:12px">
        <div class="ui-card">
          <div class="ui-section-title">Guardrails</div>
          <div style="display:grid; gap:8px; margin-top:8px">
            ${Object.entries(guardrails).map(([name, item]) => `
              <div class="regime-guardrail ${item && item.ok ? "regime-guardrail--ok" : "regime-guardrail--warn"}">
                <div style="font-weight:600">${escapeHtml(name.replace(/_/g, " "))}</div>
                <div class="ui-muted">Current ${escapeHtml(String(item && item.current != null ? item.current : "—"))} · Limit ${escapeHtml(String(item && item.limit != null ? item.limit : "—"))}</div>
              </div>
            `).join("")}
          </div>
        </div>
        <div class="ui-card">
          <div class="ui-section-title">Pending Orders</div>
          ${pendingOrders.length ? `
            <div style="display:grid; gap:8px; margin-top:8px">
              ${pendingOrders.map((plan) => `<div class="regime-order-card ${planStatusClass(plan.status)}">
                <div class="table-toolbar">
                  <div><strong>${escapeHtml(plan.ticker || "")}</strong> <span class="ui-muted">${escapeHtml(plan.status || "")}</span></div>
                  <button class="btn btn--danger btn--sm" type="button" data-paper-cancel-plan="${escapeHtml(plan.id)}">Cancel</button>
                </div>
              </div>`).join("")}
            </div>
          ` : '<div class="ui-muted" style="margin-top:8px">No non-terminal orders.</div>'}
        </div>
      </div>
      <div class="ui-card" style="margin-top:12px">
        <div class="ui-section-title">Positions</div>
        <div class="table-wrap" style="margin-top:10px">
          <table>
            <thead>
              <tr>
                <th scope="col">Ticker</th>
                <th scope="col" class="num">Qty</th>
                <th scope="col" class="num">Avg Cost</th>
                <th scope="col" class="num">Market Value</th>
                <th scope="col" class="num">Unrealized</th>
              </tr>
            </thead>
            <tbody>
              ${positions.length ? positions.map((row) => `
                <tr>
                  <td>${escapeHtml(row.ticker || row.contract_symbol || "")}</td>
                  <td class="num ui-tabular-nums">${escapeHtml(formatFixed(row.quantity, 2))}</td>
                  <td class="num ui-tabular-nums">${escapeHtml(formatCurrency(row.avg_cost, 2))}</td>
                  <td class="num ui-tabular-nums">${escapeHtml(formatCurrency(row.market_value, 2))}</td>
                  <td class="num ui-tabular-nums ${Number(row.unrealized_pnl || 0) >= 0 ? "cell-ok" : "cell-bad"}">${escapeHtml(formatCurrency(row.unrealized_pnl, 2))}</td>
                </tr>
              `).join("") : '<tr><td colspan="5" class="ui-muted">No open positions.</td></tr>'}
            </tbody>
          </table>
        </div>
      </div>
    `;
    mount.querySelectorAll("[data-paper-cancel-plan]").forEach((button) => {
      button.addEventListener("click", () => {
        cancelPaperOrder(currentPaperPortfolioId(), button.getAttribute("data-paper-cancel-plan"));
      });
    });
    const backupBtn = byId("regimeBackupNow");
    if (backupBtn && state.config?.endpoints?.backup_create) {
      backupBtn.addEventListener("click", async () => {
        try {
          const response = await fetch(state.config.endpoints.backup_create, {
            method: "POST",
            headers: { Accept: "application/json", "Content-Type": "application/json" },
            body: JSON.stringify({ label: "manual" }),
          });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Backup failed (${response.status})`);
          showToast(`Backup created: ${payload.path || "ok"}`);
          await refreshMonitoringDashboard();
        } catch (error) {
          showToast(`Unable to create backup: ${error.message || error}`, "error");
        }
      });
    }
    const recoveryBtn = byId("regimeRecoveryRun");
    if (recoveryBtn && state.config?.endpoints?.recovery_run) {
      recoveryBtn.addEventListener("click", async () => {
        if (!window.confirm("Run startup-style recovery now?")) return;
        try {
          const response = await fetch(state.config.endpoints.recovery_run, { method: "POST", headers: { Accept: "application/json" } });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Recovery failed (${response.status})`);
          showToast(`Recovery complete: ${payload.reconciled || 0} reconciled, ${payload.expired || 0} expired.`);
          await refreshMonitoringDashboard();
        } catch (error) {
          showToast(`Unable to run recovery: ${error.message || error}`, "error");
        }
      });
    }
  }

  function updateStatusBar() {
    const bar = byId("regimeStatusBar");
    if (!bar) return;
    const portfolio = currentPaperPortfolio();
    const monitoring = state.paperMonitoring || {};
    const account = monitoring.account || {};
    const connection = monitoring.connection || {};
    const pendingOrders = Array.isArray(monitoring.pending_orders) ? monitoring.pending_orders : [];
    const positions = Array.isArray(monitoring.positions) ? monitoring.positions : [];
    const visible = !!(portfolio && (String(portfolio.broker_type || "").toLowerCase() === "ibkr" || positions.length));
    if (!visible) {
      bar.style.display = "none";
      document.documentElement.style.setProperty("--sticky-header-top", "var(--top-nav-height)");
      return;
    }
    bar.style.display = "flex";
    document.documentElement.style.setProperty("--sticky-header-top", "calc(var(--top-nav-height) + 34px)");
    const dot = byId("regimeStatusDot");
    const connectionEl = byId("regimeStatusConnection");
    const marketEl = byId("regimeStatusMarket");
    const equityEl = byId("regimeStatusEquity");
    const pnlEl = byId("regimeStatusPnl");
    const exposureEl = byId("regimeStatusExposure");
    const ordersEl = byId("regimeStatusOrders");
    const kill = byId("regimeStatusKillSwitch");
    const statusText = connection.connected === true
      ? "Connected"
      : connection.connected === false
        ? "Disconnected"
        : (connection.connection || "Reconnecting...");
    const dotClass = connection.connected === true
      ? "regime-status-bar__dot--connected"
      : connection.connected === false
        ? "regime-status-bar__dot--disconnected"
        : "regime-status-bar__dot--reconnecting";
    if (dot) dot.className = `regime-status-bar__dot ${dotClass}`;
    if (connectionEl) connectionEl.textContent = statusText;
    if (marketEl) marketEl.textContent = connection.market_hours || "Market Closed";
    if (equityEl) equityEl.textContent = formatCurrency(account.equity ?? account.net_liquidation, 0);
    if (pnlEl) pnlEl.textContent = formatCurrency(account.unrealized_pnl ?? account.daily_pnl, 0);
    if (exposureEl) exposureEl.textContent = `${formatFixed(account.exposure_pct || 0, 1)}%`;
    if (ordersEl) ordersEl.textContent = `${pendingOrders.length} pending orders`;
    if (kill) {
      kill.style.display = pendingOrders.length || positions.length ? "" : "none";
      kill.onclick = () => {
        const btn = byId("regimePaperKillSwitch");
        if (btn) btn.click();
      };
    }
  }

  function stopMonitoringPolling() {
    if (state.monitoringTimer) {
      window.clearInterval(state.monitoringTimer);
      state.monitoringTimer = null;
    }
  }

  async function refreshMonitoringDashboard() {
    const portfolioId = currentPaperPortfolioId();
    if (!portfolioId || !state.config?.endpoints?.paper_monitoring) {
      state.paperMonitoring = null;
      renderMonitoringDashboard(null);
      updateStatusBar();
      return;
    }
    const [monitoringResponse, healthResponse, validationResponse] = await Promise.all([
      fetch(paperEndpoint("paper_monitoring", portfolioId), { headers: { Accept: "application/json" } }),
      fetch(state.config.endpoints.health, { headers: { Accept: "application/json" } }),
      fetch(state.config.endpoints.data_validation, { headers: { Accept: "application/json" } }),
    ]);
    const [payload, health, validation] = await Promise.all([
      monitoringResponse.json(),
      healthResponse.json(),
      validationResponse.json(),
    ]);
    if (!monitoringResponse.ok) throw new Error(payload.detail || `Monitoring failed (${monitoringResponse.status})`);
    state.paperMonitoring = payload;
    if (healthResponse.ok) state.systemHealth = health;
    if (validationResponse.ok) state.dataValidation = validation;
    renderMonitoringDashboard(payload);
    updateStatusBar();
  }

  async function loadAlerts() {
    if (!state.config?.endpoints?.alerts) return;
    const [unackResponse, historyResponse] = await Promise.all([
      fetch(`${state.config.endpoints.alerts}?unacknowledged=true&limit=5`, { headers: { Accept: "application/json" } }),
      fetch(`${state.config.endpoints.alerts}?limit=25`, { headers: { Accept: "application/json" } }),
    ]);
    const [unackPayload, historyPayload] = await Promise.all([unackResponse.json(), historyResponse.json()]);
    if (unackResponse.ok) {
      state.unacknowledgedAlerts = Array.isArray(unackPayload.alerts) ? unackPayload.alerts : [];
    }
    if (historyResponse.ok) {
      state.alertHistory = Array.isArray(historyPayload.alerts) ? historyPayload.alerts : [];
    }
    renderAlertToasts();
    renderAlertHistory();
  }

  async function loadVixStatus() {
    if (!state.config?.endpoints?.vix_status) return;
    const response = await fetch(state.config.endpoints.vix_status, { headers: { Accept: "application/json" } });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `Unable to load VIX status (${response.status})`);
    state.vixStatus = payload;
    renderVixStatus();
  }

  function startMonitoringPolling() {
    stopMonitoringPolling();
    const portfolio = currentPaperPortfolio();
    if (!portfolio || String(portfolio.broker_type || "paper").toLowerCase() !== "ibkr") return;
    const interval = isMarketOpenForMonitoring() ? 30000 : 120000;
    state.monitoringTimer = window.setInterval(async () => {
      try {
        await Promise.all([refreshMonitoringDashboard(), loadAlerts(), loadVixStatus()]);
      } catch (error) {
        stopMonitoringPolling();
        showToast(`Unable to refresh monitoring dashboard: ${error.message || error}`, "error");
      }
    }, interval);
  }

  function isMarketOpenForMonitoring() {
    const marketState = String((state.paperMonitoring && state.paperMonitoring.connection && state.paperMonitoring.connection.market_hours) || "").toLowerCase();
    return marketState === "regular" || marketState === "pre" || marketState === "after_hours";
  }

  async function renderOrderTimeline(orderId) {
    const portfolioId = currentPaperPortfolioId();
    const mount = byId("regimeOrderTimelineMount");
    if (!portfolioId || !mount || !orderId) return;
    try {
      const response = await fetch(`${paperEndpoint("paper_audit", portfolioId)}?order_id=${encodeURIComponent(orderId)}`, { headers: { Accept: "application/json" } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Timeline failed (${response.status})`);
      const rows = Array.isArray(payload.audit) ? payload.audit : [];
      mount.innerHTML = `
        <div class="ui-card" style="padding:12px; border:1px solid ${COLORS.border}">
          <div class="table-toolbar">
            <div class="ui-section-title">Order Timeline ${escapeHtml(String(orderId).slice(0, 8))}</div>
            <button class="btn btn--secondary" type="button" id="regimeTimelineClose">×</button>
          </div>
          <div style="display:grid; gap:10px; margin-top:10px">
            ${rows.map((row) => `<div style="border-left:3px solid ${["filled"].includes(row.event_type) ? COLORS.bull : ["rejected", "guardrail_blocked", "error"].includes(row.event_type) ? COLORS.bear : COLORS.warn}; padding-left:10px">
              <div style="font-weight:600">${escapeHtml(row.event_type || "")}</div>
              <div class="ui-muted">${escapeHtml(relativeTime(row.created_at))} · ${escapeHtml(row.details || "")}</div>
            </div>`).join("") || '<div class="ui-muted">No events for this order.</div>'}
          </div>
        </div>
      `;
      const close = byId("regimeTimelineClose");
      if (close) close.addEventListener("click", () => { mount.innerHTML = ""; });
    } catch (error) {
      showToast(`Unable to load order timeline: ${error.message || error}`, "error");
    }
  }

  function paperEndpoint(key, portfolioId, planId = null) {
    if (!state.config || !state.config.endpoints) return "";
    let endpoint = String(state.config.endpoints[key] || "");
    if (portfolioId != null) endpoint = endpoint.replace("__PORTFOLIO_ID__", encodeURIComponent(String(portfolioId)));
    if (planId != null) endpoint = endpoint.replace("__PLAN_ID__", encodeURIComponent(String(planId)));
    return endpoint;
  }

  function renderIBKRSettings() {
    const mount = byId("regimeIBKRSettings");
    if (!mount || !state.config?.endpoints?.ibkr_settings) return;
    const config = state.ibkrSettings || {};
    const readiness = state.ibkrReadiness || {};
    const status = state.ibkrStatus || {};
    const test = state.ibkrTestResult || null;
    const checks = [
      ["live_backend_enabled", "Live backend enabled"],
      ["port_is_valid", "Valid port"],
      ["host_is_local", "Localhost only"],
      ["account_configured", "Account configured"],
    ];
    const liveUnlocked = !!status.live_trading_unlocked;
    const liveAccountId = status?.config?.live_account_id || config.live_account_id || "";
    const accountLabel = liveAccountId ? `Live ${liveAccountId}` : `Paper ${config.account_id || ""}`;
    mount.innerHTML = `
      <section class="ui-card" style="padding:12px">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">IBKR Configuration</div>
            <div class="ui-muted" style="margin-top:4px">View, validate, and save TWS / Gateway paper settings. Changes write to .env and require a restart.</div>
          </div>
        </div>
        <div class="${readiness.all_clear ? "banner--ok" : "ui-card"}" style="margin-top:12px; padding:10px">
          <div style="font-weight:600">${readiness.all_clear ? "All Clear" : "Configuration Required"}</div>
          <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:8px">
            ${checks.map(([key, label]) => `<span class="${readiness[key] ? "ui-badge ui-badge--safe" : "ui-badge ui-badge--neutral"}">${readiness[key] ? "✓" : "•"} ${escapeHtml(label)}</span>`).join("")}
          </div>
        </div>
        ${state.ibkrRestartRequired ? '<div class="banner--warn" style="margin-top:10px; padding:10px">Settings saved. Restart the server (`make run`) to apply changes.</div>' : ""}
        <div class="ui-card" style="padding:10px; margin-top:10px">
          <div style="font-weight:600">Connection Status</div>
          <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:8px">
            <span class="${status.ib_thread_alive ? "ui-badge ui-badge--safe" : "ui-badge ui-badge--bad"}">IB Thread ${status.ib_thread_alive ? "alive" : "down"}</span>
            <span class="${readiness.live_backend_enabled ? "ui-badge ui-badge--warn" : "ui-badge ui-badge--neutral"}">${escapeHtml(accountLabel)}</span>
            <span class="${liveUnlocked ? "ui-badge ui-badge--bad" : "ui-badge ui-badge--safe"}">Live ${liveUnlocked ? "Unlocked" : "Locked"}</span>
          </div>
        </div>
        <form id="regimeIBKRSettingsForm" style="display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:10px; margin-top:12px">
          <label>Host<input type="text" name="host" value="${escapeHtml(config.host || "127.0.0.1")}" readonly /></label>
          <label>Port<select name="port"><option value="7497" ${Number(config.port || 7497) === 7497 ? "selected" : ""}>7497 (TWS paper)</option><option value="4002" ${Number(config.port || 7497) === 4002 ? "selected" : ""}>4002 (Gateway paper)</option><option value="7496" ${Number(config.port || 7497) === 7496 ? "selected" : ""}>7496 (TWS live)</option><option value="4001" ${Number(config.port || 7497) === 4001 ? "selected" : ""}>4001 (Gateway live)</option></select></label>
          <label>Client ID<input type="number" min="1" max="32" name="client_id" value="${escapeHtml(config.client_id ?? 1)}" /></label>
          <label>Account ID *<input type="text" name="account_id" value="${escapeHtml(config.account_id || "")}" required /></label>
          <label>Live Account ID<input type="text" name="live_account_id" value="${escapeHtml(config.live_account_id || "")}" /></label>
          <label>Timeout<input type="number" min="5" max="60" name="timeout" value="${escapeHtml(config.timeout ?? 10)}" /></label>
          <label style="display:flex; align-items:center; gap:8px; margin-top:24px"><input type="checkbox" name="live_backend" ${config.live_backend ? "checked" : ""} /> Live Backend</label>
          <div style="grid-column:1 / -1; display:flex; gap:8px; flex-wrap:wrap">
            <button class="btn btn--secondary" type="submit">Save Settings</button>
            <button class="btn btn--primary" type="button" id="regimeIBKRTestConnection">Test Connection</button>
            <button class="btn ${liveUnlocked ? "btn--secondary" : "btn--danger"}" type="button" id="regimeIBKRLiveToggle">${liveUnlocked ? "Lock Live Trading" : "Unlock Live Trading"}</button>
          </div>
        </form>
        ${test ? `
          <div class="ui-card" style="padding:10px; margin-top:12px">
            <div style="font-weight:600">Connection Test</div>
            <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:8px">
              <span class="${test.tcp_reachable ? "ui-badge ui-badge--safe" : "ui-badge ui-badge--bad"}">TCP ${test.tcp_reachable ? "reachable" : "unreachable"}</span>
              <span class="${test.ibkr_connected === true ? "ui-badge ui-badge--safe" : test.ibkr_connected === false ? "ui-badge ui-badge--bad" : "ui-badge ui-badge--neutral"}">IBKR ${test.ibkr_connected === true ? "connected" : test.ibkr_connected === false ? "failed" : "not tested"}</span>
              <span class="${test.account_verified ? "ui-badge ui-badge--safe" : "ui-badge ui-badge--neutral"}">Account ${test.account_verified ? "verified" : "unverified"}</span>
            </div>
            <div class="ui-muted" style="margin-top:8px">${escapeHtml(test.error || test.note || (test.net_liquidation != null ? `Net Liquidation ${formatCurrency(test.net_liquidation, 0)}` : "No result yet."))}</div>
          </div>
        ` : ""}
      </section>
    `;
    const form = byId("regimeIBKRSettingsForm");
    if (form) {
      form.addEventListener("submit", saveIBKRSettings);
    }
    const testBtn = byId("regimeIBKRTestConnection");
    if (testBtn) testBtn.addEventListener("click", testIBKRConnection);
    const liveToggleBtn = byId("regimeIBKRLiveToggle");
    if (liveToggleBtn) liveToggleBtn.addEventListener("click", toggleLiveTrading);
  }

  async function loadIBKRSettings() {
    if (!state.config?.endpoints?.ibkr_settings) return;
    try {
      const response = await fetch(state.config.endpoints.ibkr_settings, { headers: { Accept: "application/json" } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Unable to load IBKR settings (${response.status})`);
      state.ibkrSettings = payload.config || {};
      state.ibkrReadiness = payload.readiness || {};
      if (state.config?.endpoints?.ibkr_status) {
        const statusResponse = await fetch(state.config.endpoints.ibkr_status, { headers: { Accept: "application/json" } });
        const statusPayload = await statusResponse.json();
        if (statusResponse.ok) state.ibkrStatus = statusPayload;
      }
      renderIBKRSettings();
    } catch (error) {
      const mount = byId("regimeIBKRSettings");
      if (mount) {
        mount.innerHTML = `<div class="ui-card" style="padding:12px"><div class="ui-section-title">IBKR Configuration</div><div class="ui-muted" style="margin-top:6px">Unable to load settings: ${escapeHtml(error.message || error)}</div></div>`;
      }
    }
  }

  async function saveIBKRSettings(event) {
    event.preventDefault();
    const form = event.currentTarget;
    const body = new URLSearchParams();
    const fd = new FormData(form);
    body.set("host", String(fd.get("host") || "127.0.0.1"));
    body.set("port", String(fd.get("port") || "7497"));
    body.set("client_id", String(fd.get("client_id") || "1"));
    body.set("account_id", String(fd.get("account_id") || ""));
    body.set("live_account_id", String(fd.get("live_account_id") || ""));
    body.set("timeout", String(fd.get("timeout") || "10"));
    body.set("live_backend", fd.get("live_backend") ? "true" : "false");
    try {
      const response = await fetch(state.config.endpoints.ibkr_settings, {
        method: "POST",
        body,
        headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Save failed (${response.status})`);
      state.ibkrRestartRequired = !!payload.restart_required;
      showToast(payload.message || "Settings saved.");
      await loadIBKRSettings();
    } catch (error) {
      showToast(`Unable to save IBKR settings: ${error.message || error}`, "error");
    }
  }

  async function testIBKRConnection() {
    try {
      const response = await fetch(state.config.endpoints.ibkr_test_connection, {
        method: "POST",
        headers: { Accept: "application/json" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Connection test failed (${response.status})`);
      state.ibkrTestResult = payload;
      renderIBKRSettings();
      showToast(payload.ibkr_connected === false || payload.tcp_reachable === false ? "Connection test failed." : "Connection test complete.");
    } catch (error) {
      showToast(`Unable to test IBKR connection: ${error.message || error}`, "error");
    }
  }

  async function toggleLiveTrading() {
    if (!state.config?.endpoints?.ibkr_live_unlock) return;
    const liveUnlocked = !!(state.ibkrStatus && state.ibkrStatus.live_trading_unlocked);
    const nextUnlocked = !liveUnlocked;
    const liveAccountId = state.ibkrStatus?.config?.live_account_id || state.ibkrSettings?.live_account_id || "configured live account";
    if (nextUnlocked) {
      const confirmed = window.prompt(`WARNING: This will enable live trading with real money on account ${liveAccountId}.\nType exactly: I understand the risks`);
      if (confirmed !== "I understand the risks") {
        showToast("Live trading unlock cancelled.", "error");
        return;
      }
    }
    try {
      const response = await fetch(state.config.endpoints.ibkr_live_unlock, {
        method: "PUT",
        headers: { Accept: "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({
          unlocked: nextUnlocked,
          confirm: nextUnlocked ? "I understand the risks" : "",
        }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Live toggle failed (${response.status})`);
      if (!state.ibkrStatus) state.ibkrStatus = {};
      state.ibkrStatus.live_trading_unlocked = !!payload.live_trading_unlocked;
      renderIBKRSettings();
      showToast(payload.live_trading_unlocked ? "Live trading unlocked." : "Live trading locked.");
    } catch (error) {
      showToast(`Unable to update live trading lock: ${error.message || error}`, "error");
    }
  }

  function renderPaperBudget() {
    const mount = byId("regimeBudgetMount");
    if (!mount) return;
    const budget = state.paperBudget;
    if (!budget || !Array.isArray(budget.themes)) {
      mount.innerHTML = '<div class="regime-empty-state"><div class="regime-empty-state__title">No budget view yet</div><div class="ui-muted">Budget allocation appears after you create or select a paper portfolio.</div></div>';
      return;
    }
    mount.innerHTML = `
      <div class="ui-card" style="padding:12px">
        <div class="ui-section-title">Budget Allocation</div>
        <div class="ui-muted" style="margin-top:6px">Cash reserve ${escapeHtml(formatCurrency(budget.cash_reserve, 0))} · Unallocated ${escapeHtml(formatCurrency(budget.unallocated, 0))}</div>
        <div style="display:grid; gap:10px; margin-top:10px">
          ${budget.themes.map((theme) => {
            const core = Number((theme.by_role || {}).Core || 0);
            const critical = Number((theme.by_role || {})["Critical-Path"] || 0);
            const speculative = Number((theme.by_role || {}).Speculative || 0);
            const total = Math.max(1, Number(theme.allocated || 0));
            return `
              <div style="border:1px solid ${COLORS.border}; border-radius:10px; padding:10px">
                <div class="table-toolbar">
                  <div>
                    <div style="font-weight:600">${escapeHtml(theme.theme_name || "")}</div>
                    <div class="ui-muted">Conviction ${escapeHtml(theme.conviction || 0)}/5 · ${escapeHtml(formatCurrency(theme.allocated, 0))}</div>
                  </div>
                </div>
                <div style="display:flex; width:100%; height:10px; border-radius:999px; overflow:hidden; background:#e5e7eb; margin-top:8px">
                  <div style="width:${((core / total) * 100).toFixed(1)}%; background:#2563eb" title="Core ${core}"></div>
                  <div style="width:${((critical / total) * 100).toFixed(1)}%; background:#16a34a" title="Critical-Path ${critical}"></div>
                  <div style="width:${((speculative / total) * 100).toFixed(1)}%; background:#f59e0b" title="Speculative ${speculative}"></div>
                </div>
                <div class="ui-muted" style="margin-top:6px">Core ${escapeHtml(formatCurrency(core, 0))} · Critical-Path ${escapeHtml(formatCurrency(critical, 0))} · Speculative ${escapeHtml(formatCurrency(speculative, 0))}</div>
              </div>
            `;
          }).join("")}
        </div>
      </div>
    `;
  }

  function renderPaperPlans() {
    const mount = byId("regimeTradePlansMount");
    if (!mount) return;
    const plans = Array.isArray(state.paperPlans) ? state.paperPlans : [];
    const approvedCount = plans.filter((plan) => String(plan.status || "") === "Approved").length;
    const autonomyMode = String((state.autonomySettings || {}).operating_mode || "manual");
    const portfolio = currentPaperPortfolio();
    const portfolioStatus = String((portfolio && portfolio.status) || "Active");
    const paused = portfolioStatus === "Paused";
    const closed = portfolioStatus === "Closed";
    if (!currentPaperPortfolioId()) {
      mount.innerHTML = '<div class="regime-empty-state"><div class="regime-empty-state__title">No active trading portfolio</div><div class="ui-muted">Select a paper portfolio to manage trade plans.</div></div>';
      return;
    }
    mount.innerHTML = `
      <div class="ui-card" style="padding:12px">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">Trade Plans</div>
            <div class="ui-muted">Pending and approved plans for the selected paper portfolio.</div>
          </div>
          <div style="display:flex; gap:8px; flex-wrap:wrap">
            <button class="btn btn--secondary" type="button" id="regimePaperGenerate" ${paused || closed ? "disabled" : ""}>Generate Plans</button>
            ${autonomyMode !== "manual" ? `<button class="btn btn--secondary" type="button" id="regimePaperAutoApprove" ${paused || closed ? "disabled" : ""}>Run Auto-Approve</button>` : ""}
            <button class="btn btn--secondary" type="button" id="regimePaperApproveAll" ${paused || closed ? "disabled" : ""}>Approve All</button>
            <button class="btn btn--secondary" type="button" id="regimePaperRejectAll">Reject All</button>
            <button class="btn btn--primary" type="button" id="regimePaperExecute" ${approvedCount && !paused && !closed ? "" : "disabled"}>Execute Approved (${approvedCount})</button>
          </div>
        </div>
        <div style="display:grid; gap:10px; margin-top:12px">
          ${plans.length ? plans.map((plan) => `
            <div class="regime-plan-card ${planStatusClass(plan.status)}">
              <div class="table-toolbar">
                <div>
                  <div style="font-weight:600">${escapeHtml(plan.ticker || "")} <span class="${plan.action === "Buy" ? "cell-ok" : "cell-bad"}" style="margin-left:6px">${escapeHtml(String(plan.action || "").toUpperCase())}</span></div>
                  <div class="ui-muted">${escapeHtml(plan.status || "Pending")}${String(plan.notes || "").includes("Auto-approved") ? ' · <span class="ui-badge ui-badge--neutral">Auto</span>' : ""} · ${escapeHtml(plan.source || "manual")} · Qty ${escapeHtml(plan.quantity || 0)} @ ${escapeHtml(formatCurrency(plan.proposed_price, 2))}${plan.broker_status ? ` · Broker ${escapeHtml(plan.broker_status)}` : ""}</div>
                </div>
                <div style="min-width:110px">${plan.crowd_score != null ? renderCrowdScore(plan.crowd_score) : ""}</div>
              </div>
              <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:6px">
                ${renderGuardrailBadge(plan)}
                ${plan.regime_label ? `<span class="${badgeClass(plan.regime_label)}">${escapeHtml(plan.regime_label)} ${plan.regime_probability != null ? `· ${(Number(plan.regime_probability) * 100).toFixed(0)}%` : ""}</span>` : ""}
                ${plan.meta_labeler_score == null ? `<span class="ui-badge ui-badge--neutral">ML N/A</span>` : `<span class="${Number(plan.meta_labeler_score) >= 0.65 ? "ui-badge ui-badge--safe" : Number(plan.meta_labeler_score) >= 0.30 ? "ui-badge ui-badge--warn" : "ui-badge ui-badge--bad"}">ML ${(Number(plan.meta_labeler_score) * 100).toFixed(0)}%</span>`}
                ${renderTaxImpactBadge(plan)}
              </div>
              <div class="ui-muted" style="margin-top:6px">${escapeHtml(plan.rationale || "")}</div>
              ${plan.execution_result ? `<div class="ui-muted" style="margin-top:6px">${escapeHtml(plan.execution_result)}</div>` : ""}
              <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:10px">
                <button class="btn btn--secondary" type="button" data-paper-plan-action="Approved" data-paper-plan-id="${escapeHtml(plan.id)}" ${!((planPrecheck(plan.id) || {}).guardrail_passed) || paused || closed ? "disabled" : ""}>${(planPrecheck(plan.id) && !(planPrecheck(plan.id).guardrail_passed)) ? "Blocked" : "Approve"}</button>
                <button class="btn btn--secondary" type="button" data-paper-plan-action="Rejected" data-paper-plan-id="${escapeHtml(plan.id)}">Reject</button>
                <button class="btn btn--secondary" type="button" data-paper-plan-action="Modified" data-paper-plan-id="${escapeHtml(plan.id)}" ${closed ? "disabled" : ""}>Modify Qty</button>
              </div>
            </div>
          `).join("") : '<div class="regime-empty-state"><div class="regime-empty-state__title">No trade plans yet</div><div class="ui-muted">Generate plans to review discovery-driven buys and exits.</div></div>'}
        </div>
      </div>
    `;
    const generateBtn = byId("regimePaperGenerate");
    if (generateBtn) generateBtn.addEventListener("click", generatePaperPlans);
    const autoApproveBtn = byId("regimePaperAutoApprove");
    if (autoApproveBtn) autoApproveBtn.addEventListener("click", runAutoApprove);
    const approveAllBtn = byId("regimePaperApproveAll");
    if (approveAllBtn) approveAllBtn.addEventListener("click", async () => {
      for (const plan of plans) {
        const check = planPrecheck(plan.id);
        if (check && check.guardrail_passed) {
          // eslint-disable-next-line no-await-in-loop
          await updatePaperPlan(plan.id, "Approved");
        }
      }
    });
    const rejectAllBtn = byId("regimePaperRejectAll");
    if (rejectAllBtn) rejectAllBtn.addEventListener("click", async () => {
      for (const plan of plans) {
        // eslint-disable-next-line no-await-in-loop
        await updatePaperPlan(plan.id, "Rejected");
      }
    });
    const executeBtn = byId("regimePaperExecute");
    if (executeBtn) executeBtn.addEventListener("click", executePaperPlans);
    mount.querySelectorAll("[data-paper-plan-action]").forEach((button) => {
      button.addEventListener("click", async () => {
        const status = String(button.getAttribute("data-paper-plan-action") || "");
        const planId = String(button.getAttribute("data-paper-plan-id") || "");
        let quantity = null;
        if (status === "Modified") {
          quantity = window.prompt("New quantity");
          if (!quantity) return;
        }
        await updatePaperPlan(planId, status, quantity);
      });
    });
  }

  function renderPaperPositions() {
    const mount = byId("regimePositionsMount");
    if (!mount) return;
    const rows = Array.isArray(state.paperPositions) ? state.paperPositions : [];
    if (!currentPaperPortfolioId()) {
      mount.innerHTML = '<div class="regime-empty-state"><div class="regime-empty-state__title">No active portfolio</div><div class="ui-muted">Open positions appear after you select a paper portfolio.</div></div>';
      return;
    }
    mount.innerHTML = `
      <div class="ui-card" style="padding:12px">
        <div class="ui-section-title">Open Positions</div>
        <div class="table-wrap" style="margin-top:10px">
          <table>
            <thead>
              <tr>
                <th scope="col">Ticker</th>
                <th scope="col" class="num">Qty</th>
                <th scope="col" class="num">Entry</th>
                <th scope="col" class="num">Current</th>
                <th scope="col" class="num">Unrealized</th>
                <th scope="col" class="num">Stop</th>
                <th scope="col">Role</th>
                <th scope="col" class="num">Days Held</th>
              </tr>
            </thead>
            <tbody>
              ${rows.length ? rows.map((row) => {
                const pnl = Number(row.unrealized_pnl || 0);
                const daysHeld = row.entry_date ? Math.max(0, Math.floor((Date.now() - new Date(row.entry_date).getTime()) / 86400000)) : 0;
                return `<tr>
                  <td>${escapeHtml(row.ticker || "")}</td>
                  <td class="num ui-tabular-nums">${escapeHtml(row.quantity || 0)}</td>
                  <td class="num ui-tabular-nums">${escapeHtml(formatCurrency(row.entry_price, 2))}</td>
                  <td class="num ui-tabular-nums">${escapeHtml(formatCurrency(row.current_price || row.entry_price, 2))}</td>
                  <td class="num ui-tabular-nums ${pnl >= 0 ? "cell-ok" : "cell-bad"}">
                    <div style="display:flex; align-items:center; justify-content:flex-end; gap:6px">
                      ${renderPnlSparkline(pnl)}
                      <span>${escapeHtml(formatCurrency(pnl, 2))}</span>
                    </div>
                  </td>
                  <td class="num ui-tabular-nums">${escapeHtml(formatCurrency(row.stop_price, 2))}</td>
                  <td>${escapeHtml(row.role || "")}</td>
                  <td class="num ui-tabular-nums">${escapeHtml(daysHeld)}</td>
                </tr>`;
              }).join("") : '<tr><td colspan="8" class="ui-muted">No open positions.</td></tr>'}
            </tbody>
          </table>
        </div>
        <details style="margin-top:12px" open>
          <summary style="cursor:pointer; font-weight:600">Tax Lots</summary>
          ${renderTaxLotsTable()}
        </details>
        <details style="margin-top:12px">
          <summary style="cursor:pointer; font-weight:600">Wash-Sale Restrictions</summary>
          ${renderWashSaleTable()}
        </details>
      </div>
    `;
  }

  function renderTaxLotsTable() {
    const rows = Array.isArray(state.paperTaxLots) ? state.paperTaxLots : [];
    const deferWindow = Number((state.taxSettings || {}).ltcg_defer_window_days || 30);
    return `
      <div class="table-wrap" style="margin-top:10px">
        <table>
          <thead>
            <tr>
              <th scope="col">Ticker</th>
              <th scope="col">Lot #</th>
              <th scope="col" class="num">Qty</th>
              <th scope="col" class="num">Cost Basis</th>
              <th scope="col">Acquired</th>
              <th scope="col" class="num">Days Held</th>
              <th scope="col">Term</th>
              <th scope="col" class="num">Days to LTCG</th>
              <th scope="col">Status</th>
            </tr>
          </thead>
          <tbody>
            ${rows.length ? rows.map((row) => {
              const highlight = String(row.term || "") === "ST" && Number(row.days_to_ltcg || 0) <= deferWindow;
              return `<tr${highlight ? ' style="background:#fef3c7"' : ""}>
                <td>${escapeHtml(row.ticker || "")}</td>
                <td>${escapeHtml(row.id || "")}</td>
                <td class="num ui-tabular-nums">${escapeHtml(row.remaining_quantity || 0)}</td>
                <td class="num ui-tabular-nums">${escapeHtml(formatCurrency(row.cost_basis_per_share, 2))}</td>
                <td>${escapeHtml(String(row.acquisition_date || "").slice(0, 10))}</td>
                <td class="num ui-tabular-nums">${escapeHtml(row.days_held || 0)}</td>
                <td>${String(row.term || "") === "LT" ? '<span class="ui-badge ui-badge--safe">LT</span>' : '<span class="ui-badge ui-badge--warn">ST</span>'}</td>
                <td class="num ui-tabular-nums">${String(row.term || "") === "LT" ? "—" : escapeHtml(row.days_to_ltcg || 0)}</td>
                <td>${escapeHtml(row.status || "")}</td>
              </tr>`;
            }).join("") : '<tr><td colspan="9" class="ui-muted">No tax lots available.</td></tr>'}
          </tbody>
        </table>
      </div>
    `;
  }

  function renderWashSaleTable() {
    const rows = Array.isArray(state.paperWashSale) ? state.paperWashSale : [];
    return `
      <div class="table-wrap" style="margin-top:10px">
        <table>
          <thead>
            <tr>
              <th scope="col">Ticker</th>
              <th scope="col">Loss Date</th>
              <th scope="col" class="num">Loss Amount</th>
              <th scope="col">Expires</th>
            </tr>
          </thead>
          <tbody>
            ${rows.length ? rows.map((row) => `<tr>
              <td>${escapeHtml(row.ticker || "")}</td>
              <td>${escapeHtml(String(row.loss_sale_date || "").slice(0, 10))}</td>
              <td class="num ui-tabular-nums cell-bad">${escapeHtml(formatCurrency(row.loss_amount, 2))}</td>
              <td>${escapeHtml(String(row.restriction_expires || "").slice(0, 10))} ${row.days_remaining != null ? `· ${escapeHtml(row.days_remaining)} days` : ""}</td>
            </tr>`).join("") : '<tr><td colspan="4" class="ui-muted">No wash-sale restrictions active.</td></tr>'}
          </tbody>
        </table>
      </div>
    `;
  }

  function renderPaperPerformance() {
    const mount = byId("regimePerformanceMount");
    if (!mount) return;
    const performance = state.paperPerformance || null;
    const attribution = state.paperAttribution || null;
    if (!currentPaperPortfolioId()) {
      mount.innerHTML = '<div class="regime-empty-state"><div class="regime-empty-state__title">No active portfolio</div><div class="ui-muted">Performance attribution appears after you select a paper portfolio.</div></div>';
      return;
    }
    if (!performance) {
      mount.innerHTML = '<div class="regime-empty-state"><div class="regime-empty-state__title">Performance unavailable</div><div class="ui-muted">Performance data unavailable.</div></div>';
      return;
    }
    const benchmark = performance.benchmark || {};
    const metrics = performance.performance || performance;
    mount.innerHTML = `
      <div class="ui-card" style="padding:12px">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">Performance</div>
            <div class="ui-muted" style="margin-top:6px">Return ${escapeHtml(formatSignedPct((metrics.total_return_pct || 0) / 100, 2))} · Win rate ${escapeHtml(formatSignedPct(metrics.win_rate, 1))} · Alpha vs ${escapeHtml(benchmark.benchmark || "SPY")} ${escapeHtml(formatSignedPct((benchmark.alpha_pct || benchmark.alpha || 0) / 100, 2))}</div>
          </div>
          <button class="btn btn--secondary" type="button" id="regimePaperAttributionLoad">Performance Report</button>
        </div>
        <div class="ui-muted" style="margin-top:6px">Realized ${escapeHtml(formatCurrency(metrics.realized_pnl, 2))} · Unrealized ${escapeHtml(formatCurrency(metrics.unrealized_pnl, 2))} · Market value ${escapeHtml(formatCurrency(metrics.total_market_value, 2))}</div>
        ${Array.isArray(metrics.snapshots) && metrics.snapshots.length ? '<div id="regimePaperEquityCurve" style="min-height:220px; margin-top:12px"></div>' : ""}
        ${renderPerformanceAttribution(attribution, metrics)}
      </div>
    `;
    const loadBtn = byId("regimePaperAttributionLoad");
    if (loadBtn) loadBtn.addEventListener("click", loadPaperAttribution);
    if (Array.isArray(metrics.snapshots) && metrics.snapshots.length && window.Plotly) {
      const chartMount = byId("regimePaperEquityCurve");
      if (chartMount) {
        window.Plotly.newPlot(
          chartMount,
          [{
            x: metrics.snapshots.map((row) => row.snapshot_date || row.date),
            y: metrics.snapshots.map((row) => row.equity),
            mode: "lines",
            name: "Equity",
            line: { color: COLORS.ink, width: 2 },
          }],
          { margin: { l: 40, r: 10, t: 10, b: 30 }, height: 220, template: "plotly_white" },
          { responsive: true },
        );
      }
    }
  }

  function renderAttributionTable(headers, rowsHtml, emptyText) {
    return `
      <div class="table-wrap" style="margin-top:10px">
        <table>
          <thead><tr>${headers.map((header) => `<th scope="col">${header}</th>`).join("")}</tr></thead>
          <tbody>${rowsHtml || `<tr><td colspan="${headers.length}" class="ui-muted">${escapeHtml(emptyText)}</td></tr>`}</tbody>
        </table>
      </div>
    `;
  }

  function calibrationGapClass(value) {
    const gap = Math.abs(Number(value || 0));
    if (gap <= 0.10) return "cell-ok";
    if (gap <= 0.20) return "cell-warn";
    return "cell-bad";
  }

  function renderPerformanceAttribution(attribution, metrics) {
    if (!attribution) {
      return '<div class="ui-muted" style="margin-top:12px">Click Performance Report to load theme, source, regime, and ML attribution.</div>';
    }
    const snapshots = Array.isArray(metrics.snapshots) ? metrics.snapshots : [];
    const maxDrawdown = snapshots.length ? Math.min(...snapshots.map((row) => Number(row.drawdown_pct || 0))) : 0;
    const themeRows = ((attribution.theme_attribution || {}).themes || []).map((row) => `
      <tr>
        <td>${escapeHtml(row.theme_name || "Unassigned")}</td>
        <td class="num ui-tabular-nums ${Number(row.total_pnl || 0) >= 0 ? "cell-ok" : "cell-bad"}">${escapeHtml(formatCurrency(row.total_pnl, 2))}</td>
        <td class="num ui-tabular-nums">${escapeHtml(row.position_count || 0)}</td>
        <td class="num ui-tabular-nums">${row.win_rate == null ? "—" : escapeHtml(formatSignedPct(row.win_rate, 1))}</td>
        <td>${row.best_trade ? `${escapeHtml(row.best_trade.ticker)} ${escapeHtml(formatCurrency(row.best_trade.pnl, 2))}` : "—"}</td>
        <td>${row.worst_trade ? `${escapeHtml(row.worst_trade.ticker)} ${escapeHtml(formatCurrency(row.worst_trade.pnl, 2))}` : "—"}</td>
      </tr>
    `).join("");
    const sourceRows = ((attribution.source_attribution || {}).sources || []).map((row) => `
      <tr>
        <td><span class="ui-badge ui-badge--neutral">${escapeHtml(row.source || "manual")}</span></td>
        <td class="num ui-tabular-nums">${escapeHtml(row.plan_count || 0)}</td>
        <td class="num ui-tabular-nums ${Number((row.total_realized_pnl || 0) + (row.total_unrealized_pnl || 0)) >= 0 ? "cell-ok" : "cell-bad"}">${escapeHtml(formatCurrency((row.total_realized_pnl || 0) + (row.total_unrealized_pnl || 0), 2))}</td>
        <td class="num ui-tabular-nums">${row.win_rate == null ? "—" : escapeHtml(formatSignedPct(row.win_rate, 1))}</td>
        <td class="num ui-tabular-nums">${row.avg_slippage_pct == null ? "—" : escapeHtml(formatSignedPct(row.avg_slippage_pct / 100, 2))}</td>
      </tr>
    `).join("");
    const regimeRows = ((attribution.regime_attribution || {}).regimes || []).map((row) => `
      <tr>
        <td><span class="${badgeClass(row.regime)}">${escapeHtml(row.regime || "Unknown")}</span></td>
        <td class="num ui-tabular-nums">${escapeHtml(row.position_count || 0)}</td>
        <td class="num ui-tabular-nums ${Number(row.total_pnl || 0) >= 0 ? "cell-ok" : "cell-bad"}">${escapeHtml(formatCurrency(row.total_pnl, 2))}</td>
        <td class="num ui-tabular-nums">${row.win_rate == null ? "—" : escapeHtml(formatSignedPct(row.win_rate, 1))}</td>
        <td class="num ui-tabular-nums">${row.avg_return_pct == null ? "—" : escapeHtml(formatSignedPct(row.avg_return_pct / 100, 2))}</td>
      </tr>
    `).join("");
    const ml = attribution.ml_accuracy || {};
    const calibrationRows = (ml.calibration || []).map((row) => `
      <tr>
        <td>${escapeHtml(row.band || "")}</td>
        <td class="num ui-tabular-nums">${escapeHtml(row.count || 0)}</td>
        <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(row.predicted_avg, 1))}</td>
        <td class="num ui-tabular-nums">${escapeHtml(formatSignedPct(row.actual_success_rate, 1))}</td>
        <td class="num ui-tabular-nums ${calibrationGapClass(row.calibration_gap)}">${escapeHtml(formatSignedPct(row.calibration_gap, 1))}</td>
      </tr>
    `).join("");
    const historyRows = (ml.model_history || []).map((row) => `
      <tr>
        <td class="num ui-tabular-nums">${escapeHtml(row.version || 0)}</td>
        <td class="num ui-tabular-nums">${row.accuracy == null ? "—" : escapeHtml(formatSignedPct(row.accuracy, 1))}</td>
        <td class="num ui-tabular-nums">${row.f1 == null ? "—" : escapeHtml(formatSignedPct(row.f1, 1))}</td>
        <td>${escapeHtml(row.ticker || "")}</td>
        <td>${escapeHtml(row.status || "")}</td>
      </tr>
    `).join("");
    return `
      <div style="margin-top:16px">
        <div style="display:grid; grid-template-columns:repeat(4, minmax(0, 1fr)); gap:10px">
          <div class="ui-card" style="padding:10px"><div class="ui-muted">Total Return</div><div style="font-weight:700">${escapeHtml(formatSignedPct((metrics.total_return_pct || 0) / 100, 2))}</div></div>
          <div class="ui-card" style="padding:10px"><div class="ui-muted">Alpha vs SPY</div><div style="font-weight:700">${escapeHtml(formatSignedPct((((metrics.benchmark || {}).alpha_pct || 0) / 100), 2))}</div></div>
          <div class="ui-card" style="padding:10px"><div class="ui-muted">Win Rate</div><div style="font-weight:700">${metrics.win_rate == null ? "—" : escapeHtml(formatSignedPct(metrics.win_rate, 1))}</div></div>
          <div class="ui-card" style="padding:10px"><div class="ui-muted">Max Drawdown</div><div style="font-weight:700">${escapeHtml(formatSignedPct(maxDrawdown / 100, 2))}</div></div>
        </div>
        <details style="margin-top:12px" open><summary style="cursor:pointer; font-weight:600">Theme Attribution</summary>${renderAttributionTable(["Theme Name", "P&L", "Positions", "Win Rate", "Best Trade", "Worst Trade"], themeRows, "No theme attribution yet.")}</details>
        <details style="margin-top:12px"><summary style="cursor:pointer; font-weight:600">Source Attribution</summary>${renderAttributionTable(["Source", "Plans Executed", "P&L", "Win Rate", "Avg Slippage"], sourceRows, "No executed source data yet.")}</details>
        <details style="margin-top:12px"><summary style="cursor:pointer; font-weight:600">Regime Attribution</summary>${renderAttributionTable(["Regime", "Positions", "P&L", "Win Rate", "Avg Return %"], regimeRows, "No regime attribution yet.")}</details>
        <details style="margin-top:12px"><summary style="cursor:pointer; font-weight:600">ML Accuracy</summary>
          ${Number(ml.total_trades_with_ml || 0) > 0
            ? `${renderAttributionTable(["Confidence Band", "Trade Count", "Predicted Success", "Actual Success", "Calibration Gap"], calibrationRows, "No ML calibration rows yet.")}
               ${renderAttributionTable(["Version", "Accuracy", "F1", "Ticker", "Status"], historyRows, "No model history yet.")}`
            : '<div class="ui-muted" style="margin-top:10px">ML accuracy tracking will appear after trades with ML confidence are closed.</div>'}
        </details>
      </div>
    `;
  }

  function renderPaperPortfolioSection() {
    const mount = byId("regimePaperPortfolioMount");
    if (!mount) return;
    const portfolios = Array.isArray(state.paperPortfolios) ? state.paperPortfolios : [];
    const current = currentPaperPortfolio();
    const currentStatus = String((current && current.status) || "Active");
    const brokerType = String((current && current.broker_type) || "paper").toLowerCase();
    const brokerStatus = state.paperPortfolioDetail && state.paperPortfolioDetail.broker_status ? state.paperPortfolioDetail.broker_status : null;
    const autonomy = state.autonomySettings || { operating_mode: "manual", auto_approve_threshold: 0.65, daily_capital_ceiling_pct: 0.25 };
    const autonomyStatus = state.autonomyStatus || null;
    const taxSettings = state.taxSettings || { lot_selection_method: "HIFO_LTCG", ltcg_defer_window_days: 30 };
    const marketData = state.marketDataSettings || {
      settings: {
        benchmark_provider_order: ["cache", "ibkr", "stooq", "yahoo"],
        benchmark_enabled: { cache: true, ibkr: true, stooq: true, yahoo: false },
        momentum_provider_order: ["ibkr", "stooq", "finnhub"],
        momentum_enabled: { ibkr: true, stooq: true, finnhub: true },
        regime_provider_order: ["ibkr", "yfinance"],
        regime_enabled: { ibkr: true, yfinance: true },
      },
      ibkr_connected: false,
    };
    const notificationPrefs = state.notificationPreferences || {
      preferences: [],
      settings: {
        quiet_hours_start: "",
        quiet_hours_end: "",
        quiet_hours_tz: "America/New_York",
        digest_enabled: false,
        email_configured: false,
        slack_configured: false,
      },
    };
    const mode = String(autonomy.operating_mode || "manual");
    const modeBadge = mode === "autonomous" ? "ui-badge ui-badge--safe" : mode === "semi_auto" ? "ui-badge ui-badge--warn" : "ui-badge ui-badge--neutral";
    const renderProviderRows = (prefix, order, enabled) => order.map((provider, index) => `
      <div style="display:grid; grid-template-columns:auto 1fr auto auto; gap:8px; align-items:center">
        <input type="checkbox" data-provider-toggle="${prefix}:${provider}" ${enabled[provider] !== false ? "checked" : ""} ${provider === "cache" ? "disabled" : ""} />
        <span>${escapeHtml(provider.toUpperCase())}</span>
        <button class="btn btn--secondary btn--sm" type="button" data-provider-move="${prefix}:${provider}:up" ${index === 0 ? "disabled" : ""}>↑</button>
        <button class="btn btn--secondary btn--sm" type="button" data-provider-move="${prefix}:${provider}:down" ${index === order.length - 1 ? "disabled" : ""}>↓</button>
      </div>
    `).join("");
    mount.innerHTML = `
      <div class="ui-card" style="padding:12px">
        <div class="table-toolbar">
          <div>
            <div class="ui-section-title">Paper Portfolio</div>
            <div class="ui-muted">Create a simulation portfolio, generate plans, then review before execution.</div>
          </div>
          ${current ? `<div style="display:flex; gap:8px; flex-wrap:wrap">
            <span class="${brokerType === "ibkr" ? "ui-badge ui-badge--neutral" : "ui-badge ui-badge--safe"}">${brokerType === "ibkr" ? "IBKR" : "Paper"}</span>
            <span class="${currentStatus === "Active" ? "ui-badge ui-badge--safe" : currentStatus === "Paused" ? "ui-badge ui-badge--neutral" : "ui-badge ui-badge--bad"}">${escapeHtml(currentStatus)}</span>
            ${brokerStatus ? `<span class="${brokerStatus.connection === "connected" ? "ui-badge ui-badge--safe" : "ui-badge ui-badge--bad"}">${escapeHtml(brokerStatus.connection)}</span><span class="ui-badge ui-badge--neutral">${escapeHtml(brokerStatus.market_hours || "closed")}</span>` : ""}
            <button class="btn btn--secondary" type="button" id="regimePaperToggleStatus">${currentStatus === "Paused" ? "Reactivate" : "Pause"}</button>
            <button class="btn btn--secondary" type="button" id="regimePaperKillSwitch">Kill Switch</button>
            <button class="btn btn--secondary" type="button" id="regimePaperDelete">Delete</button>
          </div>` : ""}
        </div>
        <div style="display:grid; gap:10px; margin-top:10px">
          <label>
            Active portfolio
            <select id="regimePaperPortfolioSelect">
              <option value="">Select…</option>
              ${portfolios.map((portfolio) => `<option value="${escapeHtml(portfolio.id)}" ${String(portfolio.id) === String(state.currentPaperPortfolioId || "") ? "selected" : ""}>${escapeHtml(portfolio.name || "")}</option>`).join("")}
            </select>
          </label>
          <form id="regimePaperPortfolioCreate" style="display:grid; grid-template-columns:1fr 160px 180px auto; gap:8px">
            <input type="text" name="name" placeholder="New paper portfolio" />
            <input type="number" name="starting_budget" min="1000" step="1000" placeholder="100000" />
            <select name="broker_type">
              <option value="paper">Paper Trading</option>
              <option value="ibkr">IBKR (Simulated)</option>
            </select>
            <button class="btn btn--secondary" type="submit">Create</button>
          </form>
          ${current ? `
            <div style="border:1px solid ${COLORS.border}; border-radius:10px; padding:10px">
              <div class="table-toolbar">
                <div>
                  <div style="font-weight:600">Autonomy</div>
                  <div class="ui-muted">Manual, semi-auto, and autonomous approval gates.</div>
                </div>
                <span class="${modeBadge}">${escapeHtml(mode)}</span>
              </div>
              <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:10px">
                <button class="btn btn--secondary" type="button" data-autonomy-mode="manual">Manual</button>
                <button class="btn btn--secondary" type="button" data-autonomy-mode="semi_auto">Semi-Auto</button>
                <button class="btn btn--secondary" type="button" data-autonomy-mode="autonomous">Autonomous</button>
              </div>
              <details style="margin-top:10px">
                <summary style="cursor:pointer; font-weight:600">Autonomy Settings</summary>
                <div style="display:grid; grid-template-columns:1fr 1fr auto; gap:8px; margin-top:10px">
                  <label>
                    Auto-approve when ML confidence ≥
                    <input id="regimeAutonomyThreshold" type="number" min="0" max="1" step="0.05" value="${escapeHtml(autonomy.auto_approve_threshold ?? 0.65)}" ${mode !== "semi_auto" ? "disabled" : ""} />
                  </label>
                  <label>
                    Max daily deployment
                    <input id="regimeAutonomyCeiling" type="number" min="0" max="100" step="5" value="${escapeHtml((Number(autonomy.daily_capital_ceiling_pct ?? 0.25) * 100).toFixed(0))}" />
                  </label>
                  <label>
                    VIX freeze threshold
                    <input id="regimeVixFreezeThreshold" type="number" min="10" max="100" step="1" value="${escapeHtml(Number((state.vixStatus || {}).freeze_threshold || 35).toFixed(0))}" />
                  </label>
                  <label>
                    VIX resume threshold
                    <input id="regimeVixResumeThreshold" type="number" min="5" max="100" step="1" value="${escapeHtml(Number((state.vixStatus || {}).resume_threshold || 30).toFixed(0))}" />
                  </label>
                  <label>
                    Lot selection method
                    <select id="regimeLotSelectionMethod">
                      ${["HIFO_LTCG", "HIFO", "FIFO", "LIFO"].map((method) => `<option value="${method}" ${method === String(taxSettings.lot_selection_method || "HIFO_LTCG") ? "selected" : ""}>${method}</option>`).join("")}
                    </select>
                  </label>
                  <label>
                    LTCG deferral window
                    <input id="regimeLtcgDeferWindow" type="number" min="0" max="365" step="1" value="${escapeHtml(Number(taxSettings.ltcg_defer_window_days || 30).toFixed(0))}" />
                  </label>
                  <button class="btn btn--secondary" type="button" id="regimeAutonomySave" style="align-self:end">Save</button>
                </div>
              </details>
              ${autonomyStatus ? `<div class="ui-muted" style="margin-top:10px">Capital deployed today ${escapeHtml(formatCurrency(autonomyStatus.capital_deployed_today, 0))} / ${escapeHtml(formatCurrency(autonomyStatus.max_daily_capital, 0))} · Trades ${escapeHtml(autonomyStatus.trades_today || 0)} · Auto-approved ${escapeHtml(autonomyStatus.auto_approved_today || 0)} · Guardrail blocks ${escapeHtml(autonomyStatus.guardrail_blocks_today || 0)}</div>` : ""}
            </div>
            <div style="border:1px solid ${COLORS.border}; border-radius:10px; padding:10px; margin-top:10px">
              <div class="table-toolbar">
                <div>
                  <div style="font-weight:600">Market Data Providers</div>
                  <div class="ui-muted">Configure benchmark and momentum fallback order.</div>
                </div>
                <span class="${marketData.ibkr_connected ? "ui-badge ui-badge--safe" : "ui-badge ui-badge--neutral"}">IBKR ${marketData.ibkr_connected ? "connected" : "offline"}</span>
              </div>
              <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-top:10px">
                <div>
                  <div style="font-weight:600; margin-bottom:8px">Benchmarks</div>
                  ${renderProviderRows("benchmark", marketData.settings.benchmark_provider_order || ["cache", "ibkr", "stooq", "yahoo"], marketData.settings.benchmark_enabled || {})}
                </div>
                <div>
                  <div style="font-weight:600; margin-bottom:8px">Momentum</div>
                  ${renderProviderRows("momentum", marketData.settings.momentum_provider_order || ["ibkr", "stooq", "finnhub"], marketData.settings.momentum_enabled || {})}
                </div>
                <div>
                  <div style="font-weight:600; margin-bottom:8px">Regime Pipeline</div>
                  ${renderProviderRows("regime", marketData.settings.regime_provider_order || ["ibkr", "yfinance"], marketData.settings.regime_enabled || {})}
                </div>
                <div>
                  <div style="font-weight:600; margin-bottom:8px">Macro Provider Test</div>
                  <div class="ui-muted">Validate VIX / 10Y fetch through the shared IBKR market-data path.</div>
                  <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:10px">
                    <button class="btn btn--secondary" type="button" id="regimeTestMacroData">Test Macro Data</button>
                  </div>
                </div>
              </div>
              <div class="ui-muted" style="margin-top:8px">Benchmark order must keep CACHE first.</div>
              <button class="btn btn--secondary" type="button" id="regimeMarketDataSave" style="margin-top:10px">Save Market Data Settings</button>
            </div>
            <div style="border:1px solid ${COLORS.border}; border-radius:10px; padding:10px; margin-top:10px">
              <div class="table-toolbar">
                <div>
                  <div style="font-weight:600">Notifications</div>
                  <div class="ui-muted">Configure per-alert email/slack routing, quiet hours, and digest mode.</div>
                </div>
                <div style="display:flex; gap:8px; flex-wrap:wrap">
                  <span class="${notificationPrefs.settings.email_configured ? "ui-badge ui-badge--safe" : "ui-badge ui-badge--neutral"}">Email ${notificationPrefs.settings.email_configured ? "ready" : "not configured"}</span>
                  <span class="${notificationPrefs.settings.slack_configured ? "ui-badge ui-badge--safe" : "ui-badge ui-badge--neutral"}">Slack ${notificationPrefs.settings.slack_configured ? "ready" : "not configured"}</span>
                </div>
              </div>
              <div style="display:grid; grid-template-columns:repeat(4, minmax(0, 1fr)); gap:8px; margin-top:10px">
                <label>
                  Quiet start
                  <input id="regimeNotifyQuietStart" type="time" value="${escapeHtml(notificationPrefs.settings.quiet_hours_start || "")}" />
                </label>
                <label>
                  Quiet end
                  <input id="regimeNotifyQuietEnd" type="time" value="${escapeHtml(notificationPrefs.settings.quiet_hours_end || "")}" />
                </label>
                <label>
                  Timezone
                  <input id="regimeNotifyQuietTz" type="text" value="${escapeHtml(notificationPrefs.settings.quiet_hours_tz || "America/New_York")}" />
                </label>
                <label style="display:flex; gap:8px; align-items:center; margin-top:22px">
                  <input id="regimeNotifyDigestEnabled" type="checkbox" ${notificationPrefs.settings.digest_enabled ? "checked" : ""} />
                  <span>Digest non-critical email alerts</span>
                </label>
              </div>
              <div style="margin-top:12px; overflow:auto">
                <table class="report-table">
                  <thead>
                    <tr><th>Alert Type</th><th>In-App</th><th>Email</th><th>Slack</th></tr>
                  </thead>
                  <tbody>
                    ${Array.isArray(notificationPrefs.preferences) && notificationPrefs.preferences.length
                      ? Object.entries((notificationPrefs.preferences || []).reduce((acc, row) => {
                          const key = String(row.alert_type || "");
                          if (!acc[key]) acc[key] = {};
                          acc[key][String(row.channel || "")] = !!row.enabled;
                          return acc;
                        }, {})).map(([alertType, channels]) => `
                          <tr data-notify-alert-type="${escapeHtml(alertType)}">
                            <td>${escapeHtml(alertType)}</td>
                            <td><input type="checkbox" checked disabled /></td>
                            <td><input type="checkbox" data-notify-channel="email" ${channels.email ? "checked" : ""} ${notificationPrefs.settings.email_configured ? "" : "disabled"} /></td>
                            <td><input type="checkbox" data-notify-channel="slack" ${channels.slack ? "checked" : ""} ${notificationPrefs.settings.slack_configured ? "" : "disabled"} /></td>
                          </tr>
                        `).join("")
                      : '<tr><td colspan="4" class="ui-muted">Notification preferences unavailable.</td></tr>'}
                  </tbody>
                </table>
              </div>
              <button class="btn btn--secondary" type="button" id="regimeNotificationSave" style="margin-top:10px">Save Notification Settings</button>
            </div>
          ` : ""}
        </div>
      </div>
    `;
    updateStatusBar();
    const select = byId("regimePaperPortfolioSelect");
    if (select) {
      select.addEventListener("change", async () => {
        state.currentPaperPortfolioId = select.value || null;
        await refreshPaperPortfolio();
      });
    }
    const form = byId("regimePaperPortfolioCreate");
    if (form) {
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        const body = new URLSearchParams();
        const fd = new FormData(form);
        body.set("name", String(fd.get("name") || "").trim());
        if (fd.get("starting_budget")) body.set("starting_budget", String(fd.get("starting_budget")));
        body.set("broker_type", String(fd.get("broker_type") || "paper"));
        try {
          const response = await fetch(state.config.endpoints.paper_portfolios, {
            method: "POST",
            body,
            headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
          });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Create failed (${response.status})`);
          showToast(`Created paper portfolio ${payload.name}.`);
          await loadPaperPortfolios(payload.id);
          form.reset();
        } catch (error) {
          showToast(`Unable to create paper portfolio: ${error.message || error}`, "error");
        }
      });
    }
    const toggle = byId("regimePaperToggleStatus");
    if (toggle && current) {
      toggle.addEventListener("click", async () => {
        await updatePaperPortfolioStatus(currentStatus === "Paused" ? "Active" : "Paused");
      });
    }
    const kill = byId("regimePaperKillSwitch");
    if (kill && current) {
      kill.addEventListener("click", async () => {
        if (!window.confirm("EMERGENCY HALT — This will reject all pending and approved plans and pause the portfolio. Are you sure?")) return;
        try {
          const response = await fetch(paperEndpoint("paper_kill_switch", current.id), { method: "POST", headers: { Accept: "application/json" } });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Kill switch failed (${response.status})`);
          showToast(`Kill switch activated: ${payload.rejected_count || 0} plans rejected, portfolio paused.`);
          await refreshPaperPortfolio();
        } catch (error) {
          showToast(`Unable to activate kill switch: ${error.message || error}`, "error");
        }
      });
    }
    const del = byId("regimePaperDelete");
    if (del && current) {
      del.addEventListener("click", async () => {
        if (!window.confirm(`Delete portfolio '${current.name}'? This will permanently remove all positions, plans, and audit history.`)) return;
        try {
          const response = await fetch(paperEndpoint("paper_portfolio", current.id), { method: "DELETE", headers: { Accept: "application/json" } });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Delete failed (${response.status})`);
          showToast(`Deleted paper portfolio ${current.name}.`);
          state.currentPaperPortfolioId = null;
          await loadPaperPortfolios();
        } catch (error) {
          showToast(`Unable to delete paper portfolio: ${error.message || error}`, "error");
        }
      });
    }
    mount.querySelectorAll("[data-autonomy-mode]").forEach((button) => {
      button.addEventListener("click", async () => {
        const nextMode = String(button.getAttribute("data-autonomy-mode") || "manual");
        if (nextMode === mode) return;
        const detail = nextMode === "autonomous"
          ? "auto-approved and executed"
          : nextMode === "semi_auto"
            ? "auto-approved"
            : "left for manual review";
        if (!window.confirm(`Are you sure? In ${nextMode} mode, trades meeting criteria will be ${detail} without manual review.`)) return;
        await saveAutonomySettings({ operating_mode: nextMode });
      });
    });
    const autonomySave = byId("regimeAutonomySave");
    if (autonomySave) {
      autonomySave.addEventListener("click", async () => {
        const thresholdInput = byId("regimeAutonomyThreshold");
        const ceilingInput = byId("regimeAutonomyCeiling");
        const vixFreezeInput = byId("regimeVixFreezeThreshold");
        const vixResumeInput = byId("regimeVixResumeThreshold");
        const lotMethodInput = byId("regimeLotSelectionMethod");
        const ltcgWindowInput = byId("regimeLtcgDeferWindow");
        await saveAutonomySettings({
          auto_approve_threshold: thresholdInput ? Number(thresholdInput.value || autonomy.auto_approve_threshold || 0.65) : autonomy.auto_approve_threshold,
          daily_capital_ceiling_pct: ceilingInput ? Number(ceilingInput.value || 25) / 100 : autonomy.daily_capital_ceiling_pct,
        });
        if (state.config?.endpoints?.vix_settings) {
          try {
            const response = await fetch(state.config.endpoints.vix_settings, {
              method: "PUT",
              headers: { Accept: "application/json", "Content-Type": "application/json" },
              body: JSON.stringify({
                freeze_threshold: vixFreezeInput ? Number(vixFreezeInput.value || 35) : 35,
                resume_threshold: vixResumeInput ? Number(vixResumeInput.value || 30) : 30,
              }),
            });
            const payload = await response.json();
            if (!response.ok) throw new Error(payload.detail || `VIX settings failed (${response.status})`);
            state.vixStatus = { ...(state.vixStatus || {}), ...payload, frozen: !!(state.vixStatus || {}).frozen, vix: (state.vixStatus || {}).vix };
            renderVixStatus();
          } catch (error) {
            showToast(`Unable to save VIX settings: ${error.message || error}`, "error");
          }
        }
        if (state.config?.endpoints?.tax_settings) {
          try {
            const response = await fetch(state.config.endpoints.tax_settings, {
              method: "PUT",
              headers: { Accept: "application/json", "Content-Type": "application/json" },
              body: JSON.stringify({
                lot_selection_method: lotMethodInput ? String(lotMethodInput.value || "HIFO_LTCG") : "HIFO_LTCG",
                ltcg_defer_window_days: ltcgWindowInput ? Number(ltcgWindowInput.value || 30) : 30,
              }),
            });
            const payload = await response.json();
            if (!response.ok) throw new Error(payload.detail || `Tax settings failed (${response.status})`);
            state.taxSettings = payload;
          } catch (error) {
            showToast(`Unable to save tax settings: ${error.message || error}`, "error");
          }
        }
      });
    }
    mount.querySelectorAll("[data-provider-move]").forEach((button) => {
      button.addEventListener("click", () => {
        const [scope, provider, direction] = String(button.getAttribute("data-provider-move") || "").split(":");
        const settings = JSON.parse(JSON.stringify((state.marketDataSettings && state.marketDataSettings.settings) || marketData.settings));
        const key = scope === "benchmark" ? "benchmark_provider_order" : scope === "momentum" ? "momentum_provider_order" : "regime_provider_order";
        const order = Array.isArray(settings[key]) ? settings[key].slice() : [];
        const index = order.indexOf(provider);
        if (index < 0) return;
        const next = direction === "up" ? index - 1 : index + 1;
        if (next < 0 || next >= order.length) return;
        [order[index], order[next]] = [order[next], order[index]];
        settings[key] = order;
        state.marketDataSettings = { ...(state.marketDataSettings || marketData), settings };
        renderPaperPortfolioSection();
      });
    });
    mount.querySelectorAll("[data-provider-toggle]").forEach((checkbox) => {
      checkbox.addEventListener("change", () => {
        const [scope, provider] = String(checkbox.getAttribute("data-provider-toggle") || "").split(":");
        const settings = JSON.parse(JSON.stringify((state.marketDataSettings && state.marketDataSettings.settings) || marketData.settings));
        const key = scope === "benchmark" ? "benchmark_enabled" : scope === "momentum" ? "momentum_enabled" : "regime_enabled";
        settings[key] = { ...(settings[key] || {}), [provider]: checkbox.checked };
        if (provider === "cache") settings[key][provider] = true;
        state.marketDataSettings = { ...(state.marketDataSettings || marketData), settings };
      });
    });
    const marketDataSave = byId("regimeMarketDataSave");
    if (marketDataSave && state.config?.endpoints?.market_data_settings) {
      marketDataSave.addEventListener("click", async () => {
        try {
          const response = await fetch(state.config.endpoints.market_data_settings, {
            method: "PUT",
            headers: { Accept: "application/json", "Content-Type": "application/json" },
            body: JSON.stringify((state.marketDataSettings && state.marketDataSettings.settings) || marketData.settings),
          });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Market data settings failed (${response.status})`);
          state.marketDataSettings = { ...(state.marketDataSettings || {}), settings: payload.settings };
          showToast("Saved market data settings.");
          renderPaperPortfolioSection();
        } catch (error) {
          showToast(`Unable to save market data settings: ${error.message || error}`, "error");
        }
      });
    }
    const macroTester = async () => {
      if (!state.config?.endpoints?.market_data_test_macro) return;
      try {
        const response = await fetch(state.config.endpoints.market_data_test_macro, { headers: { Accept: "application/json" } });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.detail || `Macro test failed (${response.status})`);
        const formatEntry = (label, entry) => {
          const ibkr = entry?.ibkr;
          const yf = entry?.yfinance;
          const active = ibkr?.available || yf?.available;
          const source = ibkr?.available ? "IBKR" : yf?.available ? "yfinance" : "offline";
          const value = ibkr?.available ? ibkr.value : yf?.available ? yf.value : null;
          return `${label}: ${active ? `${source} ${value}` : "unavailable"}`;
        };
        const summary = [
          formatEntry("VIX", payload.vix),
          formatEntry("10Y", payload.yield_10y),
        ].join(" · ");
        const ok = Boolean(payload.vix?.ibkr?.available || payload.vix?.yfinance?.available || payload.yield_10y?.ibkr?.available || payload.yield_10y?.yfinance?.available);
        showToast(summary, ok ? "success" : "warning");
      } catch (error) {
        showToast(`Unable to test macro data: ${error.message || error}`, "error");
      }
    };
    const testMacro = byId("regimeTestMacroData");
    if (testMacro) testMacro.addEventListener("click", () => macroTester());
    const notificationSave = byId("regimeNotificationSave");
    if (notificationSave && state.config?.endpoints?.notification_preferences) {
      notificationSave.addEventListener("click", async () => {
        try {
          const preferences = [];
          mount.querySelectorAll("[data-notify-alert-type]").forEach((row) => {
            const alertType = String(row.getAttribute("data-notify-alert-type") || "");
            preferences.push({ alert_type: alertType, channel: "email", enabled: !!row.querySelector('[data-notify-channel="email"]')?.checked });
            preferences.push({ alert_type: alertType, channel: "slack", enabled: !!row.querySelector('[data-notify-channel="slack"]')?.checked });
          });
          const response = await fetch(state.config.endpoints.notification_preferences, {
            method: "PUT",
            headers: { Accept: "application/json", "Content-Type": "application/json" },
            body: JSON.stringify({
              preferences,
              settings: {
                quiet_hours_start: byId("regimeNotifyQuietStart")?.value || "",
                quiet_hours_end: byId("regimeNotifyQuietEnd")?.value || "",
                quiet_hours_tz: byId("regimeNotifyQuietTz")?.value || "America/New_York",
                digest_enabled: !!byId("regimeNotifyDigestEnabled")?.checked,
              },
            }),
          });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Notification settings failed (${response.status})`);
          state.notificationPreferences = payload;
          showToast("Saved notification settings.");
          renderPaperPortfolioSection();
        } catch (error) {
          showToast(`Unable to save notification settings: ${error.message || error}`, "error");
        }
      });
    }
  }

  async function loadPaperPortfolios(preferredId = null) {
    if (!state.config || !state.config.endpoints || !state.config.endpoints.paper_portfolios) return;
    try {
      const response = await fetch(state.config.endpoints.paper_portfolios, { headers: { Accept: "application/json" } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Unable to load paper portfolios (${response.status})`);
      state.paperPortfolios = Array.isArray(payload.portfolios) ? payload.portfolios : [];
      if (preferredId != null) state.currentPaperPortfolioId = String(preferredId);
      if (!state.currentPaperPortfolioId && state.paperPortfolios.length) state.currentPaperPortfolioId = String(state.paperPortfolios[0].id);
      renderPaperPortfolioSection();
      await refreshPaperPortfolio();
    } catch (error) {
      showToast(`Unable to load paper portfolios: ${error.message || error}`, "error");
    }
  }

  async function refreshPaperPortfolio() {
    const portfolioId = currentPaperPortfolioId();
    renderPaperPortfolioSection();
    if (state.paperOrderPollTimer) {
      window.clearInterval(state.paperOrderPollTimer);
      state.paperOrderPollTimer = null;
    }
    if (!portfolioId) {
      state.paperPortfolioDetail = null;
      state.autonomyStatus = null;
      state.paperBudget = null;
      state.paperPlans = [];
      state.paperPositions = [];
      state.paperTaxLots = [];
      state.paperWashSale = [];
      state.paperTaxEstimates = {};
      state.paperPerformance = null;
      state.paperAttribution = null;
      state.paperAudit = [];
      state.paperAuditSummary = null;
      state.paperMonitoring = null;
      state.unacknowledgedAlerts = [];
      state.alertHistory = [];
      state.vixStatus = null;
      state.paperPrecheck = {};
      renderPaperBudget();
      renderPaperPlans();
      renderPaperPositions();
      renderPaperPerformance();
      renderAuditTrail();
      renderAlertToasts();
      renderAlertHistory();
      renderVixStatus();
      renderMonitoringDashboard(null);
      stopMonitoringPolling();
      return;
    }
    try {
      const [detailResponse, autonomySettingsResponse, autonomyStatusResponse, marketDataSettingsResponse, notificationPreferencesResponse, budgetResponse, plansResponse, positionsResponse, taxLotsResponse, washSaleResponse, performanceResponse, auditResponse, precheckResponse, monitoringResponse, healthResponse, validationResponse, alertsResponse, alertHistoryResponse, vixResponse] = await Promise.all([
        fetch(paperEndpoint("paper_portfolio", portfolioId), { headers: { Accept: "application/json" } }),
        fetch(state.config.endpoints.autonomy_settings, { headers: { Accept: "application/json" } }),
        fetch(paperEndpoint("paper_autonomy_status", portfolioId), { headers: { Accept: "application/json" } }),
        fetch(state.config.endpoints.market_data_settings, { headers: { Accept: "application/json" } }),
        fetch(state.config.endpoints.notification_preferences, { headers: { Accept: "application/json" } }),
        fetch(paperEndpoint("paper_budget", portfolioId), { headers: { Accept: "application/json" } }),
        fetch(`${paperEndpoint("paper_plans", portfolioId)}?status=all`, { headers: { Accept: "application/json" } }),
        fetch(`${paperEndpoint("paper_positions", portfolioId)}?status=Open`, { headers: { Accept: "application/json" } }),
        fetch(`${paperEndpoint("paper_tax_lots", portfolioId)}?status=all`, { headers: { Accept: "application/json" } }),
        fetch(paperEndpoint("paper_wash_sale", portfolioId), { headers: { Accept: "application/json" } }),
        fetch(paperEndpoint("paper_performance", portfolioId), { headers: { Accept: "application/json" } }),
        fetch(paperEndpoint("paper_audit", portfolioId), { headers: { Accept: "application/json" } }),
        fetch(paperEndpoint("paper_precheck", portfolioId), { method: "POST", headers: { Accept: "application/json" } }),
        fetch(paperEndpoint("paper_monitoring", portfolioId), { headers: { Accept: "application/json" } }),
        fetch(state.config.endpoints.health, { headers: { Accept: "application/json" } }),
        fetch(state.config.endpoints.data_validation, { headers: { Accept: "application/json" } }),
        fetch(`${state.config.endpoints.alerts}?unacknowledged=true&limit=5`, { headers: { Accept: "application/json" } }),
        fetch(`${state.config.endpoints.alerts}?limit=25`, { headers: { Accept: "application/json" } }),
        fetch(state.config.endpoints.vix_status, { headers: { Accept: "application/json" } }),
      ]);
      const parseJson = async (response) => {
        try {
          return await response.json();
        } catch (_error) {
          return {};
        }
      };
      const [detail, autonomySettings, autonomyStatus, marketDataSettings, notificationPreferences, budget, plans, positions, taxLots, washSale, performance, audit, precheck, monitoring, health, validation, alerts, alertHistory, vixStatus] = await Promise.all([
        parseJson(detailResponse),
        parseJson(autonomySettingsResponse),
        parseJson(autonomyStatusResponse),
        parseJson(marketDataSettingsResponse),
        parseJson(notificationPreferencesResponse),
        parseJson(budgetResponse),
        parseJson(plansResponse),
        parseJson(positionsResponse),
        parseJson(taxLotsResponse),
        parseJson(washSaleResponse),
        parseJson(performanceResponse),
        parseJson(auditResponse),
        parseJson(precheckResponse),
        parseJson(monitoringResponse),
        parseJson(healthResponse),
        parseJson(validationResponse),
        parseJson(alertsResponse),
        parseJson(alertHistoryResponse),
        parseJson(vixResponse),
      ]);
      const warnPanel = (label, response, payload) => {
        const message = payload && (payload.detail || payload.error) ? payload.detail || payload.error : `${label} failed (${response.status})`;
        console.warn(`Paper trading ${label.toLowerCase()} unavailable:`, message);
      };
      if (!detailResponse.ok) throw new Error(detail.detail || `Portfolio failed (${detailResponse.status})`);
      state.paperPortfolioDetail = detail;
      if (autonomySettingsResponse.ok) {
        state.autonomySettings = autonomySettings;
        state.taxSettings = {
          lot_selection_method: autonomySettings.lot_selection_method || "HIFO_LTCG",
          ltcg_defer_window_days: autonomySettings.ltcg_defer_window_days || 30,
        };
      } else {
        warnPanel("Autonomy settings", autonomySettingsResponse, autonomySettings);
      }
      if (autonomyStatusResponse.ok) {
        state.autonomyStatus = autonomyStatus;
      } else {
        warnPanel("Autonomy status", autonomyStatusResponse, autonomyStatus);
        state.autonomyStatus = null;
      }
      if (marketDataSettingsResponse.ok) {
        state.marketDataSettings = marketDataSettings;
      } else {
        warnPanel("Market data settings", marketDataSettingsResponse, marketDataSettings);
      }
      if (notificationPreferencesResponse.ok) {
        state.notificationPreferences = notificationPreferences;
      } else {
        warnPanel("Notification settings", notificationPreferencesResponse, notificationPreferences);
      }
      if (budgetResponse.ok) {
        state.paperBudget = budget;
      } else {
        warnPanel("Budget", budgetResponse, budget);
        state.paperBudget = null;
      }
      if (plansResponse.ok) {
        state.paperPlans = Array.isArray(plans.plans) ? plans.plans : [];
      } else {
        warnPanel("Plans", plansResponse, plans);
        state.paperPlans = [];
      }
      const resultMap = new Map();
      const execResults = state.paperExecutionResults || {};
      (execResults.executed || []).forEach((row) => resultMap.set(String(row.plan_id), `Executed at ${formatCurrency(row.execution_price, 2)}`));
      (execResults.skipped || []).forEach((row) => resultMap.set(String(row.plan_id), row.reason || row.status || "Skipped"));
      state.paperPlans = state.paperPlans.map((plan) => ({ ...plan, execution_result: resultMap.get(String(plan.id)) || null }));
      if (positionsResponse.ok) {
        state.paperPositions = Array.isArray(positions.positions) ? positions.positions : [];
      } else {
        warnPanel("Positions", positionsResponse, positions);
        state.paperPositions = [];
      }
      if (taxLotsResponse.ok) {
        state.paperTaxLots = Array.isArray(taxLots.lots) ? taxLots.lots : [];
      } else {
        warnPanel("Tax lots", taxLotsResponse, taxLots);
        state.paperTaxLots = [];
      }
      if (washSaleResponse.ok) {
        state.paperWashSale = Array.isArray(washSale.restrictions) ? washSale.restrictions : [];
      } else {
        warnPanel("Wash sale", washSaleResponse, washSale);
        state.paperWashSale = [];
      }
      if (performanceResponse.ok) {
        state.paperPerformance = performance;
      } else {
        warnPanel("Performance", performanceResponse, performance);
        state.paperPerformance = null;
        state.paperAttribution = null;
      }
      if (auditResponse.ok) {
        state.paperAudit = Array.isArray(audit.audit) ? audit.audit : [];
        state.paperAuditSummary = audit.summary || null;
      } else {
        warnPanel("Audit", auditResponse, audit);
        state.paperAudit = [];
        state.paperAuditSummary = null;
      }
      if (monitoringResponse.ok) {
        state.paperMonitoring = monitoring;
      } else {
        warnPanel("Monitoring", monitoringResponse, monitoring);
        state.paperMonitoring = null;
      }
      state.systemHealth = healthResponse.ok ? health : null;
      state.dataValidation = validationResponse.ok ? validation : null;
      if (precheckResponse.ok) {
        state.paperPrecheck = Object.fromEntries((Array.isArray(precheck.plans) ? precheck.plans : []).map((row) => [String(row.plan_id), row]));
      } else {
        warnPanel("Precheck", precheckResponse, precheck);
        state.paperPrecheck = {};
      }
      if (alertsResponse.ok) {
        state.unacknowledgedAlerts = Array.isArray(alerts.alerts) ? alerts.alerts : [];
      } else {
        warnPanel("Alerts", alertsResponse, alerts);
        state.unacknowledgedAlerts = [];
      }
      if (alertHistoryResponse.ok) {
        state.alertHistory = Array.isArray(alertHistory.alerts) ? alertHistory.alerts : [];
      } else {
        warnPanel("Alert history", alertHistoryResponse, alertHistory);
        state.alertHistory = [];
      }
      if (vixResponse.ok) {
        state.vixStatus = vixStatus;
      } else {
        warnPanel("VIX", vixResponse, vixStatus);
        state.vixStatus = null;
      }
      const sellPlans = state.paperPlans.filter((plan) => String(plan.action || "") === "Sell" && Number(plan.quantity || 0) > 0);
      const estimateEntries = await Promise.all(sellPlans.map(async (plan) => {
        try {
          const response = await fetch(paperEndpoint("paper_tax_estimate", portfolioId), {
            method: "POST",
            headers: { Accept: "application/json", "Content-Type": "application/json" },
            body: JSON.stringify({
              ticker: plan.ticker,
              quantity: Number(plan.quantity || 0),
              exit_price: Number(plan.proposed_price || 0),
            }),
          });
          const payload = await parseJson(response);
          return [String(plan.id), response.ok ? payload : null];
        } catch (_error) {
          return [String(plan.id), null];
        }
      }));
      state.paperTaxEstimates = Object.fromEntries(estimateEntries);
      renderPaperPortfolioSection();
      renderPaperBudget();
      renderPaperPlans();
      renderPaperPositions();
      renderPaperPerformance();
      renderAuditTrail();
      renderAlertToasts();
      renderAlertHistory();
      renderVixStatus();
      renderMonitoringDashboard(monitoring);
      maybeStartPaperOrderPolling();
      startMonitoringPolling();
    } catch (error) {
      showToast(`Unable to refresh paper trading data: ${error.message || error}`, "error");
    }
  }

  async function loadPaperAttribution() {
    const portfolioId = currentPaperPortfolioId();
    if (!portfolioId) return;
    try {
      const response = await fetch(paperEndpoint("paper_attribution_summary", portfolioId), { headers: { Accept: "application/json" } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || payload.error || `Attribution failed (${response.status})`);
      state.paperAttribution = payload;
      renderPaperPerformance();
    } catch (error) {
      showToast(`Unable to load performance report: ${error.message || error}`, "error");
    }
  }

  function maybeStartPaperOrderPolling() {
    const portfolio = currentPaperPortfolio();
    if (!portfolio || String(portfolio.broker_type || "paper").toLowerCase() !== "ibkr") return;
    const hasPending = (state.paperPlans || []).some((plan) => ["Submitted", "Partially Filled"].includes(String(plan.status || "")));
    if (!hasPending) return;
    const portfolioId = currentPaperPortfolioId();
    state.paperOrderPollTimer = window.setInterval(async () => {
      try {
        const response = await fetch(paperEndpoint("paper_pending_orders", portfolioId), { headers: { Accept: "application/json" } });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.detail || `Pending order poll failed (${response.status})`);
        if (Array.isArray(payload.orders)) {
          const byId = new Map(payload.orders.map((row) => [String(row.id), row]));
          state.paperPlans = (state.paperPlans || []).map((plan) => byId.get(String(plan.id)) || plan);
          renderPaperPlans();
          if (!(payload.orders || []).some((plan) => ["Submitted", "Partially Filled"].includes(String(plan.status || "")))) {
            window.clearInterval(state.paperOrderPollTimer);
            state.paperOrderPollTimer = null;
            await refreshPaperPortfolio();
          }
        }
      } catch (error) {
        window.clearInterval(state.paperOrderPollTimer);
        state.paperOrderPollTimer = null;
        showToast(`Unable to poll IBKR order status: ${error.message || error}`, "error");
      }
    }, 5000);
  }

  async function generatePaperPlans() {
    const portfolioId = currentPaperPortfolioId();
    if (!portfolioId) return;
    try {
      const response = await fetch(paperEndpoint("paper_generate", portfolioId), { method: "POST", headers: { Accept: "application/json" } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Generate failed (${response.status})`);
      showToast(`Generated ${Number(payload.created_count || 0)} trade plans.`);
      await refreshPaperPortfolio();
    } catch (error) {
      showToast(`Unable to generate plans: ${error.message || error}`, "error");
    }
  }

  async function saveAutonomySettings(payload) {
    if (!state.config || !state.config.endpoints || !state.config.endpoints.autonomy_settings) return;
    try {
      const response = await fetch(state.config.endpoints.autonomy_settings, {
        method: "PUT",
        headers: { Accept: "application/json", "Content-Type": "application/json" },
        body: JSON.stringify(payload || {}),
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || data.error || `Autonomy settings failed (${response.status})`);
      state.autonomySettings = data;
      showToast("Autonomy settings saved.", "success");
      renderPaperPortfolioSection();
      await refreshPaperPortfolio();
    } catch (error) {
      showToast(`Unable to save autonomy settings: ${error.message || error}`, "error");
    }
  }

  async function runAutoApprove() {
    const portfolioId = currentPaperPortfolioId();
    if (!portfolioId) return;
    try {
      const response = await fetch(paperEndpoint("paper_auto_approve", portfolioId), {
        method: "POST",
        headers: { Accept: "application/json" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || payload.error || `Auto-approve failed (${response.status})`);
      showToast(`Auto-approved ${Number(payload.approved || 0)} plan(s).`);
      await refreshPaperPortfolio();
    } catch (error) {
      showToast(`Unable to run auto-approve: ${error.message || error}`, "error");
    }
  }

  async function updatePaperPlan(planId, status, quantity = null) {
    const portfolioId = currentPaperPortfolioId();
    if (!portfolioId) return;
    const body = new URLSearchParams();
    body.set("status", status);
    if (quantity != null) body.set("quantity", quantity);
    try {
      const response = await fetch(paperEndpoint("paper_plan", portfolioId, planId), {
        method: "PUT",
        body,
        headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Plan update failed (${response.status})`);
      showToast(`Plan ${planId} updated to ${status}.`);
      await refreshPaperPortfolio();
    } catch (error) {
      showToast(`Unable to update trade plan: ${error.message || error}`, "error");
    }
  }

  async function cancelPaperOrder(portfolioId, planId) {
    if (!portfolioId || !planId) return;
    if (!window.confirm("Cancel this order?")) return;
    try {
      const response = await fetch(paperEndpoint("paper_cancel_order", portfolioId, planId), {
        method: "POST",
        headers: { Accept: "application/json" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Cancel failed (${response.status})`);
      if (payload.cancelled) {
        showToast("Order cancelled", "success");
        await refreshPaperPortfolio();
      } else {
        showToast("Cancel failed", "error");
      }
    } catch (error) {
      showToast(`Cancel error: ${error.message || error}`, "error");
    }
  }

  async function executePaperPlans() {
    const portfolioId = currentPaperPortfolioId();
    if (!portfolioId) return;
    const approvedPlans = (state.paperPlans || []).filter((plan) => String(plan.status || "") === "Approved");
    const approved = approvedPlans.length;
    if (!approved) return;
    const totalValue = approvedPlans.reduce((sum, plan) => sum + (Number(plan.quantity || 0) * Number(plan.proposed_price || 0)), 0);
    const blocked = approvedPlans.filter((plan) => {
      const result = planPrecheck(plan.id);
      return result && !result.guardrail_passed;
    }).length;
    const lines = approvedPlans.map((plan) => `${plan.ticker} ${plan.action} ${plan.quantity} @ ${formatCurrency(plan.proposed_price, 2)}`).join("\n");
    if (!window.confirm(`Execute ${approved} approved plan(s) for ${formatCurrency(totalValue, 0)} total value?\n${blocked ? `${blocked} currently fail precheck.\n` : ""}${lines}`)) return;
    try {
      const response = await fetch(paperEndpoint("paper_execute", portfolioId), { method: "POST", headers: { Accept: "application/json" } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Execution failed (${response.status})`);
      state.paperExecutionResults = payload;
      showToast(`Executed ${(payload.executed || []).length} trade plan(s).`);
      await refreshPaperPortfolio();
    } catch (error) {
      showToast(`Unable to execute approved plans: ${error.message || error}`, "error");
    }
  }

  async function updatePaperPortfolioStatus(status) {
    const portfolioId = currentPaperPortfolioId();
    if (!portfolioId) return;
    const body = new URLSearchParams();
    body.set("status", status);
    try {
      const response = await fetch(paperEndpoint("paper_portfolio", portfolioId), {
        method: "PUT",
        body,
        headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Status update failed (${response.status})`);
      showToast(`Portfolio status updated to ${status}.`);
      await loadPaperPortfolios(portfolioId);
    } catch (error) {
      showToast(`Unable to update portfolio status: ${error.message || error}`, "error");
    }
  }

  async function loadAuditTrail() {
    const portfolioId = currentPaperPortfolioId();
    if (!portfolioId) return;
    const eventType = byId("regimeAuditEventFilter")?.value || "";
    const ticker = byId("regimeAuditTickerFilter")?.value || "";
    const days = byId("regimeAuditDaysFilter")?.value || "30";
    const params = new URLSearchParams();
    if (eventType) params.set("event_type", eventType);
    if (ticker) params.set("ticker", ticker.trim().toUpperCase());
    params.set("days", days);
    try {
      const response = await fetch(`${paperEndpoint("paper_audit", portfolioId)}?${params.toString()}`, { headers: { Accept: "application/json" } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Audit fetch failed (${response.status})`);
      state.paperAudit = Array.isArray(payload.audit) ? payload.audit : [];
      state.paperAuditSummary = payload.summary || state.paperAuditSummary;
      renderAuditTrail();
    } catch (error) {
      showToast(`Unable to load audit trail: ${error.message || error}`, "error");
    }
  }

  async function pollDiscoveryJob(jobId) {
    const button = byId("regimeDiscoveryScanBtn");
    const poll = window.setInterval(async () => {
      try {
        const response = await fetch(state.config.endpoints.discovery_scan_status.replace("__JOB_ID__", encodeURIComponent(jobId)), { headers: { Accept: "application/json" } });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.detail || `Discovery status failed (${response.status})`);
        state.discoveryJob = payload;
        if (button) {
          button.disabled = true;
          button.textContent = payload.status === "done"
            ? "Run Discovery Scan"
            : `Scanning ${payload.progress || 0}/${payload.total || 0}${payload.current_theme ? ` · ${payload.current_theme}` : ""}`;
        }
        if (payload.status === "done") {
          window.clearInterval(poll);
          if (button) {
            button.disabled = false;
            button.textContent = "Run Discovery Scan";
          }
          await loadWatchlist();
          await loadThemes();
        } else if (payload.status === "error") {
          window.clearInterval(poll);
          if (button) {
            button.disabled = false;
            button.textContent = "Run Discovery Scan";
          }
        }
      } catch (error) {
        window.clearInterval(poll);
        if (button) {
          button.disabled = false;
          button.textContent = "Run Discovery Scan";
        }
      }
    }, 2000);
  }

  async function startDiscoveryScan(themeIds = null, regenerateSupplyChain = false) {
    const body = new URLSearchParams();
    body.set("frontier_provider", currentFrontierProvider());
    if (Array.isArray(themeIds) && themeIds.length) body.set("theme_ids", themeIds.join(","));
    if (regenerateSupplyChain) body.set("regenerate_supply_chain", "true");
    const response = await fetch(state.config.endpoints.discovery_scan, {
      method: "POST",
      body,
      headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `Discovery scan failed (${response.status})`);
    state.discoveryJob = payload;
    pollDiscoveryJob(payload.job_id);
  }

  function renderThemes() {
    const mount = byId("regimeThemeList");
    const badge = byId("regimeThemeCount");
    if (!mount) return;
    const themes = Array.isArray(state.themes) ? [...state.themes] : [];
    if (badge) badge.textContent = String(themes.length);
    if (!themes.length) {
      mount.innerHTML = '<div class="ui-muted">No themes defined.</div>';
      return;
    }
    mount.innerHTML = themes.map((theme) => `
      <details class="ui-card" style="padding:10px" open>
        <summary>
          <div class="table-toolbar">
            <div>
              <div class="ui-section-title">${escapeHtml(theme.name || "")}</div>
              <div class="ui-muted">Conviction ${escapeHtml(theme.conviction || 0)}/5 · ${escapeHtml(theme.status || "Active")}</div>
            </div>
            <button class="btn btn--secondary" type="button" data-regime-theme-delete="${escapeHtml(theme.id)}">Delete</button>
          </div>
        </summary>
        <div class="ui-muted" style="margin-top:8px; white-space:pre-wrap">${escapeHtml(theme.narrative || "")}</div>
        ${theme.sector_hint ? `<div class="ui-muted" style="margin-top:4px">Sector hint: ${escapeHtml(theme.sector_hint)}</div>` : ""}
        <div style="display:grid; gap:6px; margin-top:10px">
          ${(Array.isArray(theme.tickers) ? theme.tickers : []).map((item) => `
            <div style="border:1px solid ${COLORS.border}; border-radius:10px; padding:8px">
              <div class="table-toolbar">
                <div>
                  <div style="font-weight:600">${escapeHtml(item.ticker || "")}</div>
                  <div class="ui-muted">${escapeHtml(item.role || "Core")} · ${escapeHtml(item.time_horizon || "strategic")}</div>
                </div>
                <button class="btn btn--secondary" type="button" data-regime-theme-ticker-delete="${escapeHtml(theme.id)}" data-regime-theme-ticker="${escapeHtml(item.ticker || "")}">Remove</button>
              </div>
              ${item.rationale ? `<div class="ui-muted" style="margin-top:4px">${escapeHtml(item.rationale)}</div>` : ""}
            </div>
          `).join("")}
        </div>
        <details style="margin-top:10px">
          <summary class="ui-muted" style="cursor:pointer">Supply Chain Map</summary>
          <div data-theme-supply-chain="${escapeHtml(theme.id)}" style="margin-top:8px">
            <button class="btn btn--secondary" type="button" data-generate-supply-chain="${escapeHtml(theme.id)}">Generate / Refresh</button>
            <div data-supply-chain-list="${escapeHtml(theme.id)}" style="margin-top:8px">${(Array.isArray(theme.supply_chain) && theme.supply_chain.length) ? "" : '<div class="ui-muted">No supply-chain map yet.</div>'}</div>
          </div>
        </details>
        <form data-regime-theme-ticker-form="${escapeHtml(theme.id)}" style="display:grid; gap:8px; margin-top:10px">
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px">
            <input type="text" placeholder="Ticker" maxlength="12" data-regime-theme-ticker-input="${escapeHtml(theme.id)}" />
            <select data-regime-theme-role="${escapeHtml(theme.id)}">
              <option value="Core">Core</option>
              <option value="Critical-Path">Critical-Path</option>
              <option value="Speculative">Speculative</option>
            </select>
          </div>
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px">
            <select data-regime-theme-horizon="${escapeHtml(theme.id)}">
              <option value="trade">trade</option>
              <option value="tactical">tactical</option>
              <option value="strategic" selected>strategic</option>
            </select>
            <input type="text" placeholder="Entry / Target / Stop" data-regime-theme-prices="${escapeHtml(theme.id)}" />
          </div>
          <textarea rows="2" maxlength="2000" placeholder="Ticker rationale" data-regime-theme-rationale="${escapeHtml(theme.id)}"></textarea>
          <button class="btn btn--secondary" type="submit">Add Ticker</button>
        </form>
      </details>
    `).join("");
    mount.querySelectorAll("[data-regime-theme-delete]").forEach((button) => {
      button.addEventListener("click", async () => {
        const themeId = String(button.getAttribute("data-regime-theme-delete") || "");
        try {
          const response = await fetch(state.config.endpoints.theme.replace("__THEME_ID__", encodeURIComponent(themeId)), { method: "DELETE", headers: { Accept: "application/json" } });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Delete failed (${response.status})`);
          state.themes = state.themes.filter((item) => String(item.id) !== themeId);
          renderThemes();
        } catch (error) {
          setThemeMessage(`Unable to delete theme: ${error.message || error}`, true);
        }
      });
    });
    mount.querySelectorAll("[data-regime-theme-ticker-delete]").forEach((button) => {
      button.addEventListener("click", async () => {
        const themeId = String(button.getAttribute("data-regime-theme-ticker-delete") || "");
        const ticker = String(button.getAttribute("data-regime-theme-ticker") || "");
        try {
          const response = await fetch(`${state.config.endpoints.theme_tickers.replace("__THEME_ID__", encodeURIComponent(themeId))}/${encodeURIComponent(ticker)}`, { method: "DELETE", headers: { Accept: "application/json" } });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Remove failed (${response.status})`);
          await loadThemes();
        } catch (error) {
          setThemeMessage(`Unable to remove ticker: ${error.message || error}`, true);
        }
      });
    });
    mount.querySelectorAll("[data-regime-theme-ticker-form]").forEach((form) => {
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        const themeId = String(form.getAttribute("data-regime-theme-ticker-form") || "");
        const ticker = String((form.querySelector(`[data-regime-theme-ticker-input="${CSS.escape(themeId)}"]`) || {}).value || "").trim().toUpperCase();
        const role = String((form.querySelector(`[data-regime-theme-role="${CSS.escape(themeId)}"]`) || {}).value || "Core");
        const timeHorizon = String((form.querySelector(`[data-regime-theme-horizon="${CSS.escape(themeId)}"]`) || {}).value || "strategic");
        const prices = String((form.querySelector(`[data-regime-theme-prices="${CSS.escape(themeId)}"]`) || {}).value || "").split("/").map((item) => item.trim());
        const rationale = String((form.querySelector(`[data-regime-theme-rationale="${CSS.escape(themeId)}"]`) || {}).value || "");
        const body = new URLSearchParams();
        body.set("ticker", ticker);
        body.set("role", role);
        body.set("time_horizon", timeHorizon);
        if (prices[0]) body.set("entry_price", prices[0]);
        if (prices[1]) body.set("target_price", prices[1]);
        if (prices[2]) body.set("stop_price", prices[2]);
        if (rationale) body.set("rationale", rationale);
        try {
          const response = await fetch(state.config.endpoints.theme_tickers.replace("__THEME_ID__", encodeURIComponent(themeId)), {
            method: "POST",
            body,
            headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
          });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.detail || `Add ticker failed (${response.status})`);
          await loadThemes();
          setThemeMessage(`Saved ${ticker} to theme.`, false);
        } catch (error) {
          setThemeMessage(`Unable to save theme ticker: ${error.message || error}`, true);
        }
      });
    });
    mount.querySelectorAll("[data-generate-supply-chain]").forEach((button) => {
      const themeId = String(button.getAttribute("data-generate-supply-chain") || "");
      button.addEventListener("click", async () => {
        try {
          await generateSupplyChain(themeId);
          const response = await fetch(state.config.endpoints.theme.replace("__THEME_ID__", encodeURIComponent(themeId)), { headers: { Accept: "application/json" } });
          const payload = await response.json();
          if (response.ok) {
            const match = state.themes.find((item) => String(item.id) === themeId);
            if (match) match.supply_chain = payload.supply_chain || [];
          }
        } catch (error) {
          setThemeMessage(`Unable to generate supply chain: ${error.message || error}`, true);
        }
      });
    });
    themes.forEach((theme) => renderSupplyChain(theme.id, theme.supply_chain || []));
  }

  async function loadThemes() {
    try {
      const response = await fetch(state.config.endpoints.themes, { headers: { Accept: "application/json" } });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Unable to load themes (${response.status})`);
      state.themes = Array.isArray(payload.themes) ? payload.themes : [];
      await Promise.all(state.themes.map(async (theme) => {
        try {
          const response = await fetch(state.config.endpoints.supply_chain.replace("__THEME_ID__", encodeURIComponent(String(theme.id))), { headers: { Accept: "application/json" } });
          const payload = await response.json();
          theme.supply_chain = Array.isArray(payload.layers) ? payload.layers : [];
        } catch {
          theme.supply_chain = [];
        }
      }));
      renderThemes();
      refreshWatchlistThemeFilter();
    } catch (error) {
      setThemeMessage(`Unable to load themes: ${error.message || error}`, true);
    }
  }

  async function createTheme(event) {
    event.preventDefault();
    const name = String((document.querySelector("[data-regime-theme-name]") || {}).value || "").trim();
    const narrative = String((document.querySelector("[data-regime-theme-narrative]") || {}).value || "");
    const sectorHint = String((document.querySelector("[data-regime-theme-sector-hint]") || {}).value || "").trim();
    const conviction = String((document.querySelector("[data-regime-theme-conviction]") || {}).value || "3");
    const status = String((document.querySelector("[data-regime-theme-status]") || {}).value || "Active");
    const body = new URLSearchParams();
    body.set("name", name);
    body.set("narrative", narrative);
    body.set("sector_hint", sectorHint);
    body.set("conviction", conviction);
    body.set("status", status);
    try {
      const response = await fetch(state.config.endpoints.themes, {
        method: "POST",
        body,
        headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Create failed (${response.status})`);
      await loadThemes();
      setThemeMessage(`Created theme ${payload.name}.`);
    } catch (error) {
      setThemeMessage(`Unable to create theme: ${error.message || error}`, true);
    }
  }

  async function loadPortfolioScopes() {
    const select = document.querySelector("[data-regime-portfolio-scope]");
    const counts = byId("regimePortfolioCounts");
    if (!select) return;
    const applyScopes = (scopes) => {
      state.portfolioScopes = scopes;
      const current = currentPortfolioScope();
      if (scopes.length) {
        select.innerHTML = scopes.map((scope) => `<option value="${escapeHtml(scope.value)}">${escapeHtml(scope.label)}</option>`).join("");
        select.value = current;
        if (counts) {
          counts.textContent = scopes.map((scope) => `${scope.label}: ${scope.ticker_count}`).join(" · ");
        }
        renderAccountOptions(select.value || current, false);
      }
    };
    try {
      const response = await fetch(state.config.endpoints.portfolios, { headers: { Accept: "application/json" } });
      const payload = await response.json();
      const scopes = Array.isArray(payload.scopes) ? payload.scopes : [];
      applyScopes(scopes);
    } catch (error) {
      if (counts) counts.textContent = `Unable to load portfolios: ${error.message || error}`;
    }
  }

  async function pollStatusLegacy(jobId) {
    if (state.pollTimer) window.clearInterval(state.pollTimer);
    state.pollTimer = window.setInterval(async () => {
      try {
        const url = state.config.endpoints.status.replace("__JOB_ID__", jobId);
        const response = await fetch(url, { headers: { Accept: "application/json" } });
        if (!response.ok) throw new Error(`Status request failed (${response.status})`);
        const payload = await response.json();
        setProgressFromPayload(payload);
        if (payload.partial_results) mergePartialResults(payload.partial_results);
        if (payload.status === "done") {
          stopRunTracking();
          if (payload.payload) renderPayload(payload.payload);
          const btn = document.querySelector("[data-regime-run]");
          if (btn) {
            btn.disabled = false;
            btn.textContent = "Run";
          }
        } else if (payload.status === "error") {
          stopRunTracking();
          const btn = document.querySelector("[data-regime-run]");
          if (btn) {
            btn.disabled = false;
            btn.textContent = "Run";
          }
          setProgress("error", payload.progress || 0, payload.total || 0, "", payload.error || "Analysis failed.");
        }
      } catch (error) {
        stopRunTracking();
        setProgress("error", 0, 0, "", error.message || String(error));
      }
    }, 2000);
  }

  function connectStream(jobId) {
    stopRunTracking();
    if (typeof window.EventSource === "undefined") {
      pollStatusLegacy(jobId);
      return;
    }
    const url = state.config.endpoints.stream.replace("__JOB_ID__", jobId);
    const source = new window.EventSource(url);
    state.eventSource = source;
    state.streamRetries = 0;
    updateStreamBadge("connected");

    source.addEventListener("progress", (event) => {
      const data = JSON.parse(event.data);
      setProgressFromPayload(data);
      if (data.partial_result) mergePartialResults(data.partial_result);
      updateStreamBadge("connected");
      state.streamRetries = 0;
    });

    source.addEventListener("done", (event) => {
      const data = JSON.parse(event.data);
      stopRunTracking();
      setProgressFromPayload(data);
      if (data.payload) renderPayload(data.payload);
      const btn = document.querySelector("[data-regime-run]");
      if (btn) {
        btn.disabled = false;
        btn.textContent = "Run";
      }
    });

    source.addEventListener("error", (event) => {
      if (event && event.data) {
        try {
          const data = JSON.parse(event.data);
          stopRunTracking();
          const btn = document.querySelector("[data-regime-run]");
          if (btn) {
            btn.disabled = false;
            btn.textContent = "Run";
          }
          setProgress("error", 0, 0, "", data.error || "Analysis failed.");
          return;
        } catch (_error) {
        }
      }
      if (source.readyState === window.EventSource.CLOSED) {
        stopRunTracking();
        return;
      }
      state.streamRetries += 1;
      updateStreamBadge("reconnecting");
      if (state.streamRetries >= 3) {
        source.close();
        state.eventSource = null;
        pollStatusLegacy(jobId);
      }
    });

    source.addEventListener("heartbeat", () => {
      updateStreamBadge("connected");
    });
  }

  async function submitRun(event) {
    event.preventDefault();
    const btn = document.querySelector("[data-regime-run]");
    const exportBtn = byId("regimeExportPdf");
    const tickers = getSelectedTickers();
    updateSelectionCounter();
    if (!tickers.length) {
      setProgress("error", 0, 0, "", "Select at least one ticker.");
      return;
    }
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Queued…";
    }
    if (exportBtn) {
      exportBtn.style.pointerEvents = "none";
      exportBtn.style.opacity = "0.5";
    }
    setProgress("pending", 0, tickers.length, "", "");
    const body = new URLSearchParams();
    body.set("tickers", tickers.join(","));
    body.set("benchmark", currentBenchmark());
    body.set("period", currentPeriod());
    body.set("portfolio_scope", currentPortfolioScope());
    if (currentAccountId()) body.set("account_id", currentAccountId());
    if (showAllEnabled()) body.set("show_all", "true");
    if (currentFrontierEnabled()) body.set("frontier_enabled", "true");
    if (currentFrontierEnabled()) body.set("frontier_provider", currentFrontierProvider());
    body.set("frontier_batch_size", String((document.querySelector("[data-regime-frontier-batch-size]") || {}).value || state.config.frontier_batch_size || 5));
    if (currentForceRefresh()) body.set("force_refresh", "true");
    try {
      const response = await fetch(state.config.endpoints.run, {
        method: "POST",
        body,
        headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `Run request failed (${response.status})`);
      state.currentJobId = payload.job_id;
      if (btn) btn.textContent = "Running…";
      connectStream(payload.job_id);
    } catch (error) {
      stopRunTracking();
      if (btn) {
        btn.disabled = false;
        btn.textContent = "Run";
      }
      setProgress("error", 0, tickers.length, "", error.message || String(error));
    }
  }

  function setDrawerOpen(open) {
    const drawer = byId("regimeThemeDrawer");
    const overlay = byId("regimeThemeDrawerOverlay");
    if (!drawer || !overlay) return;
    drawer.classList.toggle("regime-drawer--open", open);
    overlay.classList.toggle("regime-drawer-overlay--open", open);
    drawer.setAttribute("aria-hidden", open ? "false" : "true");
    savePref("themeDrawerOpen", !!open);
  }

  function initDisclosurePrefs() {
    const prefs = loadPrefs();
    document.querySelectorAll(".ui-disclosure, .regime-control-bar").forEach((details, index) => {
      const key = details.id || `details_${index}`;
      if (Object.prototype.hasOwnProperty.call(prefs, key)) {
        details.open = !!prefs[key];
      }
      details.addEventListener("toggle", () => savePref(key, details.open));
    });
  }

  function wireControls() {
    const form = document.querySelector("[data-regime-controls]");
    if (form) form.addEventListener("submit", submitRun);
    const runButton = document.querySelector("[data-regime-run]");
    if (runButton) {
      runButton.addEventListener("click", (event) => {
        submitRun(event);
      });
    }
    const customInput = document.querySelector("[data-regime-custom-input]");
    if (customInput) customInput.addEventListener("input", updateSelectionCounter);
    const themeForm = document.querySelector("[data-regime-theme-form]");
    if (themeForm) themeForm.addEventListener("submit", createTheme);
    const themeDrawerBtn = byId("regimeManageThemes");
    if (themeDrawerBtn) {
      themeDrawerBtn.addEventListener("click", () => setDrawerOpen(true));
    }
    const themeDrawerClose = byId("regimeThemeDrawerClose");
    if (themeDrawerClose) {
      themeDrawerClose.addEventListener("click", () => setDrawerOpen(false));
    }
    const themeDrawerOverlay = byId("regimeThemeDrawerOverlay");
    if (themeDrawerOverlay) {
      themeDrawerOverlay.addEventListener("click", () => setDrawerOpen(false));
    }
    const discoveryBtn = byId("regimeDiscoveryScanBtn");
    if (discoveryBtn) {
      discoveryBtn.addEventListener("click", async () => {
        try {
          await startDiscoveryScan();
        } catch (error) {
          setThemeMessage(`Unable to start discovery scan: ${error.message || error}`, true);
        }
      });
    }
    const watchlistThemeFilter = byId("regimeWatchlistThemeFilter");
    if (watchlistThemeFilter) watchlistThemeFilter.addEventListener("change", () => loadWatchlist());
    const watchlistStatusFilter = byId("regimeWatchlistStatusFilter");
    if (watchlistStatusFilter) watchlistStatusFilter.addEventListener("change", () => loadWatchlist());
    const showAll = document.querySelector("[data-regime-show-all]");
    if (showAll) {
      showAll.addEventListener("change", async () => {
        await loadHoldings();
        renderPayload({ ...(state.lastPayload || state.config.initial_payload), portfolio_mode: showAllEnabled() ? "All holdings" : "Filtered holdings" });
      });
    }
    const frontier = document.querySelector("[data-regime-frontier-enabled]");
    if (frontier) {
      frontier.addEventListener("change", async () => {
        syncFrontierControlVisibility();
        if (currentFrontierEnabled()) {
          await loadFrontierModels(currentFrontierProvider());
        }
        updateSelectionCounter();
        renderPayload({ ...(state.lastPayload || state.config.initial_payload), frontier_enabled: currentFrontierEnabled() });
      });
    }
    const providerSelect = document.querySelector("[data-regime-frontier-provider]");
    if (providerSelect) {
      providerSelect.addEventListener("change", async () => {
        const provider = currentFrontierProvider();
        const modelSelect = document.querySelector("[data-regime-frontier-model]");
        if (modelSelect) modelSelect.value = "";
        await saveFrontierSettings(provider, "");
        syncFrontierControlVisibility();
        await loadFrontierModels(provider);
      });
    }
    const modelSelect = document.querySelector("[data-regime-frontier-model]");
    if (modelSelect) {
      modelSelect.addEventListener("change", async () => {
        try {
          await saveFrontierSettings(currentFrontierProvider(), currentFrontierModel());
        } catch (error) {
          showToast(`Unable to save frontier model: ${error.message || error}`, "error");
        }
      });
    }
    const refreshModels = document.querySelector("[data-regime-refresh-models]");
    if (refreshModels) {
      refreshModels.addEventListener("click", async () => {
        await loadFrontierModels(currentFrontierProvider(), true);
      });
    }
    const portfolioScope = document.querySelector("[data-regime-portfolio-scope]");
    if (portfolioScope) {
      portfolioScope.addEventListener("change", async () => {
        renderAccountOptions(currentPortfolioScope(), true);
        await loadHoldings();
      });
    }
    const accountSelect = document.querySelector("[data-regime-account-id]");
    if (accountSelect) {
      accountSelect.addEventListener("change", async () => {
        await loadHoldings();
      });
    }
    const selectAll = byId("regimeSelectAll");
    if (selectAll) {
      selectAll.addEventListener("click", () => {
        if (state.selectedHoldings.length === state.holdings.length && state.holdings.length) {
          deselectAllHoldings();
        } else {
          selectAllHoldings();
        }
        selectAll.textContent = state.selectedHoldings.length === state.holdings.length && state.holdings.length ? "Deselect all" : "Select all";
      });
    }
    const searchInput = document.querySelector("[data-regime-search-input]");
    if (searchInput) {
      searchInput.addEventListener("input", () => {
        state.pickerFocusIndex = -1;
        renderPickerOptions();
      });
      searchInput.addEventListener("keydown", (event) => {
        const items = Array.from(document.querySelectorAll("[data-regime-picker-item]"));
        if (!items.length) return;
        if (event.key === "ArrowDown") {
          event.preventDefault();
          state.pickerFocusIndex = Math.min(items.length - 1, state.pickerFocusIndex + 1);
          highlightPickerFocus();
        } else if (event.key === "ArrowUp") {
          event.preventDefault();
          state.pickerFocusIndex = Math.max(0, state.pickerFocusIndex - 1);
          highlightPickerFocus();
        } else if (event.key === "Enter") {
          if (state.pickerFocusIndex >= 0 && items[state.pickerFocusIndex]) {
            event.preventDefault();
            items[state.pickerFocusIndex].click();
          }
        } else if (event.key === "Escape") {
          event.preventDefault();
          searchInput.blur();
        }
      });
    }
    const expandAll = byId("regimeExpandAll");
    if (expandAll) {
      expandAll.addEventListener("click", () => {
        document.querySelectorAll("[data-regime-detail]").forEach((detail) => {
          detail.open = true;
          renderChartsForTicker(String(detail.getAttribute("data-regime-detail") || ""));
          savePref(`detail_${String(detail.getAttribute("data-regime-detail") || "")}`, true);
        });
      });
    }
    const collapseAll = byId("regimeCollapseAll");
    if (collapseAll) {
      collapseAll.addEventListener("click", () => {
        document.querySelectorAll("[data-regime-detail]").forEach((detail) => {
          detail.open = false;
          savePref(`detail_${String(detail.getAttribute("data-regime-detail") || "")}`, false);
        });
      });
    }
    const toggleColumns = byId("regimeToggleColumns");
    if (toggleColumns) {
      toggleColumns.addEventListener("click", () => {
        state.showAllTableColumns = !state.showAllTableColumns;
        savePref("showAllColumns", state.showAllTableColumns);
        applyTableColumnVisibility();
      });
    }
  }

  function init() {
    state.config = parseJson("regimeConfig");
    if (!state.config) return;
    const prefs = loadPrefs();
    state.showAllTableColumns = !!prefs.showAllColumns;
    updateSelectionCounter();
    wireControls();
    initTabs();
    initDisclosurePrefs();
    syncFrontierControlVisibility();
    setDrawerOpen(!!prefs.themeDrawerOpen);
    if (Array.isArray(state.config.portfolio_scopes) && state.config.portfolio_scopes.length) {
      state.portfolioScopes = state.config.portfolio_scopes;
      const select = document.querySelector("[data-regime-portfolio-scope]");
      const counts = byId("regimePortfolioCounts");
      if (select) {
        select.innerHTML = state.portfolioScopes.map((scope) => `<option value="${escapeHtml(scope.value)}">${escapeHtml(scope.label)}</option>`).join("");
        select.value = currentPortfolioScope();
      }
      if (counts) {
        counts.textContent = state.portfolioScopes.map((scope) => `${scope.label}: ${scope.ticker_count}`).join(" · ");
      }
      renderAccountOptions(currentPortfolioScope(), false);
    }
    loadPortfolioScopes().then(() => loadHoldings());
    loadFrontierSettings()
      .then(async (settings) => {
        const providerSelect = document.querySelector("[data-regime-frontier-provider]");
        if (settings.provider && providerSelect) {
          providerSelect.value = settings.provider;
        }
        syncFrontierControlVisibility();
        if (settings.provider && settings.provider !== "auto" && settings.provider !== "best" && currentFrontierEnabled()) {
          await loadFrontierModels(settings.provider);
        }
      })
      .catch(() => {
        syncFrontierControlVisibility();
      });
    try {
      renderPayload(state.config.initial_payload || {});
    } catch (error) {
      console.error("Unable to render initial regime payload.", error);
    }
    try {
      renderPaperPortfolioSection();
      renderPaperBudget();
      renderPaperPlans();
      renderPaperPositions();
      renderPaperPerformance();
      renderAuditTrail();
      renderAlertToasts();
      renderAlertHistory();
      renderVixStatus();
      renderMonitoringDashboard(null);
    } catch (error) {
      console.error("Unable to render initial paper trading shell.", error);
    }
    loadThemes().catch?.(() => {});
    loadWatchlist().catch?.(() => {});
    loadPaperPortfolios().catch?.(() => {});
    loadEnsembleWeights().catch?.(() => {});
    setProgress("idle", 0, 0, "", "");
  }

  init();
})();
