(() => {
  const root = document.querySelector("[data-holdings]");
  if (!root) return;

  // Auto-apply filters (keep Apply button for no-JS).
  const filterForm = document.getElementById("holdingsFilterForm");
  if (filterForm) {
    const selects = filterForm.querySelectorAll("select");
    selects.forEach((el) => {
      el.addEventListener("change", () => {
        filterForm.requestSubmit?.();
        if (!filterForm.requestSubmit) filterForm.submit();
      });
    });
  }

  // Column visibility (persisted in localStorage).
  const tableRoot = document.getElementById("holdingsTableRoot");
  const menu = document.getElementById("holdingsColumnsMenu");
  if (tableRoot && menu) {
    const storageKey = "investor.holdings.columns.v1";
    const defaults = {
      account: false,
      price: false,
      initial_cost: false,
      tax: false,
      entered: false,
      wash: false,
    };

    const dsKey = {
      account: "colAccount",
      price: "colPrice",
      initial_cost: "colInitialCost",
      tax: "colTax",
      entered: "colEntered",
      wash: "colWash",
    };

    function loadState() {
      try {
        const raw = localStorage.getItem(storageKey);
        if (!raw) return { ...defaults };
        const parsed = JSON.parse(raw);
        return { ...defaults, ...(parsed || {}) };
      } catch {
        return { ...defaults };
      }
    }

    function saveState(state) {
      try {
        localStorage.setItem(storageKey, JSON.stringify(state));
      } catch {
        // ignore (private mode, etc.)
      }
    }

    function applyState(state) {
      Object.entries(dsKey).forEach(([col, key]) => {
        tableRoot.dataset[key] = state[col] ? "1" : "0";
      });
      menu.querySelectorAll("input[type=checkbox][data-col]").forEach((cb) => {
        const col = cb.getAttribute("data-col");
        if (!col) return;
        cb.checked = !!state[col];
      });
    }

    let state = loadState();
    applyState(state);

    menu.querySelectorAll("input[type=checkbox][data-col]").forEach((cb) => {
      cb.addEventListener("change", () => {
        const col = cb.getAttribute("data-col");
        if (!col) return;
        state = { ...state, [col]: cb.checked };
        saveState(state);
        applyState(state);
      });
    });
  }

  // Holdings row drill-down (lots + sell-today estimate).
  const table = root.querySelector("#holdingsTableRoot table");
  if (!table) return;

  const cache = new Map();
  let open = null; // { btn, tr, detailsTr }

  function fmtUsd(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    const n = Number(value);
    const sign = n < 0 ? "-" : "";
    const abs = Math.abs(n);
    const s = abs.toFixed(2);
    const parts = s.split(".");
    parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    return `${sign}$${parts.join(".")}`;
  }

  function fmtPct(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    return `${(Number(value) * 100).toFixed(2)}%`;
  }

  function el(tag, attrs = {}, children = []) {
    const node = document.createElement(tag);
    Object.entries(attrs).forEach(([k, v]) => {
      if (v === null || v === undefined) return;
      if (k === "class") node.className = String(v);
      else if (k === "html") node.innerHTML = String(v);
      else if (k.startsWith("data-")) node.setAttribute(k, String(v));
      else node.setAttribute(k, String(v));
    });
    children.forEach((c) => node.appendChild(c));
    return node;
  }

  function closeOpen() {
    if (!open) return;
    open.btn.setAttribute("aria-expanded", "false");
    const icon = open.btn.querySelector(".holdings-row-toggle__icon");
    if (icon) icon.textContent = "▸";
    open.detailsTr.remove();
    open = null;
  }

  function parseRate(s) {
    const raw = String(s ?? "").trim();
    if (!raw) return 0;
    const n = Number(raw);
    if (!Number.isFinite(n)) return 0;
    // Allow "37" as 37% or "0.37" as 37%.
    const r = n > 1 ? n / 100 : n;
    return Math.min(1, Math.max(0, r));
  }

  function renderPanel(container, payload) {
    const pos = payload.position || {};
    const formatted = pos.formatted || {};
    const lots = payload.lots || [];
    const lotsSummary = payload.lots_summary || {};
    const wash = payload.wash || {};

    const pnl = pos.pnl_amount;
    const pnlNeg = pnl !== null && pnl !== undefined && Number(pnl) < 0;
    const pnlPos = pnl !== null && pnl !== undefined && Number(pnl) > 0;

    const header = el("div", { class: "holdings-drilldown__header" }, [
      el("div", { class: "holdings-drilldown__title" }),
    ]);
    header.querySelector(".holdings-drilldown__title").textContent = `${payload.symbol} · ${pos.account_name || "—"}`;

    const summary = el("div", { class: "holdings-drilldown__summary grid2" }, [
      el("div", { class: "holdings-drilldown__kv" }),
      el("div", { class: "holdings-drilldown__kv" }),
    ]);
    summary.children[0].innerHTML = `
      <div><span class="ui-muted">Qty</span><br><b class="ui-tabular-nums">${pos.qty ?? "—"}</b></div>
      <div style="margin-top:10px"><span class="ui-muted">Price</span><br><b class="ui-tabular-nums">${formatted.price || fmtUsd(pos.price)}</b></div>
    `;
    summary.children[1].innerHTML = `
      <div><span class="ui-muted">Market value</span><br><b class="ui-tabular-nums">${formatted.market_value || fmtUsd(pos.market_value)}</b></div>
      <div style="margin-top:10px"><span class="ui-muted">P&L</span><br><b class="ui-tabular-nums ${pnlNeg ? "gain-neg" : pnlPos ? "gain-pos" : ""}">${formatted.pnl_amount || fmtUsd(pos.pnl_amount)}</b> <span class="ui-muted ui-tabular-nums">(${formatted.pnl_pct || fmtPct(pos.pnl_pct)})</span></div>
    `;

    const lotsBlock = el("div", { class: "holdings-drilldown__section" });
    const lotsTitle = el("div", { class: "holdings-drilldown__section-title" });
    lotsTitle.textContent = "Lots";
    lotsBlock.appendChild(lotsTitle);
    {
      const scope = encodeURIComponent(payload.scope || "household");
      const accountId = encodeURIComponent(String(payload.account_id || ""));
      const src = String(payload.lots_source || "none").replace(/_/g, " ");
      const link = accountId ? `<a href="/taxlots?scope=${scope}&account_id=${accountId}">Open Tax Lots</a>` : `<a href="/taxlots?scope=${scope}">Open Tax Lots</a>`;
      lotsBlock.appendChild(el("div", { class: "ui-muted", html: `Source: ${src} · ${link}` }));
    }

    let lotsTableRegion = null;
    const lotsRowsByKey = new Map();
    let planRowsByKey = new Map();
    let hoverLinkedKey = null;
    let lastSelectedMetaByKey = new Map();

    function fmtQty(x) {
      if (x === null || x === undefined || Number.isNaN(Number(x))) return "—";
      const s = Number(x).toFixed(6);
      return s.replace(/\.?0+$/, "");
    }

    function lotKeyFromRow(r, acquired) {
      if (r && r.lot_id !== null && r.lot_id !== undefined && String(r.lot_id).trim() !== "") return `id:${r.lot_id}`;
      const qty = Number(r?.qty || 0);
      const cb = r?.cost_basis === null || r?.cost_basis === undefined ? "" : Number(r.cost_basis).toFixed(2);
      return `acq:${acquired}|qty:${qty.toFixed(6)}|cb:${cb}`;
    }

    function clearHoverLinked() {
      if (!hoverLinkedKey) return;
      const lr = lotsRowsByKey.get(hoverLinkedKey);
      if (lr) lr.classList.remove("is-hover-linked");
      const pr = planRowsByKey.get(hoverLinkedKey);
      if (pr) pr.classList.remove("is-hover-linked");
      hoverLinkedKey = null;
    }

    function setHoverLinked(key) {
      if (!key) {
        clearHoverLinked();
        return;
      }
      if (hoverLinkedKey === key) return;
      clearHoverLinked();
      hoverLinkedKey = key;
      const lr = lotsRowsByKey.get(key);
      if (lr) lr.classList.add("is-hover-linked");
      const pr = planRowsByKey.get(key);
      if (pr) pr.classList.add("is-hover-linked");
    }

    function clearLotHighlights() {
      for (const [, row] of lotsRowsByKey) {
        row.classList.remove("is-selected", "is-partial");
        row.classList.remove("is-pulse");
        const cell = row.querySelector("[data-lot-selected]");
        if (cell) cell.replaceChildren();
      }
      lastSelectedMetaByKey = new Map();
    }

    function applyLotHighlights(plan) {
      clearLotHighlights();
      const selectedByKey = new Map();
      for (const p of plan || []) {
        const k = String(p.lotKey || "").trim();
        if (!k) continue;
        selectedByKey.set(k, { sellQty: Number(p.sellQty || 0), availQty: Number(p.availQty || 0) });
      }

      const prefersReduced = !!(window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches);
      const newlySelectedKeys = new Set();
      for (const [k, meta] of selectedByKey) {
        const prev = lastSelectedMetaByKey.get(k);
        if (!prev || Number(prev.sellQty || 0) !== Number(meta.sellQty || 0) || Number(prev.availQty || 0) !== Number(meta.availQty || 0)) {
          newlySelectedKeys.add(k);
        }
      }
      lastSelectedMetaByKey = selectedByKey;

      for (const [k, meta] of selectedByKey) {
        const row = lotsRowsByKey.get(k);
        if (!row) continue;
        const sellQty = Number(meta.sellQty || 0);
        const availQty = Number(meta.availQty || 0);
        if (!(sellQty > 0)) continue;
        const isPartial = availQty > 0 && sellQty < availQty - 1e-9;
        row.classList.add("is-selected");
        if (isPartial) row.classList.add("is-partial");
        if (!prefersReduced && newlySelectedKeys.has(k)) {
          row.classList.add("is-pulse");
          setTimeout(() => row.classList.remove("is-pulse"), 320);
        }
        const cell = row.querySelector("[data-lot-selected]");
        if (cell) {
          const pill = document.createElement("span");
          pill.className = `ui-badge ui-badge--outline ui-badge--neutral lot-selected-pill${isPartial ? " lot-selected-pill--partial" : ""}`;
          pill.textContent = "Selected";
          const sub = document.createElement("div");
          sub.className = "ui-muted lot-selected-sub ui-tabular-nums";
          sub.textContent = `Sell: ${fmtQty(sellQty)} / ${fmtQty(availQty)}`;
          cell.replaceChildren(pill, sub);
        }
      }
    }

    if (!lots.length) {
      const msg = lotsSummary && lotsSummary.missing_basis_lots ? "No lots with basis available." : "No lots available; snapshot basis only.";
      lotsBlock.appendChild(el("div", { class: "ui-muted", html: msg }));
    } else {
      const lotsTableWrap = el("div", { class: "table-wrap holdings-lots-table-wrap", role: "region", "aria-label": "Lots table" });
      lotsTableRegion = lotsTableWrap;

      const legend = el("div", { class: "lot-selection-legend ui-muted", role: "note" });
      legend.innerHTML = `
        <div class="lot-selection-legend__title">Lot selection</div>
        <div class="lot-selection-legend__items">
          <div class="lot-selection-legend__item">
            <span class="ui-badge ui-badge--outline ui-badge--neutral lot-selected-pill">Selected</span>
            <span>Lot included in current sell plan</span>
          </div>
          <div class="lot-selection-legend__item">
            <span class="ui-tabular-nums">Sell: 25 / 100</span>
            <span>Partial sale from this lot</span>
          </div>
        </div>
      `;
      lotsBlock.appendChild(legend);

      const t = el("table", { class: "holdings-lots-table" });
      t.innerHTML = `
        <thead>
          <tr>
            <th>Acquired</th>
            <th class="num">Qty</th>
            <th class="num">Cost basis</th>
            <th class="num">Current value</th>
            <th class="num">Gain</th>
            <th class="num">Gain %</th>
            <th>Term</th>
            <th class="num">Days</th>
            <th>Selected</th>
          </tr>
        </thead>
        <tbody></tbody>
      `;
      const tbody = t.querySelector("tbody");
      lots.forEach((r) => {
        const gain = r.gain;
        const gNeg = gain !== null && gain !== undefined && Number(gain) < 0;
        const gPos = gain !== null && gain !== undefined && Number(gain) > 0;
        const term = String(r.term || "—");
        const termBadge = `<span class="ui-badge ui-badge--outline ui-badge--neutral">${term}</span>`;
        const acquired = r.acquired_date ? String(r.acquired_date).slice(0, 10) : "—";
        const lotKey = lotKeyFromRow(r, acquired);
        const tr = el("tr", {}, []);
        tr.className = "lot-row";
        tr.dataset.lotKey = lotKey;
        tr.dataset.lotQty = String(r.qty ?? "");
        tr.tabIndex = 0;
        tr.innerHTML = `
          <td class="ui-muted nowrap">${acquired}</td>
          <td class="num ui-tabular-nums ui-muted">${Number(r.qty || 0).toFixed(6)}</td>
          <td class="num ui-tabular-nums ui-muted">${r.cost_basis === null || r.cost_basis === undefined ? "—" : fmtUsd(r.cost_basis)}</td>
          <td class="num ui-tabular-nums">${r.current_value === null || r.current_value === undefined ? "—" : fmtUsd(r.current_value)}</td>
          <td class="num ui-tabular-nums ${gNeg ? "gain-neg" : gPos ? "gain-pos" : ""}">${r.gain === null || r.gain === undefined ? "—" : fmtUsd(r.gain)}</td>
          <td class="num ui-tabular-nums ${gNeg ? "gain-neg" : gPos ? "gain-pos" : ""}">${r.gain_pct === null || r.gain_pct === undefined ? "—" : fmtPct(r.gain_pct)}</td>
          <td>${termBadge}</td>
          <td class="num ui-tabular-nums ui-muted">${r.days_held ?? "—"}</td>
          <td class="lot-selected-cell"><span data-lot-selected></span></td>
        `;
        tbody.appendChild(tr);
        lotsRowsByKey.set(lotKey, tr);
        tr.addEventListener("mouseenter", () => setHoverLinked(lotKey));
        tr.addEventListener("mouseleave", () => setHoverLinked(null));
        tr.addEventListener("focusin", () => setHoverLinked(lotKey));
        tr.addEventListener("focusout", () => setHoverLinked(null));
      });
      lotsTableWrap.appendChild(t);
      lotsBlock.appendChild(lotsTableWrap);

      if (payload.lots_truncated) {
        lotsBlock.appendChild(el("div", { class: "ui-muted", html: `Showing first ${lots.length} lots.` }));
      }
    }

    const taxBlock = el("div", { class: "holdings-drilldown__section" });
    const taxTitle = el("div", { class: "holdings-drilldown__section-title" });
    taxTitle.textContent = "Sell today (estimate)";
    taxBlock.appendChild(taxTitle);
    taxBlock.appendChild(el("div", { class: "ui-muted", html: "Uses your marginal rates. This is an estimate, not tax advice." }));

    const stGain = Number(lotsSummary.st_gain || 0);
    const ltGain = Number(lotsSummary.lt_gain || 0);
    const proceeds = pos.market_value === null || pos.market_value === undefined ? null : Number(pos.market_value);

    const form = el("div", { class: "holdings-drilldown__tax form-compact" });
    form.innerHTML = `
      <label>Filing status
        <select data-tax="status">
          <option value="SINGLE">Single</option>
          <option value="MFJ">Married filing jointly</option>
          <option value="MFS">Married filing separately</option>
          <option value="HOH">Head of household</option>
        </select>
      </label>
      <label>Marginal ordinary rate (%)
        <input data-tax="ordinary" type="number" min="0" max="100" step="0.1" value="37">
      </label>
      <label>Marginal LT cap gains rate (%)
        <input data-tax="lt" type="number" min="0" max="100" step="0.1" value="15">
      </label>
      <label style="display:flex;gap:8px;align-items:center">
        <input data-tax="niit" type="checkbox">
        Apply NIIT (3.8%)
      </label>
      <div class="holdings-drilldown__tax-out ui-card" style="padding:10px">
        <div class="ui-muted">Estimated tax</div>
        <div class="ui-tabular-nums" style="margin-top:6px">
          <div>ST tax: <b data-tax-out="st">—</b></div>
          <div>LT tax: <b data-tax-out="lt">—</b></div>
          <div style="margin-top:6px;border-top:1px solid #e5e7eb;padding-top:6px">Total: <b data-tax-out="total">—</b></div>
          <div class="ui-muted" style="margin-top:6px">Net proceeds: <b data-tax-out="net">—</b></div>
        </div>
      </div>
    `;
    taxBlock.appendChild(form);

    function getRates() {
      const ordRate = parseRate(form.querySelector('[data-tax="ordinary"]').value);
      const ltRate = parseRate(form.querySelector('[data-tax="lt"]').value);
      const niitOn = !!form.querySelector('[data-tax="niit"]').checked;
      const niit = niitOn ? 0.038 : 0;
      return { ordRate, ltRate, niit, niitOn };
    }

    function recalcTax() {
      const { ordRate, ltRate, niit } = getRates();
      const stTax = Math.max(0, stGain) * (ordRate + niit);
      const ltTax = Math.max(0, ltGain) * (ltRate + niit);
      const total = stTax + ltTax;
      const net = proceeds === null ? null : proceeds - total;
      form.querySelector('[data-tax-out="st"]').textContent = fmtUsd(stTax);
      form.querySelector('[data-tax-out="lt"]').textContent = fmtUsd(ltTax);
      form.querySelector('[data-tax-out="total"]').textContent = fmtUsd(total);
      form.querySelector('[data-tax-out="net"]').textContent = net === null ? "—" : fmtUsd(net);
      try {
        updateRatesLine();
      } catch {
        // ignore
      }
    }
    form.querySelectorAll('input[data-tax], select[data-tax]').forEach((x) => x.addEventListener("input", recalcTax));
    recalcTax();

    if (lotsSummary.missing_basis_lots && Number(lotsSummary.missing_basis_lots) > 0) {
      taxBlock.appendChild(
        el("div", {
          class: "ui-muted",
          html: `Tax estimate excludes ${lotsSummary.missing_basis_lots} lot(s) with missing basis.`,
        }),
      );
    }

    const optimizerBlock = el("div", { class: "holdings-drilldown__section" });
    const optimizerTitle = el("div", { class: "holdings-drilldown__section-title" });
    optimizerTitle.textContent = "Sell optimizer";
    optimizerBlock.appendChild(optimizerTitle);
    optimizerBlock.appendChild(
      el("div", {
        class: "ui-muted",
        html: "Builds a tax-smart lot sale plan using the marginal rates below. Estimates only.",
      }),
    );

    const hasPositionWashRisk = String((wash || {}).status || "") === "RISK";
    const defaultExcludeWash = hasPositionWashRisk;

    const controls = el("div", { class: "holdings-optimizer__controls form-compact" });
    const uid = `opt-${String(payload.account_id || "x")}-${String(payload.symbol || "x").replace(/[^a-z0-9]/gi, "_")}`;
    controls.innerHTML = `
      <div class="opt-grid">
        <div class="opt-col">
          <fieldset class="holdings-optimizer__fieldset" aria-describedby="${uid}-targetHelp">
            <legend>Sell amount</legend>
            <label class="holdings-optimizer__radio">
              <input type="radio" name="${uid}-sellMode" value="shares" checked aria-controls="${uid}-target">
              Sell shares
            </label>
            <label class="holdings-optimizer__radio">
              <input type="radio" name="${uid}-sellMode" value="gross" aria-controls="${uid}-target">
              Raise cash (pre-tax)
            </label>
            <label class="holdings-optimizer__radio">
              <input type="radio" name="${uid}-sellMode" value="net" aria-controls="${uid}-target">
              Raise cash (after-tax net)
            </label>
          </fieldset>

          <label for="${uid}-target">
            <span data-opt="targetLabel">Shares to sell</span>
            <input id="${uid}-target" data-opt="target" type="number" step="0.000001" min="0" placeholder="e.g., 150" inputmode="decimal" value="">
            <div class="ui-muted" id="${uid}-targetHelp" data-opt="targetHelp">Number of shares to sell.</div>
          </label>

          <div class="opt-rates-line ui-muted" role="note">
            <span data-opt="ratesLine">Rates in use: —</span>
            <button type="button" class="btn btn--secondary opt-inline-action" data-opt="editRates">Edit rates</button>
          </div>
        </div>

        <div class="opt-col">
          <label for="${uid}-goal">Goal
            <select id="${uid}-goal" data-opt="goal">
              <option value="min_tax" selected>Minimize taxes</option>
              <option value="max_loss">Maximize loss harvesting</option>
              <option value="max_net">Maximize net proceeds (after tax)</option>
              <option value="prefer_lt">Prefer long-term gains</option>
              <option value="min_wash">Minimize wash-sale risk</option>
            </select>
          </label>

          <fieldset class="holdings-optimizer__fieldset" aria-describedby="${uid}-constraintsHelp">
            <legend>Constraints</legend>
            <label class="holdings-optimizer__check"><input data-opt="excludeWash" type="checkbox" ${defaultExcludeWash ? "checked" : ""}> Exclude wash-sale “Risk” lots</label>
            <details class="opt-advanced">
              <summary class="ui-muted">Advanced constraints</summary>
              <div class="opt-advanced__body">
                <label class="holdings-optimizer__check"><input data-opt="avoidSt" type="checkbox"> Avoid short-term gains</label>
                <label class="holdings-optimizer__check"><input data-opt="avoidGains" type="checkbox"> Avoid realizing gains</label>
              </div>
            </details>
            <div class="ui-muted" id="${uid}-constraintsHelp">Constraints affect which lots are eligible and/or priority.</div>
          </fieldset>
        </div>

        <div class="ui-card opt-context" role="status" aria-live="polite" aria-atomic="true" data-opt="context">
          <div class="opt-context__title">Optimizer context</div>
          <div class="opt-context__rows" data-opt="contextRows">
            <div class="ui-muted">Loading…</div>
          </div>
          <div class="ui-muted opt-context__hint" data-opt="contextHint" style="display:none">No eligible lots under current constraints.</div>
        </div>
      </div>

      <div class="holdings-optimizer__actions">
        <button type="button" class="btn btn--primary" data-opt="run" disabled>Optimize</button>
        <button type="button" class="btn" data-opt="reset">Reset</button>
        <span class="ui-badge ui-badge--neutral opt-status" data-opt="status">Enter target</span>
      </div>
    `;
    optimizerBlock.appendChild(controls);

    const planSummary = el("div", {
      class: "ui-card opt-summary",
      role: "status",
      "aria-live": "polite",
      "aria-atomic": "true",
    });
    planSummary.innerHTML = `<div class="ui-muted">Plan summary</div><div style="margin-top:6px">No plan yet. Choose target + goal, then click Optimize.</div>`;
    optimizerBlock.appendChild(planSummary);

    const output = el("div", { class: "holdings-optimizer__output" });
    optimizerBlock.appendChild(output);

    const targetInput = controls.querySelector('[data-opt="target"]');
    const optimizeBtn = controls.querySelector('[data-opt="run"]');
    const statusPill = controls.querySelector('[data-opt="status"]');
    const contextRows = controls.querySelector('[data-opt="contextRows"]');
    const contextHint = controls.querySelector('[data-opt="contextHint"]');

    function updateRatesLine() {
      const line = controls.querySelector('[data-opt="ratesLine"]');
      if (!line) return;
      const { ordRate, ltRate, niitOn } = getRates();
      const stPct = (ordRate * 100).toFixed(1).replace(/\.0$/, "");
      const ltPct = (ltRate * 100).toFixed(1).replace(/\.0$/, "");
      line.textContent = `Rates in use: ST ${stPct}% · LT ${ltPct}% · NIIT ${niitOn ? "on" : "off"}`;
    }
    updateRatesLine();

    function updateTargetUI(mode) {
      const label = controls.querySelector('[data-opt="targetLabel"]');
      const help = controls.querySelector('[data-opt="targetHelp"]');
      if (!targetInput) return;
      if (mode === "gross") {
        if (label) label.textContent = "Gross proceeds target ($)";
        targetInput.step = "0.01";
        targetInput.inputMode = "decimal";
        targetInput.placeholder = "e.g., 25000";
        if (help) help.textContent = "Optimizer targets gross proceeds before tax.";
      } else if (mode === "net") {
        if (label) label.textContent = "Net proceeds target ($)";
        targetInput.step = "0.01";
        targetInput.inputMode = "decimal";
        targetInput.placeholder = "e.g., 25000";
        if (help) help.textContent = "Optimizer targets proceeds after estimated tax.";
      } else {
        if (label) label.textContent = "Shares to sell";
        targetInput.step = "0.000001";
        targetInput.inputMode = "decimal";
        targetInput.placeholder = "e.g., 150";
        if (help) help.textContent = "Number of shares to sell.";
      }
    }
    updateTargetUI("shares");

    function computeContextCounts() {
      const totalLots = Array.isArray(lots) ? lots.length : 0;
      let stLots = 0;
      let ltLots = 0;
      let gainLots = 0;
      let lossLots = 0;
      let sharesTotal = 0;
      for (const r of lots || []) {
        const term = String(r.term || "");
        if (term === "ST") stLots += 1;
        else if (term === "LT") ltLots += 1;
        const qty = Number(r.qty || 0);
        if (Number.isFinite(qty)) sharesTotal += qty;
        const g = r.gain;
        if (g === null || g === undefined) continue;
        const gv = Number(g);
        if (!Number.isFinite(gv)) continue;
        if (gv > 0) gainLots += 1;
        else if (gv < 0) lossLots += 1;
      }
      return { totalLots, stLots, ltLots, gainLots, lossLots, sharesTotal };
    }

    function washRiskState() {
      // v1: position-level only (no per-lot wash tags).
      if (!lots || !lots.length) return { label: "—", tone: "neutral", title: "" };
      if (hasPositionWashRisk) {
        return { label: "All", tone: "risk", title: "Based on buys within the last 30 days (position-level)." };
      }
      return { label: "None", tone: "safe", title: "Based on buys within the last 30 days (position-level)." };
    }

    function updateContextPanel() {
      if (!contextRows) return;
      const counts = computeContextCounts();
      const excludeWash = !!controls.querySelector('[data-opt="excludeWash"]')?.checked;
      const goal = String(controls.querySelector('[data-opt="goal"]')?.value || "min_tax");
      const excludeWashEffective = excludeWash || goal === "min_wash";
      const eligibleLots = excludeWashEffective && hasPositionWashRisk ? 0 : counts.totalLots;
      const eligibleShares = excludeWashEffective && hasPositionWashRisk ? 0 : counts.sharesTotal;
      const risk = washRiskState();

      const eligibleWarn = eligibleLots === 0 && counts.totalLots > 0;
      if (contextHint) contextHint.style.display = eligibleWarn ? "block" : "none";

      const eligibleCls = eligibleWarn ? "opt-context__value opt-context__value--warn" : "opt-context__value";
      const riskBadgeCls =
        risk.tone === "safe"
          ? "ui-badge ui-badge--outline ui-badge--safe"
          : risk.tone === "risk"
            ? "ui-badge ui-badge--outline ui-badge--risk"
            : "ui-badge ui-badge--outline ui-badge--neutral";

      contextRows.innerHTML = `
        <div class="opt-kv"><span class="opt-kv__k ui-muted">Total lots</span><span class="opt-kv__v ui-tabular-nums"><b>${counts.totalLots}</b></span></div>
        <div class="opt-kv"><span class="opt-kv__k ui-muted">Eligible lots</span><span class="${eligibleCls} ui-tabular-nums"><b>${eligibleLots}</b></span></div>
        <div class="opt-kv"><span class="opt-kv__k ui-muted">ST lots</span><span class="opt-kv__v ui-tabular-nums"><b>${eligibleLots ? counts.stLots : 0}</b></span></div>
        <div class="opt-kv"><span class="opt-kv__k ui-muted">LT lots</span><span class="opt-kv__v ui-tabular-nums"><b>${eligibleLots ? counts.ltLots : 0}</b></span></div>
        <div class="opt-kv"><span class="opt-kv__k ui-muted">Gain lots</span><span class="opt-kv__v ui-tabular-nums"><b>${eligibleLots ? counts.gainLots : 0}</b></span></div>
        <div class="opt-kv"><span class="opt-kv__k ui-muted">Loss lots</span><span class="opt-kv__v ui-tabular-nums"><b>${eligibleLots ? counts.lossLots : 0}</b></span></div>
        <div class="opt-kv"><span class="opt-kv__k ui-muted">Wash-sale risk</span><span class="opt-kv__v"><span class="${riskBadgeCls}" title="${risk.title}">${risk.label}</span></span></div>
        <div class="opt-kv"><span class="opt-kv__k ui-muted">Shares available</span><span class="opt-kv__v ui-tabular-nums"><b>${eligibleLots ? eligibleShares.toFixed(6) : "0.000000"}</b></span></div>
      `;
    }

    function isTargetValid() {
      if (!targetInput) return false;
      const raw = String(targetInput.value ?? "").trim();
      if (!raw) return false;
      const n = Number(raw);
      return Number.isFinite(n) && n > 0;
    }

    function updateOptimizeEnabled() {
      const ok = isTargetValid();
      if (optimizeBtn) optimizeBtn.disabled = !ok;
      if (statusPill) statusPill.textContent = ok ? "Ready" : "Enter target";
    }
    updateOptimizeEnabled();
    updateContextPanel();

    controls.querySelectorAll(`input[type="radio"][name="${uid}-sellMode"]`).forEach((r) => {
      r.addEventListener("change", () => {
        updateTargetUI(r.value);
        updateOptimizeEnabled();
      });
    });
    controls.querySelector('[data-opt="goal"]').addEventListener("change", (e) => {
      const v = e?.target?.value || "";
      if (v === "min_wash") {
        const cb = controls.querySelector('[data-opt="excludeWash"]');
        if (cb) cb.checked = true;
      }
      updateContextPanel();
    });
    if (targetInput) targetInput.addEventListener("input", updateOptimizeEnabled);
    controls.querySelectorAll('input[data-opt="excludeWash"], input[data-opt="avoidSt"], input[data-opt="avoidGains"]').forEach((x) => {
      x.addEventListener("change", updateContextPanel);
    });

    controls.querySelector('[data-opt="editRates"]').addEventListener("click", () => {
      const ordInput = form.querySelector('[data-tax="ordinary"]');
      if (!ordInput) return;
      ordInput.scrollIntoView?.({ behavior: "smooth", block: "center" });
      setTimeout(() => ordInput.focus?.(), 150);
    });

    function normalizeLotsForOptimizer() {
      const px = pos.price === null || pos.price === undefined ? null : Number(pos.price);
      const excluded = { missingPrice: 0, missingBasis: 0, zeroQty: 0 };
      const out = [];
      for (const r of lots) {
        const qty = Number(r.qty || 0);
        if (!(qty > 0)) {
          excluded.zeroQty += 1;
          continue;
        }
        const cv = r.current_value === null || r.current_value === undefined ? null : Number(r.current_value);
        const unitPrice = cv !== null ? cv / qty : px;
        if (!unitPrice || !Number.isFinite(unitPrice) || unitPrice <= 0) {
          excluded.missingPrice += 1;
          continue;
        }
        const cb = r.cost_basis === null || r.cost_basis === undefined ? null : Number(r.cost_basis);
        if (cb === null || !Number.isFinite(cb)) {
          excluded.missingBasis += 1;
          continue;
        }
        const unitBasis = cb / qty;
        const unitGain = unitPrice - unitBasis;
        const term = String(r.term || "—");
        const acquired = r.acquired_date ? String(r.acquired_date).slice(0, 10) : "—";
        const lotKey = lotKeyFromRow(r, acquired);
        out.push({
          lotId: r.lot_id ?? null,
          lotKey,
          acquired,
          term,
          qty,
          unitPrice,
          unitBasis,
          unitGain,
          daysHeld: r.days_held ?? null,
        });
      }
      return { lots: out, excluded };
    }

    function compareKeys(a, b) {
      const n = Math.min(a.length, b.length);
      for (let i = 0; i < n; i += 1) {
        const av = a[i];
        const bv = b[i];
        if (av < bv) return -1;
        if (av > bv) return 1;
      }
      return a.length - b.length;
    }

    function lotSortKey(l, goal, constraints) {
      const isGain = l.unitGain > 0 ? 1 : 0;
      const isST = l.term === "ST" ? 1 : 0;
      const isLT = l.term === "LT" ? 1 : 0;
      const gain = l.unitGain;
      const net = l.unitPrice - Math.max(gain, 0) * (l.term === "ST" ? constraints.stRate : constraints.ltRate);

      // "Avoid realizing gains": losses first, then smallest gains.
      const gainPriority = constraints.avoidGains ? [isGain, gain] : [];

      if (goal === "max_loss") return [...gainPriority, gain, isST, l.acquired, String(l.lotId ?? "")];
      if (goal === "max_net") return [...gainPriority, -net, isST, l.acquired, String(l.lotId ?? "")];
      if (goal === "prefer_lt") return [...gainPriority, isST, isGain, gain, l.acquired, String(l.lotId ?? "")];
      if (goal === "min_wash") return [...gainPriority, isGain, isST, gain, l.acquired, String(l.lotId ?? "")];
      // default: min_tax
      return [...gainPriority, isGain, isST, gain, l.acquired, String(l.lotId ?? "")];
    }

    function optimizePlan() {
      const mode = controls.querySelector(`input[type="radio"][name="${uid}-sellMode"]:checked`)?.value || "shares";
      const goal = controls.querySelector('[data-opt="goal"]')?.value || "min_tax";
      const avoidSt = !!controls.querySelector('[data-opt="avoidSt"]')?.checked;
      const avoidGains = !!controls.querySelector('[data-opt="avoidGains"]')?.checked;
      const excludeWash = !!controls.querySelector('[data-opt="excludeWash"]')?.checked;
      const targetRaw = Number(controls.querySelector('[data-opt="target"]')?.value || 0);
      const target = Number.isFinite(targetRaw) ? targetRaw : 0;

      const { ordRate, ltRate, niit } = getRates();
      const stRate = ordRate + niit;
      const ltRateEff = ltRate + niit;

      const warnings = [];
      if (!(target > 0)) warnings.push("Enter a target greater than 0.");

      const excludeWashEffective = excludeWash || goal === "min_wash";
      if (excludeWashEffective && hasPositionWashRisk) {
        if (goal === "min_wash") {
          warnings.push("Position is in the wash-risk window. v1 cannot identify “safe” lots within the position; try again after the sellable date or choose a different goal.");
        } else {
          warnings.push("All lots are within the wash-risk window (position-level). Uncheck “Exclude wash-sale Risk lots” to generate a plan.");
        }
        return { blocked: true, blockReason: "wash_exclusion", warnings, plan: [] };
      }

      if (goal === "min_wash") {
        warnings.push("Wash-risk minimization is position-level (no per-lot wash tags).");
      }

      const norm = normalizeLotsForOptimizer();
      const lotsNorm = norm.lots;
      if (!lotsNorm.length) {
        warnings.push("No eligible lots (need price + cost basis).");
        if (norm.excluded.missingBasis) warnings.push(`Excluded ${norm.excluded.missingBasis} lot(s) missing basis.`);
        if (norm.excluded.missingPrice) warnings.push(`Excluded ${norm.excluded.missingPrice} lot(s) missing price.`);
        return { warnings, plan: [] };
      }

      if (norm.excluded.missingBasis) warnings.push(`Excluded ${norm.excluded.missingBasis} lot(s) missing basis.`);
      if (norm.excluded.missingPrice) warnings.push(`Excluded ${norm.excluded.missingPrice} lot(s) missing price.`);

      const constraints = { avoidGains, stRate, ltRate: ltRateEff };

      const sorted = [...lotsNorm].sort((a, b) => compareKeys(lotSortKey(a, goal, constraints), lotSortKey(b, goal, constraints)));

      let primary = sorted;
      let secondary = [];
      if (avoidSt) {
        // "Avoid short-term gains": keep ST losses eligible, but treat ST gain lots as last resort.
        const safe = sorted.filter((l) => !(l.term === "ST" && l.unitGain > 0));
        const stGains = sorted.filter((l) => l.term === "ST" && l.unitGain > 0);
        if (safe.length && stGains.length) {
          primary = safe;
          secondary = stGains;
        }
      }

      const plan = [];
      let remainingShares = mode === "shares" ? target : null;
      let remainingGross = mode === "gross" ? target : null;
      let remainingNet = mode === "net" ? target : null;
      let totalAvail = lotsNorm.reduce((a, l) => a + l.qty, 0);

      function lotTaxRate(l) {
        return l.term === "ST" ? stRate : ltRateEff;
      }

      function lotUnitTax(l) {
        return Math.max(l.unitGain, 0) * lotTaxRate(l);
      }

      function lotUnitNet(l) {
        return l.unitPrice - lotUnitTax(l);
      }

      function takeFrom(lot) {
        const avail = lot.qty;
        if (!(avail > 0)) return;
        let sell = 0;
        if (remainingShares !== null) {
          sell = Math.min(avail, remainingShares);
        } else if (remainingGross !== null) {
          sell = Math.min(avail, remainingGross / lot.unitPrice);
        } else if (remainingNet !== null) {
          const un = lotUnitNet(lot);
          if (!(un > 0)) return;
          sell = Math.min(avail, remainingNet / un);
        }
        if (!(sell > 0)) return;
        // Keep precision reasonable for display; underlying math uses float.
        sell = Math.min(avail, sell);

        const gross = sell * lot.unitPrice;
        const basis = sell * lot.unitBasis;
        const gain = sell * lot.unitGain;
        const tax = Math.max(gain, 0) * lotTaxRate(lot);
        const net = gross - tax;
        plan.push({
          lotKey: lot.lotKey,
          acquired: lot.acquired,
          term: lot.term,
          availQty: avail,
          sellQty: sell,
          gross,
          gain,
          tax,
          net,
        });

        if (remainingShares !== null) remainingShares -= sell;
        if (remainingGross !== null) remainingGross -= gross;
        if (remainingNet !== null) remainingNet -= net;
      }

      for (const lot of primary) {
        if (remainingShares !== null && remainingShares <= 1e-12) break;
        if (remainingGross !== null && remainingGross <= 0.01) break;
        if (remainingNet !== null && remainingNet <= 0.01) break;
        takeFrom(lot);
      }

      const unmet =
        (remainingShares !== null && remainingShares > 1e-12) ||
        (remainingGross !== null && remainingGross > 0.01) ||
        (remainingNet !== null && remainingNet > 0.01);

      if (unmet && secondary.length) {
        warnings.push("No sufficient LT lots to satisfy target; using ST lots as needed.");
        for (const lot of secondary) {
          if (remainingShares !== null && remainingShares <= 1e-12) break;
          if (remainingGross !== null && remainingGross <= 0.01) break;
          if (remainingNet !== null && remainingNet <= 0.01) break;
          takeFrom(lot);
        }
      }

      const unmetFinal =
        (remainingShares !== null && remainingShares > 1e-12) ||
        (remainingGross !== null && remainingGross > 0.01) ||
        (remainingNet !== null && remainingNet > 0.01);
      if (unmetFinal) {
        warnings.push("Requested target exceeds available lots; using maximum available.");
      }

      if (mode === "shares" && target > totalAvail + 1e-12) warnings.push(`Available shares: ${totalAvail.toFixed(6)}.`);
      return { warnings, plan, mode, goal, target, rates: { stRate, ltRate: ltRateEff } };
    }

    function renderOptimizerResult(result) {
      output.replaceChildren();
      planRowsByKey = new Map();
      clearHoverLinked();
      const goal = String(result?.goal || controls.querySelector('[data-opt="goal"]')?.value || "min_tax");
      const whyMap = {
        min_tax: "Selected loss lots first; then long-term lots with smallest gains to reduce tax.",
        max_loss: "Selected lots with the largest losses per share to maximize harvesting.",
        max_net: "Selected lots with the highest after-tax proceeds per share.",
        prefer_lt: "Selected long-term lots first; avoided short-term gain lots where possible.",
        min_wash: "Attempted to avoid wash-risk lots where possible (v1: position-level only).",
      };

      const warn = (result?.warnings || []).filter(Boolean);
      if (warn.length) {
        const isBlocked = !!result?.blocked;
        const d = el("div", { class: "alert alert--warn opt-note", role: "note" });
        const title = isBlocked ? "Cannot generate plan" : "Notes";
        const actions = [];
        if (result?.blockReason === "wash_exclusion") {
          actions.push(`<button type="button" class="btn btn--secondary opt-inline-action" data-opt="allowRisk">Allow Risk lots</button>`);
        }
        d.innerHTML = `
          <div class="opt-note__head">
            <b>${title}</b>
            ${actions.join("")}
          </div>
          <ul class="ui-muted opt-note__list">${warn.map((w) => `<li>${w}</li>`).join("")}</ul>
        `;
        output.appendChild(d);
        const allowBtn = d.querySelector('[data-opt="allowRisk"]');
        if (allowBtn) {
          allowBtn.addEventListener("click", () => {
            const goalSel = controls.querySelector('[data-opt="goal"]');
            if (goalSel && goalSel.value === "min_wash") goalSel.value = "min_tax";
            const cb = controls.querySelector('[data-opt="excludeWash"]');
            if (cb) cb.checked = false;
            updateOptimizeEnabled();
            updateContextPanel();
            renderOptimizerResult(optimizePlan());
          });
        }
      }

      const plan = result?.plan || [];
      if (!plan.length) {
        if (statusPill && result?.blocked) statusPill.textContent = "Cannot plan";
        clearLotHighlights();
        if (result?.blocked) {
          planSummary.innerHTML = `<div class="ui-muted">Plan summary</div><div style="margin-top:6px"><b>Cannot generate plan.</b> Adjust constraints or choose a different goal.</div>`;
        } else if (!isTargetValid()) {
          planSummary.innerHTML = `<div class="ui-muted">Plan summary</div><div style="margin-top:6px">Enter a target greater than 0, then click Optimize.</div>`;
        } else {
          planSummary.innerHTML = `<div class="ui-muted">Plan summary</div><div style="margin-top:6px">No plan yet. Choose target + goal, then click Optimize.</div>`;
        }
        return;
      }
      if (statusPill) statusPill.textContent = "Planned";
      applyLotHighlights(plan);

      let stGain = 0;
      let ltGain = 0;
      let stTax = 0;
      let ltTax = 0;
      let grossTotal = 0;
      let netTotal = 0;
      let qtyTotal = 0;
      for (const r of plan) {
        grossTotal += r.gross;
        netTotal += r.net;
        qtyTotal += r.sellQty;
        if (r.term === "ST") {
          stGain += r.gain;
          stTax += r.tax;
        } else if (r.term === "LT") {
          ltGain += r.gain;
          ltTax += r.tax;
        }
      }
      const totalTax = stTax + ltTax;

      planSummary.innerHTML = `
        <div class="ui-muted">Plan summary</div>
        <div class="ui-muted" style="margin-top:6px">Why these lots?</div>
        <div style="margin-top:2px">${whyMap[goal] || whyMap.min_tax}</div>
        <div class="ui-muted ui-tabular-nums" style="margin-top:8px">
          Shares: <b>${qtyTotal.toFixed(6)}</b>
          <span class="ui-muted">·</span>
          Gross: <b>${fmtUsd(grossTotal)}</b>
          <span class="ui-muted">·</span>
          Est tax: <b>${fmtUsd(totalTax)}</b>
          <span class="ui-muted">·</span>
          Net: <b>${fmtUsd(netTotal)}</b>
        </div>
      `;

      const summaryCard = el("div", { class: "ui-card holdings-optimizer__summary" });
      summaryCard.innerHTML = `
        <div class="ui-muted">Plan summary</div>
        <div class="holdings-optimizer__summary-grid ui-tabular-nums" style="margin-top:8px">
          <div><span class="ui-muted">Sell qty</span><br><b>${qtyTotal.toFixed(6)}</b></div>
          <div><span class="ui-muted">Gross proceeds</span><br><b>${fmtUsd(grossTotal)}</b></div>
          <div><span class="ui-muted">Est. tax</span><br><b>${fmtUsd(totalTax)}</b></div>
          <div><span class="ui-muted">Net proceeds</span><br><b>${fmtUsd(netTotal)}</b></div>
          <div><span class="ui-muted">ST gain/loss</span><br><b class="${stGain < 0 ? "gain-neg" : stGain > 0 ? "gain-pos" : ""}">${fmtUsd(stGain)}</b></div>
          <div><span class="ui-muted">LT gain/loss</span><br><b class="${ltGain < 0 ? "gain-neg" : ltGain > 0 ? "gain-pos" : ""}">${fmtUsd(ltGain)}</b></div>
          <div><span class="ui-muted">ST tax</span><br><b>${fmtUsd(stTax)}</b></div>
          <div><span class="ui-muted">LT tax</span><br><b>${fmtUsd(ltTax)}</b></div>
        </div>
        <div class="ui-muted" style="margin-top:8px">Lots used: <b>${plan.length}</b></div>
      `;
      output.appendChild(summaryCard);

      const planWrap = el("div", { class: "table-wrap holdings-optimizer__plan-wrap", role: "region", "aria-label": "Sell optimizer plan" });
      const planTable = el("table", { class: "holdings-optimizer__plan" });
      planTable.innerHTML = `
        <thead>
          <tr>
            <th>Acquired</th>
            <th>Term</th>
            <th class="num">Avail qty</th>
            <th class="num">Sell qty</th>
            <th class="num">Gross</th>
            <th class="num">Gain/Loss</th>
            <th class="num">Tax</th>
            <th class="num">Net</th>
          </tr>
        </thead>
        <tbody></tbody>
      `;
      const tbody = planTable.querySelector("tbody");
      for (const r of plan) {
        const gNeg = r.gain < 0;
        const gPos = r.gain > 0;
        const termBadge = `<span class="ui-badge ui-badge--outline ui-badge--neutral">${r.term}</span>`;
        const tr = document.createElement("tr");
        tr.className = "plan-row";
        tr.dataset.lotKey = String(r.lotKey || "");
        tr.tabIndex = 0;
        tr.innerHTML = `
          <td class="ui-muted nowrap">${r.acquired}</td>
          <td>${termBadge}</td>
          <td class="num ui-tabular-nums ui-muted">${Number(r.availQty).toFixed(6)}</td>
          <td class="num ui-tabular-nums"><b>${Number(r.sellQty).toFixed(6)}</b></td>
          <td class="num ui-tabular-nums">${fmtUsd(r.gross)}</td>
          <td class="num ui-tabular-nums ${gNeg ? "gain-neg" : gPos ? "gain-pos" : ""}">${fmtUsd(r.gain)}</td>
          <td class="num ui-tabular-nums">${fmtUsd(r.tax)}</td>
          <td class="num ui-tabular-nums"><b>${fmtUsd(r.net)}</b></td>
        `;
        tbody.appendChild(tr);
        const k = String(r.lotKey || "").trim();
        if (k) planRowsByKey.set(k, tr);
        tr.addEventListener("mouseenter", () => setHoverLinked(k));
        tr.addEventListener("mouseleave", () => setHoverLinked(null));
        tr.addEventListener("focusin", () => setHoverLinked(k));
        tr.addEventListener("focusout", () => setHoverLinked(null));
      }
      planWrap.appendChild(planTable);
      output.appendChild(planWrap);

      const helpers = el("div", { class: "holdings-optimizer__helpers" });
      helpers.innerHTML = `
        <button type="button" class="btn btn--secondary" data-opt="copy">Copy plan</button>
        <button type="button" class="btn btn--secondary" data-opt="csv">Download CSV</button>
      `;
      output.appendChild(helpers);

      function planText() {
        const asOf = pos.as_of ? String(pos.as_of) : "—";
        const lines = [];
        lines.push(`Sell plan — ${payload.symbol} (${pos.account_name || "—"})`);
        lines.push(`As of: ${asOf}`);
        lines.push(`Gross: ${fmtUsd(grossTotal)}  Tax: ${fmtUsd(totalTax)}  Net: ${fmtUsd(netTotal)}`);
        lines.push(`ST gain/loss: ${fmtUsd(stGain)}  LT gain/loss: ${fmtUsd(ltGain)}`);
        lines.push("");
        lines.push("Acquired, Term, SellQty, Gross, GainLoss, Tax, Net");
        for (const r of plan) {
          lines.push([r.acquired, r.term, Number(r.sellQty).toFixed(6), fmtUsd(r.gross), fmtUsd(r.gain), fmtUsd(r.tax), fmtUsd(r.net)].join(", "));
        }
        return lines.join("\n");
      }

      async function copyToClipboard(text) {
        try {
          await navigator.clipboard.writeText(text);
          return true;
        } catch {
          try {
            const ta = document.createElement("textarea");
            ta.value = text;
            ta.style.position = "fixed";
            ta.style.left = "-9999px";
            document.body.appendChild(ta);
            ta.focus();
            ta.select();
            const ok = document.execCommand("copy");
            ta.remove();
            return ok;
          } catch {
            return false;
          }
        }
      }

      function downloadCsv() {
        const rows = [["Acquired", "Term", "AvailQty", "SellQty", "Gross", "GainLoss", "Tax", "Net"]];
        for (const r of plan) {
          rows.push([
            r.acquired,
            r.term,
            Number(r.availQty).toFixed(6),
            Number(r.sellQty).toFixed(6),
            fmtUsd(r.gross),
            fmtUsd(r.gain),
            fmtUsd(r.tax),
            fmtUsd(r.net),
          ]);
        }
        const csv = rows.map((row) => row.map((c) => `"${String(c).replaceAll('"', '""')}"`).join(",")).join("\n");
        const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `sell_plan_${payload.symbol}.csv`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
      }

      helpers.querySelector('[data-opt="copy"]').addEventListener("click", async () => {
        const ok = await copyToClipboard(planText());
        if (!ok) alert("Copy failed. Your browser may block clipboard access.");
      });
      helpers.querySelector('[data-opt="csv"]').addEventListener("click", downloadCsv);
    }

    controls.querySelector('[data-opt="run"]').addEventListener("click", () => renderOptimizerResult(optimizePlan()));
    controls.querySelector('[data-opt="reset"]').addEventListener("click", () => {
      controls.querySelector(`input[type="radio"][name="${uid}-sellMode"][value="shares"]`).checked = true;
      controls.querySelector('[data-opt="target"]').value = "";
      controls.querySelector('[data-opt="goal"]').value = "min_tax";
      controls.querySelector('[data-opt="avoidSt"]').checked = false;
      controls.querySelector('[data-opt="avoidGains"]').checked = false;
      controls.querySelector('[data-opt="excludeWash"]').checked = defaultExcludeWash;
      updateTargetUI("shares");
      updateOptimizeEnabled();
      updateContextPanel();
      clearLotHighlights();
      planSummary.innerHTML = `<div class="ui-muted">Plan summary</div><div style="margin-top:6px">No plan yet. Choose target + goal, then click Optimize.</div>`;
      output.replaceChildren();
    });

    const washBlock = el("div", { class: "holdings-drilldown__section" });
    const washTitle = el("div", { class: "holdings-drilldown__section-title" });
    washTitle.textContent = "Wash-sale context";
    washBlock.appendChild(washTitle);
    if (wash.wash_safe_exit_date) {
      const badgeClass = wash.status === "SAFE" ? "ui-badge--safe" : "ui-badge--risk";
      washBlock.innerHTML += `
        <div class="wash-cell" style="margin-top:6px">
          <span class="ui-badge ui-badge--outline ${badgeClass}">${wash.status === "SAFE" ? "Safe" : "Risk"}</span>
          <div class="ui-muted wash-date">Sellable: ${String(wash.wash_safe_exit_date).slice(0, 10)}</div>
        </div>
      `;
    } else {
      washBlock.appendChild(el("div", { class: "ui-muted", html: "—" }));
    }

    container.replaceChildren(
      el("div", { class: "holdings-drilldown-panel ui-card" }, [
        header,
        summary,
        lotsBlock,
        optimizerBlock,
        taxBlock,
        washBlock,
      ]),
    );
  }

  function insertDetailsRow(afterTr, drillId) {
    const colCount = table.querySelectorAll("thead th").length || 1;
    const detailsTr = document.createElement("tr");
    detailsTr.className = "holdings-drilldown-row";
    const td = document.createElement("td");
    td.colSpan = colCount;
    const container = document.createElement("div");
    container.id = drillId;
    container.tabIndex = -1;
    container.className = "holdings-drilldown-container";
    container.innerHTML = `<div class="ui-muted">Loading…</div>`;
    td.appendChild(container);
    detailsTr.appendChild(td);
    afterTr.insertAdjacentElement("afterend", detailsTr);
    return { detailsTr, container };
  }

  async function loadPayload(scope, accountId, symbol) {
    const key = `${scope}|${accountId}|${symbol}`;
    if (cache.has(key)) return cache.get(key);
    const url = `/holdings/drilldown.json?scope=${encodeURIComponent(scope)}&account_id=${encodeURIComponent(accountId)}&symbol=${encodeURIComponent(symbol)}`;
    const res = await fetch(url, { headers: { Accept: "application/json" } });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || `HTTP ${res.status}`);
    }
    const payload = await res.json();
    cache.set(key, payload);
    return payload;
  }

  root.querySelectorAll("button[data-holding-toggle]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const tr = btn.closest("tr");
      if (!tr) return;
      const expanded = btn.getAttribute("aria-expanded") === "true";
      if (expanded) {
        closeOpen();
        return;
      }
      if (open) closeOpen();

      const drillId = btn.getAttribute("data-drilldown-id") || `holding-drilldown-${Math.random().toString(16).slice(2)}`;
      btn.setAttribute("aria-expanded", "true");
      const icon = btn.querySelector(".holdings-row-toggle__icon");
      if (icon) icon.textContent = "▾";

      const { detailsTr, container } = insertDetailsRow(tr, drillId);
      open = { btn, tr, detailsTr };

      const scope = btn.getAttribute("data-scope") || "household";
      const accountId = btn.getAttribute("data-account-id") || "";
      const symbol = btn.getAttribute("data-symbol") || "";
      try {
        const payload = await loadPayload(scope, accountId, symbol);
        renderPanel(container, payload);
        container.focus?.();
      } catch (e) {
        const msg = e && e.message ? e.message : String(e || "Failed to load.");
        container.innerHTML = `<div class="alert alert--warn" role="alert">Failed to load lots: ${msg}</div>`;
      }
    });
  });
})();
