(() => {
  function byId(id) {
    return document.getElementById(id);
  }

  function setDisplay(el, on) {
    if (!el) return;
    el.style.display = on ? "" : "none";
  }

  function updateDateControls() {
    const sel = byId("perfPeriodSel");
    const yearWrap = byId("perfYearWrap");
    const customWrap = byId("perfCustomWrap");
    if (!sel) return;
    const v = String(sel.value || "").toLowerCase();
    setDisplay(yearWrap, v === "year");
    setDisplay(customWrap, v === "custom");
  }

  function parseChartData() {
    const el = byId("perfChartData");
    if (!el) return null;
    try {
      return JSON.parse(el.textContent || "{}");
    } catch {
      return null;
    }
  }

  function isOffscreen(el) {
    const r = el.getBoundingClientRect();
    return r.bottom < 0 || r.top > window.innerHeight;
  }

  function fmtDate(d) {
    return String(d || "").slice(0, 10);
  }

  function clamp(n, lo, hi) {
    return Math.max(lo, Math.min(hi, n));
  }

  function fmtVal(v) {
    if (!Number.isFinite(v)) return "—";
    return v.toFixed(4) + "×";
  }

  function fmtPctFrom1(v) {
    if (!Number.isFinite(v)) return "—";
    return ((v - 1.0) * 100.0).toFixed(2) + "%";
  }

  function fmtUsd(v) {
    if (!Number.isFinite(v)) return "—";
    const sign = v < 0 ? "-" : "";
    const abs = Math.abs(v);
    return (
      sign +
      "$" +
      abs.toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })
    );
  }

  function drawdownSeries(values) {
    let peak = null;
    return values.map((v) => {
      if (!Number.isFinite(v)) return NaN;
      if (peak === null || v > peak) peak = v;
      if (!peak || peak <= 0) return NaN;
      return v / peak - 1.0;
    });
  }

  function rollingVolFromGrowth(values, windowSize, periodsPerYear) {
    // values: growth curve (1.0 baseline). Returns a vol series aligned to dates (NaN until enough points).
    const rets = [];
    for (let i = 1; i < values.length; i++) {
      const v0 = values[i - 1];
      const v1 = values[i];
      if (!Number.isFinite(v0) || !Number.isFinite(v1) || v0 === 0) {
        rets.push(NaN);
      } else {
        rets.push(v1 / v0 - 1.0);
      }
    }
    const out = new Array(values.length).fill(NaN);
    function std(xs) {
      const ys = xs.filter((x) => Number.isFinite(x));
      if (ys.length < 2) return NaN;
      const m = ys.reduce((a, b) => a + b, 0) / ys.length;
      const v = ys.reduce((a, b) => a + (b - m) * (b - m), 0) / (ys.length - 1);
      return Math.sqrt(v);
    }
    for (let i = windowSize; i <= rets.length; i++) {
      const w = rets.slice(i - windowSize, i);
      const s = std(w);
      if (Number.isFinite(s)) out[i] = s * Math.sqrt(periodsPerYear);
    }
    return out;
  }

  function binarySearchOnOrBefore(points, targetDateStr) {
    // points: [["YYYY-MM-DD", value], ...] sorted by date asc
    const t = String(targetDateStr || "").slice(0, 10);
    let lo = 0;
    let hi = points.length - 1;
    let best = null;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const d = String(points[mid][0] || "").slice(0, 10);
      if (d <= t) {
        best = points[mid];
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return best;
  }

  function linePathAligned(values, xScale, yScale) {
    if (!values || values.length === 0) return "";
    let d = "";
    let started = false;
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (!Number.isFinite(v)) {
        started = false;
        continue;
      }
      const x = xScale(i);
      const y = yScale(v);
      d += (started ? "L" : "M") + x.toFixed(2) + " " + y.toFixed(2) + " ";
      started = true;
    }
    return d.trim();
  }

  function maxDrawdownIndex(values) {
    // returns index of trough with minimum drawdown; null if insufficient.
    if (!values || values.length < 2) return null;
    let peak = null;
    let minDd = 0;
    let minIdx = null;
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (!Number.isFinite(v)) continue;
      if (peak === null || v > peak) peak = v;
      if (peak && peak > 0) {
        const dd = v / peak - 1.0;
        if (dd < minDd) {
          minDd = dd;
          minIdx = i;
        }
      }
    }
    return minIdx;
  }

  let perfChartMode = "growth";
  let perfShowEvents = true;

  function applyModeButtons() {
    document.querySelectorAll(".perf-mode-btn").forEach((btn) => {
      const mode = String(btn.getAttribute("data-perf-mode") || "");
      btn.setAttribute("aria-pressed", mode === perfChartMode ? "true" : "false");
    });
  }

  function renderChart() {
    const svg = byId("perfChart");
    if (!svg) return;
    const data = parseChartData();
    if (!data) return;

    const pCurve = (data.portfolio && data.portfolio.curve) || [];
    const bCurve = (data.benchmark && data.benchmark.curve) || [];
    // Format: [["YYYY-MM-DD", 1.0], ...]
    const pPts = Array.isArray(pCurve) ? pCurve : [];
    const bPts = Array.isArray(bCurve) ? bCurve : [];
    const freq = String(data.frequency || "month_end");
    const periodsPerYear = freq === "daily" ? 252.0 : 12.0;
    const volWindowSize = freq === "daily" ? 63 : 6;
    const volBtn = document.querySelector('.perf-mode-btn[data-perf-mode="vol"]');
    if (volBtn) {
      const ok = (pPts.length ? pPts.length : bPts.length) - 1 >= volWindowSize;
      volBtn.disabled = !ok;
      volBtn.title = ok ? "" : `Need at least ${volWindowSize} return observations for rolling volatility.`;
    }
    const n = pPts.length || bPts.length;
    if (n < 2) {
      svg.setAttribute("viewBox", "0 0 1000 260");
      svg.innerHTML =
        '<text x="0" y="18" fill="#6b7280" font-size="14">Chart unavailable (need at least 2 points).</text>';
      return;
    }

    // Align benchmark values to portfolio valuation dates (preferred) for inspection.
    const xDates = (pPts.length ? pPts : bPts).map((p) => String(p[0] || "").slice(0, 10));
    const pGrowth = xDates.map((d, i) => {
      if (pPts.length) return Number(pPts[i] && pPts[i][1]);
      const hit = binarySearchOnOrBefore(pPts, d);
      return hit ? Number(hit[1]) : NaN;
    });
    const bGrowth = xDates.map((d) => {
      const hit = binarySearchOnOrBefore(bPts, d);
      return hit ? Number(hit[1]) : NaN;
    });

    // Chart mode transforms (UI-only; derived from existing growth series).
    let mode = perfChartMode || "growth";
    let pVals = pGrowth.slice();
    let bVals = bGrowth.slice();
    let yLabelFmt = (v) => v.toFixed(2) + "×";
    let tooltipValFmt = fmtVal;
    let tooltipExtraFmt = fmtPctFrom1;
    let yTitle = "×";

    if (mode === "drawdown") {
      pVals = drawdownSeries(pGrowth);
      bVals = drawdownSeries(bGrowth);
      yLabelFmt = (v) => (v * 100.0).toFixed(0) + "%";
      tooltipValFmt = (v) => (Number.isFinite(v) ? (v * 100.0).toFixed(2) + "%" : "—");
      tooltipExtraFmt = () => "";
      yTitle = "%";
    } else if (mode === "vol") {
      const windowSize = volWindowSize;
      if (pGrowth.length - 1 < windowSize) {
        // Not enough observations: keep growth mode but inform via button disable.
        perfChartMode = "growth";
        mode = "growth";
        applyModeButtons();
      } else {
        pVals = rollingVolFromGrowth(pGrowth, windowSize, periodsPerYear);
        bVals = rollingVolFromGrowth(bGrowth, windowSize, periodsPerYear);
        yLabelFmt = (v) => (v * 100.0).toFixed(0) + "%";
        tooltipValFmt = (v) => (Number.isFinite(v) ? (v * 100.0).toFixed(2) + "%" : "—");
        tooltipExtraFmt = () => "";
        yTitle = "%";
      }
    }

    const yVals = [];
    pVals.forEach((v) => yVals.push(v));
    bVals.forEach((v) => yVals.push(v));
    const finite = yVals.filter((v) => Number.isFinite(v));
    const yMin = Math.min(...finite);
    const yMax = Math.max(...finite);
    const pad = (yMax - yMin) * 0.08 || 0.05;
    const minY = yMin - pad;
    const maxY = yMax + pad;

    const W = 1000;
    const H = 260;
    const m = { top: 18, right: 16, bottom: 32, left: 48 };
    const iw = W - m.left - m.right;
    const ih = H - m.top - m.bottom;

    const xScale = (i) => m.left + (iw * i) / Math.max(1, xDates.length - 1);
    const yScale = (v) => m.top + ih * (1 - (v - minY) / Math.max(1e-12, maxY - minY));

    const gridLines = 4;
    const grid = [];
    for (let i = 0; i <= gridLines; i++) {
      const y = m.top + (ih * i) / gridLines;
      grid.push(`<line x1="${m.left}" y1="${y}" x2="${W - m.right}" y2="${y}" stroke="#e5e7eb" stroke-width="1" />`);
    }

    const yLabels = [];
    for (let i = 0; i <= gridLines; i++) {
      const v = maxY - ((maxY - minY) * i) / gridLines;
      const y = m.top + (ih * i) / gridLines + 4;
      yLabels.push(
        `<text x="${m.left - 10}" y="${y}" text-anchor="end" fill="#6b7280" font-size="12">${yLabelFmt(v)}</text>`
      );
    }

    const xLabelLeft = fmtDate(xDates[0] || "");
    const xLabelRight = fmtDate(xDates[xDates.length - 1] || "");

    const pPath = linePathAligned(pVals, xScale, yScale);
    const bPath = linePathAligned(bVals, xScale, yScale);

    const portfolioLabel = (data.portfolio && data.portfolio.label) || "Portfolio";
    const benchLabel = (data.benchmark && data.benchmark.label) || "Benchmark";

    // Keep the SVG legend comfortably inside the viewBox to avoid text baseline clipping.
    const legendY = Math.max(14, m.top - 4);
    const legend = `
      <g id="perfLegend">
        <text id="perfLegendDate" x="${m.left}" y="${legendY - 4}" fill="#6b7280" font-size="12">Hover to inspect</text>
        <rect x="${m.left + 210}" y="${legendY - 8}" width="10" height="3" fill="#111827" />
        <text id="perfLegendP" x="${m.left + 226}" y="${legendY - 4}" fill="#111827" font-size="12">${portfolioLabel}</text>
        <rect x="${m.left + 520}" y="${legendY - 8}" width="10" height="3" fill="#2563eb" />
        <text id="perfLegendB" x="${m.left + 536}" y="${legendY - 4}" fill="#111827" font-size="12">${benchLabel}</text>
      </g>
    `;

    const startIdx = 0;
    const endIdx = xDates.length - 1;
    // Drawdown indices are computed from the growth curve (regardless of display mode).
    const pDdIdx = mode === "vol" ? null : maxDrawdownIndex(pGrowth);
    const bDdIdx = mode === "vol" ? null : maxDrawdownIndex(bGrowth);

    function marker(x, y, fill, stroke, title) {
      const t = title ? `<title>${title}</title>` : "";
      return `<circle cx="${x.toFixed(2)}" cy="${y.toFixed(2)}" r="4" fill="${fill}" stroke="${stroke}" stroke-width="2">${t}</circle>`;
    }

    svg.setAttribute("viewBox", `0 0 ${W} ${H}`);

    // Cashflow markers (net TRANSFER flows aligned to valuation dates on the server).
    const rawEvents = Array.isArray(data.events) ? data.events : [];
    const evByDate = new Map();
    rawEvents.forEach((e) => {
      const d = fmtDate(e && e.date);
      const amt = Number(e && e.amount);
      if (!d || !Number.isFinite(amt)) return;
      evByDate.set(d, (evByDate.get(d) || 0) + amt);
    });
    const eventsMarkup =
      perfShowEvents && evByDate.size
        ? `<g id="perfEvents" pointer-events="none">
            ${xDates
              .map((d, i) => {
                const amt = evByDate.get(d);
                if (!Number.isFinite(amt) || amt === 0) return "";
                const x = xScale(i);
                const y = m.top + ih - 8;
                const fill = amt > 0 ? "#16a34a" : "#f59e0b";
                const title = `${d} · ${amt > 0 ? "Deposit" : "Withdrawal"} ${fmtUsd(amt)}`;
                return `<circle cx="${x.toFixed(2)}" cy="${y.toFixed(2)}" r="4" fill="${fill}" stroke="#111827" stroke-width="1"><title>${title}</title></circle>`;
              })
              .join("")}
          </g>`
        : "";

    svg.innerHTML = `
      ${grid.join("")}
      ${yLabels.join("")}
      <line x1="${m.left}" y1="${m.top + ih}" x2="${W - m.right}" y2="${m.top + ih}" stroke="#d1d5db" stroke-width="1" />
      <text x="${m.left}" y="${H - 10}" fill="#6b7280" font-size="12">${xLabelLeft}</text>
      <text x="${W - m.right}" y="${H - 10}" text-anchor="end" fill="#6b7280" font-size="12">${xLabelRight}</text>
      ${legend}
      <line id="perfCrosshair" x1="${m.left}" y1="${m.top}" x2="${m.left}" y2="${m.top + ih}" stroke="#9ca3af" stroke-width="1" stroke-dasharray="3 3" style="display:none" />
      ${
        pPath
          ? `<path d="${pPath}" fill="none" stroke="#111827" stroke-width="2" vector-effect="non-scaling-stroke" />`
          : ""
      }
      ${
        bPath
          ? `<path d="${bPath}" fill="none" stroke="#2563eb" stroke-width="2" vector-effect="non-scaling-stroke" />`
          : ""
      }
      ${eventsMarkup}
      <g id="perfKeyPoints">
        ${marker(xScale(startIdx), yScale(pVals[startIdx]), "#fff", "#111827", `Start ${xDates[startIdx]} · ${portfolioLabel} ${tooltipValFmt(pVals[startIdx])}`)}
        ${marker(xScale(endIdx), yScale(pVals[endIdx]), "#fff", "#111827", `End ${xDates[endIdx]} · ${portfolioLabel} ${tooltipValFmt(pVals[endIdx])}`)}
        ${Number.isFinite(bVals[startIdx]) ? marker(xScale(startIdx), yScale(bVals[startIdx]), "#fff", "#2563eb", `Start ${xDates[startIdx]} · ${benchLabel} ${tooltipValFmt(bVals[startIdx])}`) : ""}
        ${Number.isFinite(bVals[endIdx]) ? marker(xScale(endIdx), yScale(bVals[endIdx]), "#fff", "#2563eb", `End ${xDates[endIdx]} · ${benchLabel} ${tooltipValFmt(bVals[endIdx])}`) : ""}
        ${
          pDdIdx !== null
            ? marker(xScale(pDdIdx), yScale(pVals[pDdIdx]), "#111827", "#111827", `Max drawdown (portfolio) ${xDates[pDdIdx]} · ${tooltipValFmt(pVals[pDdIdx])}`)
            : ""
        }
        ${
          bDdIdx !== null
            ? marker(xScale(bDdIdx), yScale(bVals[bDdIdx]), "#2563eb", "#2563eb", `Max drawdown (benchmark) ${xDates[bDdIdx]} · ${tooltipValFmt(bVals[bDdIdx])}`)
            : ""
        }
      </g>
      <circle id="perfDotP" cx="${m.left}" cy="${m.top}" r="5" fill="#111827" style="display:none" />
      <circle id="perfDotB" cx="${m.left}" cy="${m.top}" r="5" fill="#2563eb" style="display:none" />
      <rect id="perfHit" x="${m.left}" y="${m.top}" width="${iw}" height="${ih}" fill="transparent" />
    `;

    const tooltip = byId("perfChartTooltip");
    const legendEl = byId("perfChartLegend");
    const inspector = byId("perfChartInspector");

    function setLegend(i) {
      if (!legendEl) return;
      const d = xDates[i];
      const pv = pVals[i];
      const bv = bVals[i];
      const ex = Number.isFinite(pv) && Number.isFinite(bv) ? pv - bv : NaN;
      const flow = evByDate.get(d);
      const flowTxt = Number.isFinite(flow) && flow !== 0 ? ` · Cashflow ${flow >= 0 ? "+" : ""}${fmtUsd(flow)}` : "";
      if (mode === "growth") {
        legendEl.textContent =
          `${d} · Portfolio ${fmtVal(pv)} (${fmtPctFrom1(pv)}) · Benchmark ${fmtVal(bv)} (${fmtPctFrom1(bv)})` +
          (Number.isFinite(ex) ? ` · Excess ${ex >= 0 ? "+" : ""}${ex.toFixed(4)}×` : "") +
          flowTxt;
      } else {
        legendEl.textContent = `${d} · Portfolio ${tooltipValFmt(pv)} · Benchmark ${tooltipValFmt(bv)}` + flowTxt;
      }
    }

    function setSvgLegend(i) {
      const dText = svg.querySelector("#perfLegendDate");
      const pText = svg.querySelector("#perfLegendP");
      const bText = svg.querySelector("#perfLegendB");
      const d = xDates[i];
      const pv = pVals[i];
      const bv = bVals[i];
      if (dText) dText.textContent = d;
      if (pText) pText.textContent = `${portfolioLabel} ${fmtVal(pv)}`;
      if (bText) bText.textContent = `${benchLabel} ${fmtVal(bv)}`;
    }

    function showAtIndex(i, { scrollLots = false } = {}) {
      const idx = clamp(i, 0, xDates.length - 1);
      const x = xScale(idx);
      const pv = pVals[idx];
      const bv = bVals[idx];
      const cross = svg.querySelector("#perfCrosshair");
      const dotP = svg.querySelector("#perfDotP");
      const dotB = svg.querySelector("#perfDotB");
      if (cross) {
        cross.setAttribute("x1", String(x));
        cross.setAttribute("x2", String(x));
        cross.style.display = "";
      }
      if (dotP && Number.isFinite(pv)) {
        dotP.setAttribute("cx", String(x));
        dotP.setAttribute("cy", String(yScale(pv)));
        dotP.style.display = "";
      } else if (dotP) {
        dotP.style.display = "none";
      }
      if (dotB && Number.isFinite(bv)) {
        dotB.setAttribute("cx", String(x));
        dotB.setAttribute("cy", String(yScale(bv)));
        dotB.style.display = "";
      } else if (dotB) {
        dotB.style.display = "none";
      }

      setSvgLegend(idx);
      setLegend(idx);

      if (tooltip) {
        const ex = Number.isFinite(pv) && Number.isFinite(bv) ? pv - bv : NaN;
        const flow = evByDate.get(xDates[idx]);
        const flowRow =
          Number.isFinite(flow) && flow !== 0
            ? `<div class="perf-chart-tooltip__row"><span class="perf-chart-tooltip__k">Cashflow</span><span class="perf-chart-tooltip__v">${flow > 0 ? "Deposit" : "Withdrawal"} <span class="ui-muted">(${flow >= 0 ? "+" : ""}${fmtUsd(flow)})</span></span></div>`
            : "";
        if (mode === "growth") {
          tooltip.innerHTML =
            `<div class="perf-chart-tooltip__date">${xDates[idx]}</div>` +
            `<div class="perf-chart-tooltip__row"><span class="perf-chart-tooltip__k">${portfolioLabel}</span><span class="perf-chart-tooltip__v">${fmtVal(pv)} <span class="ui-muted">(${fmtPctFrom1(pv)})</span></span></div>` +
            `<div class="perf-chart-tooltip__row"><span class="perf-chart-tooltip__k">${benchLabel}</span><span class="perf-chart-tooltip__v">${fmtVal(bv)} <span class="ui-muted">(${fmtPctFrom1(bv)})</span></span></div>` +
            (Number.isFinite(ex)
              ? `<div class="perf-chart-tooltip__row"><span class="perf-chart-tooltip__k">Excess</span><span class="perf-chart-tooltip__v">${ex >= 0 ? "+" : ""}${ex.toFixed(4)}×</span></div>`
              : "") +
            flowRow;
        } else {
          tooltip.innerHTML =
            `<div class="perf-chart-tooltip__date">${xDates[idx]}</div>` +
            `<div class="perf-chart-tooltip__row"><span class="perf-chart-tooltip__k">${portfolioLabel}</span><span class="perf-chart-tooltip__v">${tooltipValFmt(pv)}</span></div>` +
            `<div class="perf-chart-tooltip__row"><span class="perf-chart-tooltip__k">${benchLabel}</span><span class="perf-chart-tooltip__v">${tooltipValFmt(bv)}</span></div>` +
            flowRow;
        }
        tooltip.style.display = "";
      }
      return idx;
    }

    function hideHover() {
      const cross = svg.querySelector("#perfCrosshair");
      const dotP = svg.querySelector("#perfDotP");
      const dotB = svg.querySelector("#perfDotB");
      if (cross) cross.style.display = "none";
      if (dotP) dotP.style.display = "none";
      if (dotB) dotB.style.display = "none";
      if (tooltip) tooltip.style.display = "none";
      const dText = svg.querySelector("#perfLegendDate");
      const pText = svg.querySelector("#perfLegendP");
      const bText = svg.querySelector("#perfLegendB");
      if (dText) dText.textContent = "Hover to inspect";
      if (pText) pText.textContent = portfolioLabel;
      if (bText) bText.textContent = benchLabel;
      if (legendEl) legendEl.textContent = "";
    }

    function positionTooltip(clientX, clientY) {
      if (!tooltip) return;
      const wrap = svg.closest(".perf-chart-wrap");
      if (!wrap) return;
      const r = wrap.getBoundingClientRect();
      const tw = tooltip.offsetWidth || 240;
      const th = tooltip.offsetHeight || 120;
      const padPx = 10;
      let left = clientX - r.left + padPx;
      let top = clientY - r.top + padPx;
      if (left + tw > r.width) left = clientX - r.left - tw - padPx;
      if (top + th > r.height) top = clientY - r.top - th - padPx;
      left = clamp(left, 6, Math.max(6, r.width - tw - 6));
      top = clamp(top, 6, Math.max(6, r.height - th - 6));
      tooltip.style.left = left + "px";
      tooltip.style.top = top + "px";
    }

    let pinnedIndex = null;

    function onMove(ev) {
      if (pinnedIndex !== null) return;
      const rect = svg.getBoundingClientRect();
      const x = ev.clientX - rect.left;
      const frac = clamp((x - (m.left * rect.width) / W) / ((iw * rect.width) / W), 0, 1);
      const idx = Math.round(frac * (xDates.length - 1));
      const shown = showAtIndex(idx);
      positionTooltip(ev.clientX, ev.clientY);
      return shown;
    }

    const hit = svg.querySelector("#perfHit");
    if (hit) {
      hit.addEventListener("pointermove", onMove);
      hit.addEventListener("pointerenter", (ev) => {
        pinnedIndex = null;
        onMove(ev);
      });
      hit.addEventListener("pointerleave", () => {
        if (pinnedIndex === null) hideHover();
      });
      hit.addEventListener("click", (ev) => {
        // Pin/unpin on click for easier inspection.
        if (pinnedIndex !== null) {
          pinnedIndex = null;
          onMove(ev);
        } else {
          const shown = onMove(ev);
          pinnedIndex = shown;
        }
      });
    }

    // Keyboard-friendly inspector
    if (inspector) {
      const id = "perfInspectorRange";
      inspector.innerHTML = `
        <div class="perf-chart-inspector__row">
          <label for="${id}"><b>Data inspector</b></label>
          <input id="${id}" type="range" min="0" max="${xDates.length - 1}" value="${xDates.length - 1}" step="1" />
          <button type="button" class="btn btn--secondary btn--sm" id="perfInspectorClear">Clear</button>
        </div>
        <div class="perf-chart-inspector__help">Use the slider to inspect exact values (also works without hover).</div>
      `;
      const range = byId(id);
      const clear = byId("perfInspectorClear");
      if (range) {
        range.addEventListener("input", () => {
          pinnedIndex = Number(range.value || 0);
          showAtIndex(pinnedIndex);
          // Position tooltip near the right edge by default.
          const wrap = svg.closest(".perf-chart-wrap");
          if (wrap && tooltip) {
            const r = wrap.getBoundingClientRect();
            tooltip.style.left = clamp(r.width - (tooltip.offsetWidth || 240) - 10, 6, r.width) + "px";
            tooltip.style.top = "10px";
            tooltip.style.display = "";
          }
        });
      }
      if (clear) {
        clear.addEventListener("click", () => {
          pinnedIndex = null;
          hideHover();
        });
      }
    }

    // Initialize legend with end point values (no hover).
    setLegend(endIdx);
  }

  updateDateControls();
  const sel = byId("perfPeriodSel");
  if (sel) sel.addEventListener("change", updateDateControls);

  const showEvents = byId("perfShowEvents");
  if (showEvents) {
    perfShowEvents = !!showEvents.checked;
    showEvents.addEventListener("change", () => {
      perfShowEvents = !!showEvents.checked;
      window.requestAnimationFrame(renderChart);
    });
  }
  document.querySelectorAll(".perf-mode-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      perfChartMode = String(btn.getAttribute("data-perf-mode") || "growth");
      applyModeButtons();
      window.requestAnimationFrame(renderChart);
    });
  });
  applyModeButtons();

  // Render chart after layout.
  window.requestAnimationFrame(renderChart);
})();
