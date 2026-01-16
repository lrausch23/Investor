(() => {
  function byId(id) {
    return document.getElementById(id);
  }

  function setDisplay(el, on) {
    if (!el) return;
    el.style.display = on ? "" : "none";
  }

  function updateControlsVisibility() {
    const universeSel = byId("momUniverseSel");
    const customWrap = byId("momCustomListWrap");
    if (universeSel && customWrap) {
      setDisplay(customWrap, String(universeSel.value || "") === "custom");
    }
    const periodSel = byId("momPeriodSel");
    const datesWrap = byId("momCustomDatesWrap");
    if (periodSel && datesWrap) {
      setDisplay(datesWrap, String(periodSel.value || "") === "custom");
    }
  }

  function wireRunButtonLoading() {
    document.querySelectorAll("form[data-mom-controls]").forEach((form) => {
      const btn = form.querySelector("[data-mom-run]");
      if (!btn) return;
      form.addEventListener("submit", () => {
        try {
          btn.disabled = true;
          btn.textContent = "Loading…";
          form.setAttribute("aria-busy", "true");
        } catch {
          // ignore
        }
      });
    });
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
    if (!td) return "";
    return (td.textContent || "").trim().toLowerCase();
  }

  function wireSortTables() {
    document.querySelectorAll("[data-mom-sort-table]").forEach((wrap) => {
      const table = wrap.querySelector("table");
      if (!table) return;
      const thead = table.querySelector("thead");
      const tbody = table.querySelector("tbody");
      if (!thead || !tbody) return;
      const ths = Array.from(thead.querySelectorAll("th"));
      const rows = () => Array.from(tbody.querySelectorAll("tr"));
      let state = { col: -1, dir: "desc" };

      ths.forEach((th, idx) => {
        const typ = th.getAttribute("data-sort");
        if (!typ) return;
        th.style.cursor = "pointer";
        th.setAttribute("title", "Click to sort");
        th.addEventListener("click", () => {
          const dir = state.col === idx && state.dir === "desc" ? "asc" : "desc";
          state = { col: idx, dir };
          const rs = rows();
          const isNum = typ === "num";
          rs.sort((a, b) => {
            const aTd = a.children[idx];
            const bTd = b.children[idx];
            if (isNum) {
              const av = numFromCell(aTd);
              const bv = numFromCell(bTd);
              const aa = Number.isFinite(av) ? av : -Infinity;
              const bb = Number.isFinite(bv) ? bv : -Infinity;
              if (aa === bb) return textFromCell(a.children[0]).localeCompare(textFromCell(b.children[0]));
              return aa < bb ? -1 : 1;
            }
            const at = textFromCell(aTd);
            const bt = textFromCell(bTd);
            if (at === bt) return 0;
            return at < bt ? -1 : 1;
          });
          if (dir === "desc") rs.reverse();
          for (const r of rs) tbody.appendChild(r);
        });
      });
    });
  }

  function wireSearch() {
    const input = byId("momSearchInput");
    if (!input) return;
    const tableWrap = document.querySelector('[data-mom-table="stocks"]');
    if (!tableWrap) return;
    const rows = Array.from(tableWrap.querySelectorAll("tbody tr[data-mom-row]"));
    const apply = () => {
      const q = String(input.value || "").trim().toLowerCase();
      let shown = 0;
      for (const r of rows) {
        const hay = (r.getAttribute("data-search") || "").toLowerCase();
        const ok = !q || hay.includes(q);
        r.style.display = ok ? "" : "none";
        if (ok) shown += 1;
      }
      input.setAttribute("aria-label", `Search stocks (${shown} shown)`);
    };
    input.addEventListener("input", apply);
    apply();
  }

  function parseChartData() {
    const el = byId("momSectorChartData");
    if (!el) return null;
    try {
      return JSON.parse(el.textContent || "{}");
    } catch {
      return null;
    }
  }

  function binarySearchOnOrBefore(points, targetDateStr) {
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

  function clamp(n, lo, hi) {
    return Math.max(lo, Math.min(hi, n));
  }

  function fmtDate(d) {
    return String(d || "").slice(0, 10);
  }

  function fmtVal(v) {
    if (!Number.isFinite(v)) return "—";
    return v.toFixed(4) + "×";
  }

  function renderSectorChart() {
    const svg = byId("momSectorChart");
    if (!svg) return;
    const data = parseChartData();
    if (!data) return;

    const sPts = (data.sector && data.sector.curve) || [];
    const bPts = (data.benchmark && data.benchmark.curve) || [];
    if (!Array.isArray(sPts) || sPts.length < 2) {
      svg.setAttribute("viewBox", "0 0 1000 260");
      svg.innerHTML =
        '<text x="0" y="18" fill="#6b7280" font-size="14">Chart unavailable (need at least 2 points).</text>';
      return;
    }

    const dates = sPts.map((p) => String(p[0] || "").slice(0, 10));
    const sVals = dates.map((d, i) => Number(sPts[i] && sPts[i][1]));
    const bVals = dates.map((d) => {
      const hit = binarySearchOnOrBefore(bPts, d);
      return hit ? Number(hit[1]) : NaN;
    });

    const W = 1000;
    const H = 260;
    const pad = { l: 48, r: 14, t: 14, b: 26 };
    const iw = W - pad.l - pad.r;
    const ih = H - pad.t - pad.b;

    const all = sVals.concat(bVals).filter((v) => Number.isFinite(v));
    const minY = Math.min(...all);
    const maxY = Math.max(...all);
    const y0 = minY === maxY ? minY * 0.98 : minY;
    const y1 = minY === maxY ? maxY * 1.02 : maxY;

    const xScale = (i) => pad.l + (i / Math.max(1, dates.length - 1)) * iw;
    const yScale = (v) => pad.t + (1 - (v - y0) / Math.max(1e-9, y1 - y0)) * ih;

    function linePath(values) {
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

    const sectorName = (data.sector && data.sector.name) || "Sector";
    const benchName = (data.benchmark && data.benchmark.name) || "Benchmark";

    svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
    svg.innerHTML = `
      <rect x="0" y="0" width="${W}" height="${H}" fill="#fff" />
      <g>
        <line x1="${pad.l}" y1="${pad.t + ih}" x2="${pad.l + iw}" y2="${pad.t + ih}" stroke="#e5e7eb" />
        <line x1="${pad.l}" y1="${pad.t}" x2="${pad.l}" y2="${pad.t + ih}" stroke="#e5e7eb" />
      </g>
      <path d="${linePath(bVals)}" fill="none" stroke="#6b7280" stroke-width="2" opacity="0.85" />
      <path d="${linePath(sVals)}" fill="none" stroke="#111827" stroke-width="2.5" />
      <line id="momXhair" x1="0" y1="0" x2="0" y2="0" stroke="#94a3b8" stroke-dasharray="4 3" opacity="0" />
      <circle id="momPtSector" r="4" fill="#111827" opacity="0" />
      <circle id="momPtBench" r="4" fill="#6b7280" opacity="0" />
    `;

    const tooltip = byId("momSectorChartTooltip");
    const legend = byId("momSectorChartLegend");
    if (legend) {
      legend.textContent = `${sectorName} vs ${benchName}`;
    }

    function showAt(idx) {
      const x = xScale(idx);
      const sV = sVals[idx];
      const bV = bVals[idx];
      const xhair = byId("momXhair");
      const ptS = byId("momPtSector");
      const ptB = byId("momPtBench");
      if (xhair) {
        xhair.setAttribute("x1", String(x));
        xhair.setAttribute("x2", String(x));
        xhair.setAttribute("y1", String(pad.t));
        xhair.setAttribute("y2", String(pad.t + ih));
        xhair.setAttribute("opacity", "1");
      }
      if (ptS) {
        ptS.setAttribute("cx", String(x));
        ptS.setAttribute("cy", String(yScale(sV)));
        ptS.setAttribute("opacity", Number.isFinite(sV) ? "1" : "0");
      }
      if (ptB) {
        ptB.setAttribute("cx", String(x));
        ptB.setAttribute("cy", String(yScale(bV)));
        ptB.setAttribute("opacity", Number.isFinite(bV) ? "1" : "0");
      }
      if (legend) {
        legend.textContent = `${fmtDate(dates[idx])} • ${sectorName}: ${fmtVal(sV)} • ${benchName}: ${fmtVal(bV)}`;
      }
      if (tooltip) {
        tooltip.style.display = "";
        tooltip.innerHTML = `
          <div class="perf-chart-tooltip__date">${fmtDate(dates[idx])}</div>
          <div class="perf-chart-tooltip__row"><span class="perf-chart-tooltip__k">${sectorName}</span><span class="perf-chart-tooltip__v">${fmtVal(sV)}</span></div>
          <div class="perf-chart-tooltip__row"><span class="perf-chart-tooltip__k">${benchName}</span><span class="perf-chart-tooltip__v">${fmtVal(bV)}</span></div>
        `;
      }
    }

    function hide() {
      const xhair = byId("momXhair");
      const ptS = byId("momPtSector");
      const ptB = byId("momPtBench");
      if (xhair) xhair.setAttribute("opacity", "0");
      if (ptS) ptS.setAttribute("opacity", "0");
      if (ptB) ptB.setAttribute("opacity", "0");
      if (tooltip) tooltip.style.display = "none";
      if (legend) legend.textContent = `${sectorName} vs ${benchName}`;
    }

    function onMove(ev) {
      const rect = svg.getBoundingClientRect();
      const x = ev.clientX - rect.left;
      const t = clamp((x - pad.l) / Math.max(1, iw), 0, 1);
      const idx = Math.round(t * (dates.length - 1));
      showAt(idx);
      if (tooltip) {
        const ttW = tooltip.offsetWidth || 320;
        const ttH = tooltip.offsetHeight || 72;
        const left = clamp(ev.clientX - rect.left + 12, 8, rect.width - ttW - 8);
        const top = clamp(ev.clientY - rect.top - ttH - 8, 8, rect.height - ttH - 8);
        tooltip.style.left = `${left}px`;
        tooltip.style.top = `${top}px`;
      }
    }

    svg.addEventListener("mousemove", onMove);
    svg.addEventListener("mouseleave", hide);
    hide();
  }

  // init
  const universeSel = byId("momUniverseSel");
  const periodSel = byId("momPeriodSel");
  if (universeSel) universeSel.addEventListener("change", updateControlsVisibility);
  if (periodSel) periodSel.addEventListener("change", updateControlsVisibility);
  updateControlsVisibility();
  wireRunButtonLoading();
  wireSortTables();
  wireSearch();
  renderSectorChart();
})();

