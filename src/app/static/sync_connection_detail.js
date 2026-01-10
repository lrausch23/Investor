(() => {
  const root = document.querySelector("[data-sync-connection-detail]");
  if (!root) return;

  const setSectionEnabled = (section, enabled) => {
    if (!section) return;
    section.hidden = !enabled;
    const inputs = section.querySelectorAll("input, select, textarea, button");
    inputs.forEach((el) => {
      if (!(el instanceof HTMLInputElement || el instanceof HTMLSelectElement || el instanceof HTMLTextAreaElement || el instanceof HTMLButtonElement)) {
        return;
      }
      if (el instanceof HTMLInputElement && el.type === "hidden") return;
      // Never override explicit disabled attrs (e.g., "end date ignored").
      if (el.hasAttribute("data-keep-disabled")) return;

      if (!enabled) {
        if (!el.disabled) {
          el.disabled = true;
          el.setAttribute("data-disabled-by-section", "1");
        }
        return;
      }

      if (el.getAttribute("data-disabled-by-section") === "1") {
        el.disabled = false;
        el.removeAttribute("data-disabled-by-section");
      }
    });
  };

  const initDisableConfirm = () => {
    const forms = Array.from(root.querySelectorAll("form[data-confirm-disable]"));
    forms.forEach((form) => {
      form.addEventListener("submit", (e) => {
        const name = form.getAttribute("data-conn-name") || "this connection";
        const ok = window.confirm(`Disable connection "${name}"?`);
        if (!ok) e.preventDefault();
      });
    });
  };

  const initDropdowns = () => {
    const dropdowns = Array.from(root.querySelectorAll("details.dropdown"));
    if (dropdowns.length === 0) return;

    const syncAria = (d) => {
      const summary = d.querySelector("summary");
      if (!summary) return;
      summary.setAttribute("aria-haspopup", "menu");
      summary.setAttribute("aria-expanded", d.open ? "true" : "false");
    };

    const closeAll = (except) => {
      dropdowns.forEach((d) => {
        if (except && d === except) return;
        if (d.open) d.open = false;
        syncAria(d);
      });
    };

    dropdowns.forEach((d) => {
      syncAria(d);
      d.addEventListener("toggle", () => {
        if (d.open) closeAll(d);
        syncAria(d);
      });
    });

    document.addEventListener(
      "click",
      (e) => {
        const target = e.target;
        if (!(target instanceof Element)) return;
        const inside = target.closest("details.dropdown");
        if (inside) return;
        closeAll(null);
      },
      { capture: true }
    );

    document.addEventListener("keydown", (e) => {
      if (e.key !== "Escape") return;
      closeAll(null);
    });
  };

  const initSyncModeToggles = () => {
    const selects = Array.from(root.querySelectorAll("[data-sync-mode-select]"));
    selects.forEach((sel) => {
      const apply = () => {
        const connId = sel.getAttribute("data-conn-id");
        if (!connId) return;
        const mode = (sel.value || "").toUpperCase();
        const full = root.querySelector(`#full_fields_${connId}`);
        const inc = root.querySelector(`#inc_fields_${connId}`);
        const payloads = root.querySelector(`#payloads_${connId}`);
        const isFull = mode === "FULL";

        setSectionEnabled(full, isFull);
        setSectionEnabled(inc, !isFull);

        if (payloads) {
          payloads.disabled = isFull;
          payloads.checked = isFull ? true : false;
        }
      };
      sel.addEventListener("change", apply);
      apply();
    });
  };

  // Mark explicitly disabled inputs so section toggles don't re-enable them.
  root.querySelectorAll("input[disabled], select[disabled], textarea[disabled]").forEach((el) => {
    el.setAttribute("data-keep-disabled", "1");
  });

  initDisableConfirm();
  initDropdowns();
  initSyncModeToggles();
})();
