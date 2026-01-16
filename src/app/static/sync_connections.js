(() => {
  const root = document.querySelector("[data-sync-connections]");
  if (!root) return;

  const setSectionEnabled = (el, enabled) => {
    if (!el) return;
    el.hidden = !enabled;
    const inputs = el.querySelectorAll("input, select, textarea, button");
    inputs.forEach((i) => {
      if (i.closest("[data-create-section]") === el) {
        i.disabled = !enabled && i.getAttribute("type") !== "hidden";
      }
    });
  };

  const initCreateForm = () => {
    const kindSel = root.querySelector("[data-create-kind]");
    if (!kindSel) return;
    const nameInput = root.querySelector("#create_name");
    const secIb = root.querySelector('[data-create-section="IB_FLEX_WEB"]');
    const secOffline = root.querySelector('[data-create-section="OFFLINE"]');
    const secPlaid = root.querySelector('[data-create-section="CHASE_PLAID"]');
    const secPlaidAmex = root.querySelector('[data-create-section="AMEX_PLAID"]');

    const suggestedNameForKind = (kind) => {
      if (kind === "CHASE_PLAID") return "Chase (Plaid)";
      if (kind === "AMEX_PLAID") return "Amex (Plaid)";
      if (kind === "CHASE_OFFLINE") return "Chase (Offline)";
      if (kind === "RJ_OFFLINE") return "RJ (Offline)";
      return "IB Flex (Web)";
    };

    const apply = () => {
      const kind = (kindSel.value || "").trim().toUpperCase();
      const isIb = kind === "IB_FLEX_WEB";
      const isPlaid = kind === "CHASE_PLAID" || kind === "AMEX_PLAID";
      setSectionEnabled(secIb, isIb);
      setSectionEnabled(secOffline, !isIb && !isPlaid);
      setSectionEnabled(secPlaid, kind === "CHASE_PLAID");
      setSectionEnabled(secPlaidAmex, kind === "AMEX_PLAID");

      if (nameInput) {
        const prevSuggested = nameInput.getAttribute("data-suggested-name") || "";
        const nextSuggested = suggestedNameForKind(kind);
        if (!nameInput.value || nameInput.value === prevSuggested) {
          nameInput.value = nextSuggested;
        }
        nameInput.setAttribute("data-suggested-name", nextSuggested);
      }
    };

    kindSel.addEventListener("change", apply);
    apply();
  };

  const initRowDetails = () => {
    const toggles = Array.from(root.querySelectorAll("[data-conn-toggle]"));
    const secondaryToggles = Array.from(root.querySelectorAll("[data-conn-toggle-secondary]"));
    const allToggles = toggles.concat(secondaryToggles);

    const closeAll = (exceptId) => {
      const rows = Array.from(root.querySelectorAll("[data-conn-row]"));
      rows.forEach((row) => {
        const id = row.getAttribute("data-conn-id");
        if (!id || (exceptId && id === exceptId)) return;
        const detailsRow = root.querySelector(`#conn_details_${id}`);
        if (detailsRow) detailsRow.hidden = true;
        const btn = row.querySelector("[data-conn-toggle]");
        if (btn) {
          btn.setAttribute("aria-expanded", "false");
          const icon = btn.querySelector(".holdings-row-toggle__icon");
          if (icon) icon.textContent = "▸";
        }
      });
    };

    const toggleRow = (btn) => {
      const controlsId = btn.getAttribute("aria-controls");
      if (!controlsId) return;
      const detailsRow = root.querySelector(`#${controlsId}`);
      if (!detailsRow) return;
      const connId = (controlsId || "").replace("conn_details_", "");
      const isOpen = !detailsRow.hidden;
      closeAll(connId);
      detailsRow.hidden = isOpen;

      const row = btn.closest("[data-conn-row]");
      if (row) {
        const primaryBtn = row.querySelector("[data-conn-toggle]");
        if (primaryBtn) {
          primaryBtn.setAttribute("aria-expanded", String(!isOpen));
          const icon = primaryBtn.querySelector(".holdings-row-toggle__icon");
          if (icon) icon.textContent = !isOpen ? "▾" : "▸";
        }
      }
    };

    allToggles.forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        toggleRow(btn);
      });
      btn.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          toggleRow(btn);
        }
      });
    });
  };

  const initDisableConfirm = () => {
    const forms = Array.from(root.querySelectorAll("form[data-confirm-disable]"));
    forms.forEach((form) => {
      form.addEventListener("submit", (e) => {
        const name = form.getAttribute("data-conn-name") || "this connection";
        const ok = window.confirm(`Disable connection "${name}"?`);
        if (!ok) {
          e.preventDefault();
        }
      });
    });
  };

  const initDeleteConfirm = () => {
    const forms = Array.from(root.querySelectorAll("form[data-confirm-delete]"));
    forms.forEach((form) => {
      form.addEventListener("submit", (e) => {
        const name = form.getAttribute("data-conn-name") || "this connection";
        const ok = window.confirm(
          `Delete connection "${name}"?\n\nThis removes the connection, its stored credentials (if any), and its sync run history. It does not delete already-imported transactions/holdings data.`
        );
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
    const apply = (sel) => {
      const connId = sel.getAttribute("data-conn-id");
      if (!connId) return;
      const mode = (sel.value || "").toUpperCase();
      const full = root.querySelector(`#full_fields_${connId}`);
      const inc = root.querySelector(`#inc_fields_${connId}`);
      const payloads = root.querySelector(`#payloads_${connId}`);
      const isFull = mode === "FULL";
      if (full) full.hidden = !isFull;
      if (inc) inc.hidden = isFull;
      if (payloads) {
        payloads.disabled = isFull;
        payloads.checked = isFull ? true : false;
      }
    };
    selects.forEach((sel) => {
      sel.addEventListener("change", () => apply(sel));
      apply(sel);
    });
  };

  initCreateForm();
  initRowDetails();
  initDisableConfirm();
  initDeleteConfirm();
  initDropdowns();
  initSyncModeToggles();
})();
