(() => {
  const root = document.querySelector("[data-docs-root]");
  if (!root) return;

  const searchInput = root.querySelector("#docs-search");
  const navLinks = Array.from(root.querySelectorAll("[data-doc-link]"));
  const sections = Array.from(root.querySelectorAll("[data-doc-section]"));
  const currentLabel = root.querySelector("[data-docs-current]");
  const toggleButton = root.querySelector("[data-docs-toggle]");
  const sidebar = root.querySelector("[data-docs-sidebar]");
  const nav = root.querySelector("#docs-nav");
  const uatState = {};

  const sectionMap = new Map(sections.map((section) => [section.id, section]));

  function setCurrent(id) {
    const currentSection = sectionMap.get(id);
    const title = currentSection?.dataset.docTitle || "Platform Overview";
    if (currentLabel) currentLabel.textContent = title;
    navLinks.forEach((link) => {
      const active = link.getAttribute("href") === `#${id}`;
      link.classList.toggle("is-active", active);
      link.setAttribute("aria-current", active ? "location" : "false");
    });
  }

  function applySearch() {
    const query = (searchInput?.value || "").trim().toLowerCase();
    navLinks.forEach((link) => {
      const matches = !query || link.textContent.toLowerCase().includes(query);
      link.classList.toggle("is-hidden", !matches);
    });
  }

  function syncFromHash() {
    const id = window.location.hash.replace(/^#/, "") || sections[0]?.id;
    if (!id || !sectionMap.has(id)) return;
    setCurrent(id);
    const section = sectionMap.get(id);
    if (section && window.location.hash) {
      section.scrollIntoView({ block: "start", behavior: "smooth" });
    }
  }

  function initUATChecklist() {
    const steps = document.querySelectorAll(".uat-step__check");
    const progressText = document.getElementById("uatProgressText");
    const progressFill = document.getElementById("uatProgressFill");
    if (!steps.length) return;

    function updateProgress() {
      const total = steps.length;
      const checked = Object.values(uatState).filter(Boolean).length;
      if (progressText) progressText.textContent = `${checked} of ${total} steps completed`;
      if (progressFill) progressFill.style.width = `${(checked / total) * 100}%`;
    }

    steps.forEach((checkbox) => {
      const stepId = checkbox.getAttribute("data-uat-step");
      checkbox.checked = !!uatState[stepId];
      checkbox.closest(".uat-step")?.classList.toggle("uat-step--passed", checkbox.checked);
      checkbox.addEventListener("change", () => {
        uatState[stepId] = checkbox.checked;
        checkbox.closest(".uat-step")?.classList.toggle("uat-step--passed", checkbox.checked);
        updateProgress();
      });
    });

    const resetBtn = document.getElementById("uatReset");
    if (resetBtn) {
      resetBtn.addEventListener("click", () => {
        Object.keys(uatState).forEach((key) => delete uatState[key]);
        steps.forEach((checkbox) => {
          checkbox.checked = false;
          checkbox.closest(".uat-step")?.classList.remove("uat-step--passed");
        });
        updateProgress();
      });
    }

    updateProgress();
  }

  const observer = new IntersectionObserver(
    (entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];
      if (!visible) return;
      setCurrent(visible.target.id);
      if (window.location.hash !== `#${visible.target.id}`) {
        history.replaceState(null, "", `#${visible.target.id}`);
      }
    },
    { rootMargin: "-20% 0px -60% 0px", threshold: [0.15, 0.4, 0.7] },
  );

  sections.forEach((section) => observer.observe(section));

  navLinks.forEach((link) => {
    link.addEventListener("click", () => {
      if (window.innerWidth <= 1024 && sidebar) {
        sidebar.classList.remove("is-open");
        toggleButton?.setAttribute("aria-expanded", "false");
      }
    });
  });

  if (searchInput) searchInput.addEventListener("input", applySearch);

  if (toggleButton && sidebar && nav) {
    toggleButton.addEventListener("click", () => {
      const open = !sidebar.classList.contains("is-open");
      sidebar.classList.toggle("is-open", open);
      toggleButton.setAttribute("aria-expanded", open ? "true" : "false");
    });
  }

  applySearch();
  syncFromHash();
  initUATChecklist();
  window.addEventListener("hashchange", syncFromHash);
})();
