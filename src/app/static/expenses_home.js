(() => {
  const root = document.querySelector("[data-expenses-home]");
  if (!root) return;

  const filesInput = root.querySelector("#expenses-files");
  const folderInput = root.querySelector("#expenses-folder");
  const uploadHint = root.querySelector("#expenses-upload-hint");

  const setUploadHint = (text) => {
    if (!uploadHint) return;
    uploadHint.textContent = text;
  };

  const updateUploadState = () => {
    const filesCount = filesInput && filesInput.files ? filesInput.files.length : 0;
    const folderCount = folderInput && folderInput.files ? folderInput.files.length : 0;

    if (filesInput && folderInput) {
      if (filesCount > 0) {
        folderInput.disabled = true;
        setUploadHint(`Selected ${filesCount} file(s). Folder upload is disabled.`);
      } else if (folderCount > 0) {
        filesInput.disabled = true;
        setUploadHint(`Selected ${folderCount} file(s) from folder. File upload is disabled.`);
      } else {
        filesInput.disabled = false;
        folderInput.disabled = false;
        setUploadHint("Choose files or a folder. For unknown formats, the importer will tell you what needs mapping.");
      }
    }
  };

  if (filesInput && folderInput) {
    filesInput.addEventListener("change", () => {
      // Enforce mutual exclusion: choosing files clears any folder selection.
      if (filesInput.files && filesInput.files.length > 0) {
        try {
          folderInput.value = "";
        } catch {
          // ignore
        }
      }
      updateUploadState();
    });
    folderInput.addEventListener("change", () => {
      // Enforce mutual exclusion: choosing a folder clears any file selection.
      if (folderInput.files && folderInput.files.length > 0) {
        try {
          filesInput.value = "";
        } catch {
          // ignore
        }
      }
      updateUploadState();
    });
    updateUploadState();
  }

  const batchFilter = root.querySelector("#expenses-batch-filter");
  if (batchFilter) {
    const rows = Array.from(root.querySelectorAll(".expenses-batch-row"));
    const apply = () => {
      const q = (batchFilter.value || "").trim().toLowerCase();
      let shown = 0;
      for (const r of rows) {
        const hay = (r.getAttribute("data-search") || "").toLowerCase();
        const ok = !q || hay.includes(q);
        r.style.display = ok ? "" : "none";
        if (ok) shown += 1;
      }
      batchFilter.setAttribute("aria-label", `Filter import batches (${shown} shown)`);
    };
    batchFilter.addEventListener("input", apply);
    apply();
  }

  for (const el of Array.from(root.querySelectorAll("[data-expenses-confirm-nav]"))) {
    el.addEventListener("click", () => {
      const href = el.getAttribute("data-expenses-confirm-nav-href") || "";
      const text = el.getAttribute("data-expenses-confirm-nav-text") || "Continue?";
      if (!href) return;
      if (window.confirm(text)) {
        window.location.assign(href);
      }
    });
  }
})();

