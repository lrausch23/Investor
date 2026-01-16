(() => {
  const btn = document.querySelector("[data-plaid-connect]");
  if (!btn) return;

  const connId = btn.getAttribute("data-connection-id");
  if (!connId) return;

  const errBox = document.querySelector("[data-plaid-error]");
  const okBox = document.querySelector("[data-plaid-ok]");

  const setError = (msg) => {
    if (!errBox) return;
    errBox.textContent = msg || "Plaid linking failed.";
    errBox.hidden = false;
    if (okBox) okBox.hidden = true;
  };

  const setOk = (msg) => {
    if (!okBox) return;
    okBox.textContent = msg || "Connected.";
    okBox.hidden = false;
    if (errBox) errBox.hidden = true;
  };

  const fetchJson = async (url, body) => {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : "{}",
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok || data.ok === false) {
      throw new Error(data.error || `HTTP ${resp.status}`);
    }
    return data;
  };

  let handler = null;

  const openLink = async ({ receivedRedirectUri, linkToken }) => {
    let token = linkToken;
    if (!token) {
      const tokenResp = await fetchJson(`/sync/connections/${connId}/plaid/link_token`, {});
      token = tokenResp.link_token;
    }
    if (!token) throw new Error("Missing link_token.");

    handler = Plaid.create({
      token,
      receivedRedirectUri,
      onSuccess: async (public_token) => {
        try {
          await fetchJson(`/sync/connections/${connId}/plaid/exchange_public_token`, { public_token });
          setOk("Connected. You can run Sync now.");
          // Reload to show updated masked token / item id.
          window.location.href = `/sync/connections/${connId}?ok=Connected%20via%20Plaid`;
        } catch (e) {
          setError(String(e && e.message ? e.message : e));
        }
      },
      onExit: (err) => {
        if (err) setError(err.display_message || err.error_message || err.error_code || "Plaid Link exited.");
      },
    });

    handler.open();
  };

  // OAuth: when returning from Plaid, re-open with receivedRedirectUri.
  const maybeResumeOAuth = async () => {
    const href = window.location.href || "";
    if (href.includes("oauth_state_id=")) {
      try {
        const u = new URL(href);
        const linkToken = u.searchParams.get("link_token");
        if (!linkToken) {
          throw new Error("OAuth return missing link_token. Try connecting again.");
        }
        await openLink({ receivedRedirectUri: href, linkToken });
      } catch (e) {
        setError(String(e && e.message ? e.message : e));
      }
    }
  };

  btn.addEventListener("click", async () => {
    btn.disabled = true;
    try {
      await openLink({ receivedRedirectUri: undefined, linkToken: null });
    } catch (e) {
      setError(String(e && e.message ? e.message : e));
    } finally {
      btn.disabled = false;
    }
  });

  maybeResumeOAuth();
})();
