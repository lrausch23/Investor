from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, request

from plaid.api import plaid_api
from plaid.configuration import Configuration
from plaid.model.country_code import CountryCode
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.products import Products
from plaid.model.webhook_type import WebhookType
from plaid.model.webhook_verification_key_get_request import WebhookVerificationKeyGetRequest
from plaid import ApiClient
from plaid.exceptions import ApiException


load_dotenv()


DB_PATH = Path(os.environ.get("PLAID_DB_PATH", "investments.db")).resolve()


def _db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con


def _db_init() -> None:
    with _db_connect() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS plaid_items (
              item_id TEXT PRIMARY KEY,
              access_token TEXT NOT NULL,
              institution_name TEXT,
              transactions_cursor TEXT NOT NULL DEFAULT '',
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_plaid_items_updated_at ON plaid_items(updated_at)")


def _db_upsert_item(*, item_id: str, access_token: str, institution_name: str | None = None) -> None:
    now = int(time.time())
    with _db_connect() as con:
        con.execute(
            """
            INSERT INTO plaid_items(item_id, access_token, institution_name, transactions_cursor, created_at, updated_at)
            VALUES (?, ?, ?, '', ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
              access_token=excluded.access_token,
              institution_name=COALESCE(excluded.institution_name, plaid_items.institution_name),
              updated_at=excluded.updated_at
            """,
            (item_id, access_token, institution_name, now, now),
        )


def _require_env(name: str) -> str:
    v = (os.environ.get(name) or "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _plaid_client() -> plaid_api.PlaidApi:
    env = (os.environ.get("PLAID_ENV") or "sandbox").strip().lower()
    if env not in {"sandbox", "development", "production"}:
        env = "sandbox"

    # Defaults match plaid-python docs.
    host_by_env = {
        "sandbox": "https://sandbox.plaid.com",
        "development": "https://development.plaid.com",
        "production": "https://production.plaid.com",
    }
    configuration = Configuration(
        host=host_by_env[env],
        api_key={
            "clientId": _require_env("PLAID_CLIENT_ID"),
            "secret": _require_env("PLAID_SECRET"),
        },
    )
    api_client = ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(16).hex())


@dataclass(frozen=True)
class LinkContext:
    link_token: str
    env: str
    received_redirect_uri: str | None


def _make_link_token(*, client: plaid_api.PlaidApi, received_redirect_uri: str | None) -> LinkContext:
    env = (os.environ.get("PLAID_ENV") or "sandbox").strip().lower()
    redirect_uri = (os.environ.get("PLAID_REDIRECT_URI") or "").strip() or None

    # Products: investments + transactions.
    req = LinkTokenCreateRequest(
        products=[Products("investments"), Products("transactions")],
        client_name="Investor (Plaid setup)",
        country_codes=[CountryCode("US")],
        language="en",
        user=LinkTokenCreateRequestUser(client_user_id=str(int(time.time()))),
        redirect_uri=redirect_uri,
    )
    resp = client.link_token_create(req)
    link_token = str(resp["link_token"])
    return LinkContext(link_token=link_token, env=env, received_redirect_uri=received_redirect_uri)


@app.get("/")
def index() -> str:
    _db_init()
    client = _plaid_client()
    # OAuth return to this page will include a "received_redirect_uri" param.
    received_redirect_uri = request.args.get("received_redirect_uri") or None
    ctx = _make_link_token(client=client, received_redirect_uri=received_redirect_uri)

    # Minimal HTML (no templates) with Plaid Link.
    return f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Investor · Plaid Setup</title>
    <script src="https://cdn.plaid.com/link/v2/stable/link-initialize.js"></script>
    <style>
      body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 32px; }}
      .card {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px; max-width: 720px; }}
      .muted {{ color: #6b7280; font-size: 13px; }}
      button {{ background: #111827; color: #fff; border: 0; padding: 10px 14px; border-radius: 10px; cursor: pointer; }}
      button:disabled {{ opacity: 0.6; cursor: not-allowed; }}
      code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 6px; }}
      .ok {{ margin-top: 10px; color: #166534; }}
      .err {{ margin-top: 10px; color: #991b1b; }}
    </style>
  </head>
  <body>
    <div class="card">
      <h1 style="margin:0 0 6px 0">Plaid one-time linker</h1>
      <div class="muted">
        Environment: <code>{ctx.env}</code><br/>
        This stores <b>access_token</b> + <b>item_id</b> into <code>{DB_PATH.name}</code> (table <code>plaid_items</code>). Keep that DB private.
      </div>
      <div style="margin-top:14px">
        <button id="link-btn">Connect Chase via Plaid</button>
        <div id="msg" class="muted" aria-live="polite" style="margin-top:10px"></div>
      </div>
    </div>

    <script>
      const linkToken = {ctx.link_token!r};
      const receivedRedirectUri = {ctx.received_redirect_uri!r};
      const msg = document.getElementById("msg");
      const btn = document.getElementById("link-btn");

      async function exchange(public_token, metadata) {{
        msg.textContent = "Exchanging token…";
        const res = await fetch("/exchange_public_token", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ public_token, metadata }}),
        }});
        const data = await res.json();
        if (!res.ok) {{
          msg.className = "err";
          msg.textContent = data.error || "Token exchange failed";
          return;
        }}
        msg.className = "ok";
        msg.textContent = "Saved. item_id=" + data.item_id;
      }}

      const handler = Plaid.create({{
        token: linkToken,
        receivedRedirectUri: receivedRedirectUri || undefined,
        onSuccess: function(public_token, metadata) {{
          exchange(public_token, metadata);
        }},
        onExit: function(err, metadata) {{
          if (err) {{
            msg.className = "err";
            msg.textContent = err.display_message || err.error_message || "Exited with error";
          }} else {{
            msg.className = "muted";
            msg.textContent = "Exited.";
          }}
        }},
      }});

      btn.addEventListener("click", () => handler.open());
    </script>
  </body>
</html>
    """.strip()


@app.get("/link_token")
def link_token() -> Any:
    _db_init()
    client = _plaid_client()
    ctx = _make_link_token(client=client, received_redirect_uri=None)
    return jsonify({"link_token": ctx.link_token, "env": ctx.env})


@app.post("/exchange_public_token")
def exchange_public_token() -> Any:
    _db_init()
    client = _plaid_client()
    payload = request.get_json(force=True, silent=True) or {}
    public_token = str(payload.get("public_token") or "").strip()
    if not public_token:
        return jsonify({"error": "Missing public_token"}), 400

    metadata = payload.get("metadata") or {}
    institution_name = None
    try:
        inst = metadata.get("institution") or {}
        institution_name = str(inst.get("name") or "").strip() or None
    except Exception:
        institution_name = None

    try:
        req = ItemPublicTokenExchangeRequest(public_token=public_token)
        resp = client.item_public_token_exchange(req)
        access_token = str(resp["access_token"])
        item_id = str(resp["item_id"])
    except ApiException as e:
        return jsonify({"error": f"Plaid API error: {e}"}), 400
    except Exception as e:
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 400

    _db_upsert_item(item_id=item_id, access_token=access_token, institution_name=institution_name)
    return jsonify({"item_id": item_id, "institution_name": institution_name})


@app.get("/oauth-return")
def oauth_return() -> Any:
    """
    OAuth redirect URI handler (Chase).

    Plaid Link expects you to re-open Link with `receivedRedirectUri` after the redirect.
    This route just redirects back to `/` carrying the full redirect URL in a query param.
    """
    # Preserve full querystring.
    full = request.url
    return redirect("/?received_redirect_uri=" + full)


if __name__ == "__main__":
    _db_init()
    port = int(os.environ.get("PORT", "5000") or "5000")
    app.run(host="127.0.0.1", port=port, debug=True)

