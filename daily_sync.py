from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv

from plaid.api import plaid_api
from plaid.configuration import Configuration
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
from plaid.model.products import Products
from plaid.model.transactions_sync_request import TransactionsSyncRequest
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
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS securities (
              security_id TEXT PRIMARY KEY,
              ticker_symbol TEXT,
              name TEXT,
              type TEXT,
              close_price REAL,
              iso_currency_code TEXT,
              unofficial_currency_code TEXT,
              raw_json TEXT,
              updated_at INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
              account_id TEXT PRIMARY KEY,
              item_id TEXT NOT NULL,
              name TEXT,
              official_name TEXT,
              type TEXT,
              subtype TEXT,
              mask TEXT,
              raw_json TEXT,
              updated_at INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS holdings (
              item_id TEXT NOT NULL,
              account_id TEXT NOT NULL,
              security_id TEXT NOT NULL,
              quantity REAL,
              cost_basis REAL,
              institution_price REAL,
              institution_value REAL,
              iso_currency_code TEXT,
              unofficial_currency_code TEXT,
              raw_json TEXT,
              as_of_date TEXT,
              updated_at INTEGER NOT NULL,
              PRIMARY KEY (item_id, account_id, security_id)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
              transaction_id TEXT PRIMARY KEY,
              item_id TEXT NOT NULL,
              account_id TEXT NOT NULL,
              date TEXT,
              authorized_date TEXT,
              name TEXT,
              merchant_name TEXT,
              amount REAL,
              iso_currency_code TEXT,
              unofficial_currency_code TEXT,
              pending INTEGER NOT NULL DEFAULT 0,
              personal_finance_category_primary TEXT,
              personal_finance_category_detailed TEXT,
              payment_channel TEXT,
              transaction_type TEXT,
              raw_json TEXT,
              is_removed INTEGER NOT NULL DEFAULT 0,
              updated_at INTEGER NOT NULL
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_txn_item_date ON transactions(item_id, date)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_holdings_item ON holdings(item_id)")


def _require_env(name: str) -> str:
    v = (os.environ.get(name) or "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _plaid_client() -> plaid_api.PlaidApi:
    env = (os.environ.get("PLAID_ENV") or "production").strip().lower()
    if env not in {"sandbox", "development", "production"}:
        env = "production"
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


@dataclass(frozen=True)
class PlaidItem:
    item_id: str
    access_token: str
    transactions_cursor: str
    institution_name: str | None


def _db_list_items() -> list[PlaidItem]:
    _db_init()
    with _db_connect() as con:
        rows = con.execute(
            "SELECT item_id, access_token, transactions_cursor, institution_name FROM plaid_items ORDER BY updated_at DESC"
        ).fetchall()
    out: list[PlaidItem] = []
    for r in rows:
        out.append(
            PlaidItem(
                item_id=str(r["item_id"]),
                access_token=str(r["access_token"]),
                transactions_cursor=str(r["transactions_cursor"] or ""),
                institution_name=(str(r["institution_name"]) if r["institution_name"] is not None else None),
            )
        )
    return out


def _db_update_cursor(*, item_id: str, cursor: str) -> None:
    now = int(time.time())
    with _db_connect() as con:
        con.execute("UPDATE plaid_items SET transactions_cursor=?, updated_at=? WHERE item_id=?", (cursor or "", now, item_id))


def _json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return "{}"


def _upsert_securities(*, securities: Iterable[dict[str, Any]]) -> int:
    now = int(time.time())
    rows = []
    for s in securities:
        sid = str(s.get("security_id") or "").strip()
        if not sid:
            continue
        ticker = (s.get("ticker_symbol") or None)
        name = (s.get("name") or None)
        sec_type = (s.get("type") or None)
        close = None
        try:
            close = float(s.get("close_price")) if s.get("close_price") is not None else None
        except Exception:
            close = None
        rows.append(
            (
                sid,
                (str(ticker) if ticker is not None else None),
                (str(name) if name is not None else None),
                (str(sec_type) if sec_type is not None else None),
                close,
                (str(s.get("iso_currency_code")) if s.get("iso_currency_code") is not None else None),
                (str(s.get("unofficial_currency_code")) if s.get("unofficial_currency_code") is not None else None),
                _json_dumps(s),
                now,
            )
        )
    if not rows:
        return 0
    with _db_connect() as con:
        con.executemany(
            """
            INSERT INTO securities(security_id, ticker_symbol, name, type, close_price, iso_currency_code, unofficial_currency_code, raw_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(security_id) DO UPDATE SET
              ticker_symbol=COALESCE(excluded.ticker_symbol, securities.ticker_symbol),
              name=COALESCE(excluded.name, securities.name),
              type=COALESCE(excluded.type, securities.type),
              close_price=COALESCE(excluded.close_price, securities.close_price),
              iso_currency_code=COALESCE(excluded.iso_currency_code, securities.iso_currency_code),
              unofficial_currency_code=COALESCE(excluded.unofficial_currency_code, securities.unofficial_currency_code),
              raw_json=excluded.raw_json,
              updated_at=excluded.updated_at
            """,
            rows,
        )
    return len(rows)


def _upsert_accounts(*, item_id: str, accounts: Iterable[dict[str, Any]]) -> int:
    now = int(time.time())
    rows = []
    for a in accounts:
        aid = str(a.get("account_id") or "").strip()
        if not aid:
            continue
        rows.append(
            (
                aid,
                item_id,
                (str(a.get("name")) if a.get("name") is not None else None),
                (str(a.get("official_name")) if a.get("official_name") is not None else None),
                (str(a.get("type")) if a.get("type") is not None else None),
                (str(a.get("subtype")) if a.get("subtype") is not None else None),
                (str(a.get("mask")) if a.get("mask") is not None else None),
                _json_dumps(a),
                now,
            )
        )
    if not rows:
        return 0
    with _db_connect() as con:
        con.executemany(
            """
            INSERT INTO accounts(account_id, item_id, name, official_name, type, subtype, mask, raw_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
              item_id=excluded.item_id,
              name=COALESCE(excluded.name, accounts.name),
              official_name=COALESCE(excluded.official_name, accounts.official_name),
              type=COALESCE(excluded.type, accounts.type),
              subtype=COALESCE(excluded.subtype, accounts.subtype),
              mask=COALESCE(excluded.mask, accounts.mask),
              raw_json=excluded.raw_json,
              updated_at=excluded.updated_at
            """,
            rows,
        )
    return len(rows)


def _upsert_holdings(*, item_id: str, holdings: Iterable[dict[str, Any]], as_of_date: str | None) -> int:
    now = int(time.time())
    rows = []
    for h in holdings:
        aid = str(h.get("account_id") or "").strip()
        sid = str(h.get("security_id") or "").strip()
        if not aid or not sid:
            continue
        qty = None
        cb = None
        ip = None
        iv = None
        try:
            qty = float(h.get("quantity")) if h.get("quantity") is not None else None
        except Exception:
            qty = None
        try:
            cb = float(h.get("cost_basis")) if h.get("cost_basis") is not None else None
        except Exception:
            cb = None
        try:
            ip = float(h.get("institution_price")) if h.get("institution_price") is not None else None
        except Exception:
            ip = None
        try:
            iv = float(h.get("institution_value")) if h.get("institution_value") is not None else None
        except Exception:
            iv = None

        rows.append(
            (
                item_id,
                aid,
                sid,
                qty,
                cb,
                ip,
                iv,
                (str(h.get("iso_currency_code")) if h.get("iso_currency_code") is not None else None),
                (str(h.get("unofficial_currency_code")) if h.get("unofficial_currency_code") is not None else None),
                _json_dumps(h),
                as_of_date,
                now,
            )
        )
    if not rows:
        return 0
    with _db_connect() as con:
        con.executemany(
            """
            INSERT INTO holdings(item_id, account_id, security_id, quantity, cost_basis, institution_price, institution_value,
                                 iso_currency_code, unofficial_currency_code, raw_json, as_of_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id, account_id, security_id) DO UPDATE SET
              quantity=excluded.quantity,
              cost_basis=excluded.cost_basis,
              institution_price=excluded.institution_price,
              institution_value=excluded.institution_value,
              iso_currency_code=COALESCE(excluded.iso_currency_code, holdings.iso_currency_code),
              unofficial_currency_code=COALESCE(excluded.unofficial_currency_code, holdings.unofficial_currency_code),
              raw_json=excluded.raw_json,
              as_of_date=excluded.as_of_date,
              updated_at=excluded.updated_at
            """,
            rows,
        )
    return len(rows)


def _upsert_transactions(*, item_id: str, txns: Iterable[dict[str, Any]]) -> int:
    now = int(time.time())
    rows = []
    for t in txns:
        tid = str(t.get("transaction_id") or "").strip()
        aid = str(t.get("account_id") or "").strip()
        if not tid or not aid:
            continue
        pfc = t.get("personal_finance_category") or {}
        rows.append(
            (
                tid,
                item_id,
                aid,
                str(t.get("date") or "") or None,
                str(t.get("authorized_date") or "") or None,
                str(t.get("name") or "") or None,
                (str(t.get("merchant_name")) if t.get("merchant_name") is not None else None),
                float(t.get("amount") or 0.0),
                (str(t.get("iso_currency_code")) if t.get("iso_currency_code") is not None else None),
                (str(t.get("unofficial_currency_code")) if t.get("unofficial_currency_code") is not None else None),
                1 if bool(t.get("pending")) else 0,
                (str(pfc.get("primary")) if pfc and pfc.get("primary") is not None else None),
                (str(pfc.get("detailed")) if pfc and pfc.get("detailed") is not None else None),
                (str(t.get("payment_channel")) if t.get("payment_channel") is not None else None),
                (str(t.get("transaction_type")) if t.get("transaction_type") is not None else None),
                _json_dumps(t),
                0,
                now,
            )
        )
    if not rows:
        return 0
    with _db_connect() as con:
        con.executemany(
            """
            INSERT INTO transactions(transaction_id, item_id, account_id, date, authorized_date, name, merchant_name, amount,
                                     iso_currency_code, unofficial_currency_code, pending, personal_finance_category_primary,
                                     personal_finance_category_detailed, payment_channel, transaction_type, raw_json, is_removed, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(transaction_id) DO UPDATE SET
              item_id=excluded.item_id,
              account_id=excluded.account_id,
              date=excluded.date,
              authorized_date=excluded.authorized_date,
              name=excluded.name,
              merchant_name=excluded.merchant_name,
              amount=excluded.amount,
              iso_currency_code=COALESCE(excluded.iso_currency_code, transactions.iso_currency_code),
              unofficial_currency_code=COALESCE(excluded.unofficial_currency_code, transactions.unofficial_currency_code),
              pending=excluded.pending,
              personal_finance_category_primary=excluded.personal_finance_category_primary,
              personal_finance_category_detailed=excluded.personal_finance_category_detailed,
              payment_channel=excluded.payment_channel,
              transaction_type=excluded.transaction_type,
              raw_json=excluded.raw_json,
              is_removed=0,
              updated_at=excluded.updated_at
            """,
            rows,
        )
    return len(rows)


def _mark_removed_transactions(*, removed: Iterable[dict[str, Any]]) -> int:
    now = int(time.time())
    ids = []
    for r in removed:
        tid = str(r.get("transaction_id") or "").strip()
        if tid:
            ids.append(tid)
    if not ids:
        return 0
    with _db_connect() as con:
        con.executemany("UPDATE transactions SET is_removed=1, updated_at=? WHERE transaction_id=?", [(now, tid) for tid in ids])
    return len(ids)


def _is_item_login_required(exc: ApiException) -> bool:
    try:
        body = json.loads(exc.body or "{}")
        return str(body.get("error_code") or "").strip().upper() == "ITEM_LOGIN_REQUIRED"
    except Exception:
        return False


def sync_investments(*, client: plaid_api.PlaidApi) -> None:
    _db_init()
    items = _db_list_items()
    if not items:
        print("No Plaid items found. Run setup_auth.py first.")
        return

    for it in items:
        try:
            req = InvestmentsHoldingsGetRequest(access_token=it.access_token)
            resp = client.investments_holdings_get(req)
        except ApiException as e:
            if _is_item_login_required(e):
                print(f"[WARN] Chase re-auth required for item_id={it.item_id} (ITEM_LOGIN_REQUIRED). Re-link via setup_auth.py.")
                continue
            raise

        accounts = resp.get("accounts") or []
        holdings = resp.get("holdings") or []
        securities = resp.get("securities") or []

        # As-of is not explicit in this endpoint; store today's date for audit.
        as_of_date = dt.date.today().isoformat()
        n_sec = _upsert_securities(securities=securities)
        n_acct = _upsert_accounts(item_id=it.item_id, accounts=accounts)
        n_hold = _upsert_holdings(item_id=it.item_id, holdings=holdings, as_of_date=as_of_date)
        print(f"[OK] investments: item_id={it.item_id} securities={n_sec} accounts={n_acct} holdings={n_hold}")


def sync_transactions(*, client: plaid_api.PlaidApi) -> None:
    _db_init()
    items = _db_list_items()
    if not items:
        print("No Plaid items found. Run setup_auth.py first.")
        return

    for it in items:
        cursor = it.transactions_cursor or ""
        has_more = True
        added_total = 0
        modified_total = 0
        removed_total = 0
        try:
            while has_more:
                req = TransactionsSyncRequest(access_token=it.access_token, cursor=cursor)
                resp = client.transactions_sync(req)
                added = resp.get("added") or []
                modified = resp.get("modified") or []
                removed = resp.get("removed") or []
                next_cursor = str(resp.get("next_cursor") or "")
                has_more = bool(resp.get("has_more"))

                added_total += _upsert_transactions(item_id=it.item_id, txns=added)
                modified_total += _upsert_transactions(item_id=it.item_id, txns=modified)
                removed_total += _mark_removed_transactions(removed=removed)

                cursor = next_cursor
                if not cursor:
                    # Defensive: stop if we didn't receive a cursor.
                    has_more = False

            _db_update_cursor(item_id=it.item_id, cursor=cursor)
            print(f"[OK] transactions: item_id={it.item_id} added={added_total} modified={modified_total} removed={removed_total}")
        except ApiException as e:
            if _is_item_login_required(e):
                print(f"[WARN] Chase re-auth required for item_id={it.item_id} (ITEM_LOGIN_REQUIRED). Re-link via setup_auth.py.")
                continue
            raise


if __name__ == "__main__":
    _db_init()
    client = _plaid_client()
    sync_investments(client=client)
    sync_transactions(client=client)
