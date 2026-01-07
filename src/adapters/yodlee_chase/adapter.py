from __future__ import annotations

import datetime as dt
import os
from typing import Any

from src.adapters.yodlee_chase.client import YodleeChaseClient
from src.core.net import network_enabled
from src.importers.adapters import BrokerAdapter, ProviderError
from src.utils.time import utcnow


def _as_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return None
        return float(s.replace(",", ""))
    except Exception:
        return None


def _as_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _extract_list(obj: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if not isinstance(obj, dict):
        return []
    for k in keys:
        v = obj.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
        if isinstance(v, dict):
            # Sometimes nested like {"account": [...]} or {"transaction": [...]}.
            for kk in ("account", "accounts", "holding", "holdings", "transaction", "transactions"):
                vv = v.get(kk) if isinstance(v, dict) else None
                if isinstance(vv, list):
                    return [x for x in vv if isinstance(x, dict)]
    return []


def _is_chase_institution(raw: dict[str, Any]) -> bool:
    hay = " ".join(
        [
            _as_str(raw.get("providerName")),
            _as_str(raw.get("institutionName")),
            _as_str(raw.get("providerAccountName")),
            _as_str(raw.get("provider")),
            _as_str(raw.get("aggregationSource")),
        ]
    ).upper()
    return any(k in hay for k in ["CHASE", "JPMORGAN", "J.P. MORGAN", "JPM "])


def _infer_account_type(raw: dict[str, Any]) -> str:
    """
    Map provider account types into our AccountType enum:
      - TAXABLE | IRA | OTHER
    """
    hay = " ".join(
        [
            _as_str(raw.get("accountType")),
            _as_str(raw.get("account_type")),
            _as_str(raw.get("type")),
            _as_str(raw.get("classification")),
            _as_str(raw.get("accountName")),
            _as_str(raw.get("name")),
        ]
    ).upper()
    if any(k in hay for k in ["IRA", "ROTH", "TRADITIONAL", "SEP", "SIMPLE"]):
        return "IRA"
    if any(k in hay for k in ["BROKERAGE", "INVESTMENT", "TAXABLE"]):
        return "TAXABLE"
    return "OTHER"


def _infer_tx_type(raw: dict[str, Any]) -> str:
    """
    Produce our canonical Transaction.type:
      BUY | SELL | DIV | INT | FEE | TRANSFER | OTHER
    """
    t = _as_str(raw.get("type") or raw.get("transactionType") or raw.get("transaction_type")).upper()
    desc = _as_str(raw.get("description") or raw.get("memo") or raw.get("merchant") or raw.get("baseType")).upper()
    cat = _as_str(raw.get("category") or raw.get("categoryType") or raw.get("category_type")).upper()
    subtype = _as_str(raw.get("subType") or raw.get("sub_type") or raw.get("transactionSubType")).upper()

    blob = " ".join([t, cat, subtype, desc]).strip()
    if not blob:
        return "OTHER"
    if any(k in blob for k in ["DIVIDEND", "DIV "]):
        return "DIV"
    if "INTEREST" in blob:
        return "INT"
    if any(k in blob for k in ["FEE", "COMMISSION", "ADVISORY", "SERVICE CHARGE"]):
        return "FEE"
    if any(k in blob for k in ["CONTRIBUTION", "DISTRIBUTION", "DEPOSIT", "WITHDRAWAL", "TRANSFER", "ROLLOVER"]):
        return "TRANSFER"
    if any(k in blob for k in ["BUY", "PURCHASE"]):
        return "BUY"
    if any(k in blob for k in ["SELL", "REDEMPTION"]):
        return "SELL"
    return "OTHER"


def _normalize_signed_amount(raw: dict[str, Any], tx_type: str) -> float | None:
    amount = raw.get("amount")
    if isinstance(amount, dict):
        # Common pattern: {"amount": 12.34, "currency": "USD"}
        amount = amount.get("amount") if isinstance(amount.get("amount"), (int, float, str)) else amount.get("value")
    amt = _as_float(amount)
    if amt is None:
        return None

    base = _as_str(raw.get("baseType") or raw.get("base_type")).upper()
    if base == "DEBIT":
        amt = -abs(amt)
    elif base == "CREDIT":
        amt = abs(amt)

    # Normalize obvious trade directions when provider emits positive-only amounts.
    if tx_type == "BUY":
        amt = -abs(amt)
    elif tx_type == "SELL":
        amt = abs(amt)
    return float(amt)


def _normalize_symbol(raw: dict[str, Any]) -> str | None:
    sym = _as_str(raw.get("symbol") or raw.get("securitySymbol") or raw.get("ticker") or raw.get("cusip") or raw.get("isin"))
    if not sym:
        return None
    sym_u = sym.upper()
    if sym_u.isalnum() and len(sym_u) in {9} and sym_u != "CASH":
        # Likely CUSIP; preserve as stable internal ticker.
        return f"CUSIP:{sym_u}"
    if sym_u == "CASH":
        return "CASH:USD"
    return sym_u


class YodleeChaseAdapter(BrokerAdapter):
    """
    Live Yodlee-backed Chase connector (read-only).

    Connection:
      - provider=YODLEE
      - broker=CHASE
      - connector=CHASE_YODLEE

    Credentials (AdapterConnectionContext.credentials):
      - YODLEE_ACCESS_TOKEN (required)
      - YODLEE_REFRESH_TOKEN (optional; currently not auto-refreshed)

    Connection metadata_json:
      - yodlee_base_url (optional; overrides env YODLEE_BASE_URL)
      - yodlee_api_version (optional; forwarded as Api-Version header)
    """

    @property
    def page_size(self) -> int:
        return 200

    def _client(self, ctx: Any) -> YodleeChaseClient:
        if not network_enabled():
            raise ProviderError("Network disabled; set NETWORK_ENABLED=1 to enable live connectors.")
        meta = getattr(ctx, "metadata_json", None) or getattr(getattr(ctx, "connection", None), "metadata_json", {}) or {}
        base_url = _as_str(meta.get("yodlee_base_url")) or _as_str(os.environ.get("YODLEE_BASE_URL"))
        api_ver = _as_str(meta.get("yodlee_api_version")) or _as_str(os.environ.get("YODLEE_API_VERSION"))
        token = (getattr(ctx, "credentials", {}) or {}).get("YODLEE_ACCESS_TOKEN")
        client = YodleeChaseClient(base_url=base_url, access_token=token or "", api_version=api_ver or None)
        # Persist safe adapter metrics into run_settings (mutable dict).
        try:
            if getattr(ctx, "run_settings", None) is not None:
                ctx.run_settings["yodlee_rate_limit_hits"] = int(getattr(client, "rate_limit_hits", 0) or 0)
        except Exception:
            pass
        return client

    def fetch_accounts(self, connection: Any) -> list[dict[str, Any]]:
        client = self._client(connection)
        data = client.get_accounts()
        rows = _extract_list(data, "account", "accounts", "data", "response")
        if not rows and isinstance(data.get("account"), list):
            rows = [x for x in data.get("account") if isinstance(x, dict)]
        # Filter to Chase when identifiable.
        chase_rows = [r for r in rows if _is_chase_institution(r)]
        used = chase_rows if chase_rows else rows
        out: list[dict[str, Any]] = []
        for r in used:
            provider_account_id = _as_str(r.get("id") or r.get("accountId") or r.get("account_id"))
            name = _as_str(r.get("accountName") or r.get("name") or r.get("nickname") or provider_account_id)
            if not provider_account_id:
                continue
            out.append(
                {
                    "provider_account_id": provider_account_id,
                    "name": name,
                    "account_type": _infer_account_type(r),
                    "raw_provider_name": _as_str(r.get("providerName") or r.get("institutionName")),
                }
            )
        # Cache for pagination to avoid re-fetching accounts per page.
        try:
            if getattr(connection, "run_settings", None) is not None:
                connection.run_settings["_yodlee_accounts_cache"] = out
        except Exception:
            pass
        if not out:
            raise ProviderError("No accounts returned from Yodlee.")
        return out

    def fetch_transactions(
        self,
        connection: Any,
        start_date: dt.date,
        end_date: dt.date,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        client = self._client(connection)
        accounts = []
        try:
            accounts = list((getattr(connection, "run_settings", {}) or {}).get("_yodlee_accounts_cache") or [])
        except Exception:
            accounts = []
        if not accounts:
            accounts = self.fetch_accounts(connection)

        acct_idx = 0
        skip = 0
        if cursor:
            try:
                parts = cursor.split(":", 1)
                acct_idx = int(parts[0])
                skip = int(parts[1]) if len(parts) > 1 else 0
            except Exception:
                acct_idx = 0
                skip = 0
        if acct_idx >= len(accounts):
            return [], None

        provider_account_id = _as_str(accounts[acct_idx].get("provider_account_id"))
        if not provider_account_id:
            return [], None

        data = client.get_transactions(
            account_id=provider_account_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            skip=skip,
            top=int(self.page_size),
        )
        rows = _extract_list(data, "transaction", "transactions", "data", "response")
        out: list[dict[str, Any]] = []
        for r in rows:
            d_s = _as_str(r.get("date") or r.get("transactionDate") or r.get("postedDate") or r.get("postDate"))
            d_s = d_s[:10]
            if not d_s:
                continue
            tx_type = _infer_tx_type(r)
            amount = _normalize_signed_amount(r, tx_type)
            if amount is None:
                continue
            symbol = _normalize_symbol(r)
            qty = _as_float(r.get("quantity") or r.get("qty"))
            provider_txn_id = _as_str(r.get("id") or r.get("transactionId") or r.get("transaction_id"))
            desc = _as_str(r.get("description") or r.get("memo") or r.get("merchant") or r.get("descriptionSimple") or "")
            ccy = None
            try:
                if isinstance(r.get("amount"), dict):
                    ccy = _as_str((r.get("amount") or {}).get("currency"))
                ccy = ccy or _as_str(r.get("currency"))
            except Exception:
                ccy = None

            out.append(
                {
                    "provider_transaction_id": provider_txn_id or None,
                    "provider_account_id": provider_account_id,
                    "date": d_s,
                    "type": tx_type,
                    "symbol": symbol,
                    "qty": qty,
                    "amount": float(amount),
                    "description": desc,
                    "currency": ccy or "USD",
                }
            )

        # Decide whether to continue within the same account.
        if len(rows) >= int(self.page_size):
            next_cursor = f"{acct_idx}:{skip + int(self.page_size)}"
        else:
            next_idx = acct_idx + 1
            next_cursor = f"{next_idx}:0" if next_idx < len(accounts) else None

        # Persist safe adapter metrics into run_settings.
        try:
            if getattr(connection, "run_settings", None) is not None:
                connection.run_settings["yodlee_rate_limit_hits"] = int(getattr(client, "rate_limit_hits", 0) or 0)
        except Exception:
            pass

        return out, next_cursor

    def fetch_holdings(self, connection: Any, as_of: dt.datetime | None = None) -> dict[str, Any]:
        client = self._client(connection)
        now_dt = as_of or utcnow()
        accounts = []
        try:
            accounts = list((getattr(connection, "run_settings", {}) or {}).get("_yodlee_accounts_cache") or [])
        except Exception:
            accounts = []
        if not accounts:
            accounts = self.fetch_accounts(connection)

        items: list[dict[str, Any]] = []
        cash_balances: list[dict[str, Any]] = []

        for a in accounts:
            provider_account_id = _as_str(a.get("provider_account_id"))
            if not provider_account_id:
                continue
            data = client.get_holdings(account_id=provider_account_id)
            rows = _extract_list(data, "holding", "holdings", "data", "response")
            for r in rows:
                symbol = _normalize_symbol(r)
                qty = _as_float(r.get("quantity") or r.get("qty") or r.get("units"))
                mv = _as_float(r.get("marketValue") or r.get("value") or r.get("market_value"))
                cb = _as_float(r.get("costBasis") or r.get("cost_basis") or r.get("costBasisValue"))
                if symbol is None:
                    continue
                if symbol.startswith("CASH:") and mv is not None:
                    cash_balances.append(
                        {
                            "provider_account_id": provider_account_id,
                            "currency": "USD",
                            "amount": float(mv),
                            "as_of_date": now_dt.date().isoformat(),
                        }
                    )
                items.append(
                    {
                        "provider_account_id": provider_account_id,
                        "symbol": symbol,
                        "qty": qty,
                        "market_value": mv,
                        "cost_basis_total": cb,
                    }
                )

        # Persist safe adapter metrics into run_settings.
        try:
            if getattr(connection, "run_settings", None) is not None:
                connection.run_settings["yodlee_rate_limit_hits"] = int(getattr(client, "rate_limit_hits", 0) or 0)
        except Exception:
            pass

        return {"as_of": now_dt.isoformat(), "items": items, "cash_balances": cash_balances}

    def test_connection(self, connection: Any) -> dict[str, Any]:
        try:
            client = self._client(connection)
            data = client.get_accounts()
            rows = _extract_list(data, "account", "accounts", "data", "response")
            ok = bool(rows)
            msg = "OK" if ok else "No accounts returned."
            return {
                "ok": ok,
                "message": msg,
                "accounts_seen": len(rows),
                "rate_limit_hits": int(getattr(client, "rate_limit_hits", 0) or 0),
            }
        except ProviderError as e:
            return {"ok": False, "message": str(e)[:250]}
        except Exception as e:
            return {"ok": False, "message": f"{type(e).__name__}: {e}"[:250]}
