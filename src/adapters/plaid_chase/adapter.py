from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path
from decimal import Decimal
from typing import Any

from src.adapters.plaid_chase.client import PlaidApiError, PlaidClient, parse_plaid_date
from src.importers.adapters import BrokerAdapter, ProviderError
from src.utils.time import utcnow


def _as_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


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


def _plaid_account_type(raw: dict[str, Any]) -> str:
    # For ExpenseAccount.type: BANK|CREDIT|UNKNOWN
    t = _as_str(raw.get("type")).lower()
    if t in {"depository"}:
        return "BANK"
    if t in {"credit"}:
        return "CREDIT"
    return "UNKNOWN"


def _plaid_security_symbol(sec: dict[str, Any]) -> str | None:
    """
    Produce a stable symbol string for Investor.

    Preference order:
      1) ticker_symbol (common)
      2) cusip -> "CUSIP:<cusip>"
      3) security_id -> "PLAIDSEC:<id>"
    """
    try:
        t = _as_str(sec.get("ticker_symbol")).upper()
        if t:
            return t[:32]
    except Exception:
        pass
    try:
        cusip = _as_str(sec.get("cusip")).upper()
        if cusip:
            return f"CUSIP:{cusip}"[:32]
    except Exception:
        pass
    try:
        sid = _as_str(sec.get("security_id"))
        if sid:
            return f"PLAIDSEC:{sid[:20]}".upper()[:32]
    except Exception:
        pass
    return None


def _plaid_is_cash_security(sec: dict[str, Any]) -> bool:
    """
    Best-effort detection for cash / sweep positions in Plaid holdings.

    For many institutions, cash is represented as a sweep "security" (e.g., QCERQ).
    Treating these as cash avoids double-counting when the Holdings view separately
    models cash via `CashBalance` / snapshot cash.
    """
    try:
        t = _as_str(sec.get("type")).lower()
        if t in {"cash", "cash_equivalent", "deposit"}:
            return True
    except Exception:
        pass
    try:
        name = _as_str(sec.get("name")).upper()
        if any(k in name for k in ("DEPOSIT SWEEP", "DEPOSIT PROGRAM", "CASH SWEEP", "SWEEP")):
            return True
    except Exception:
        pass
    return False


def _plaid_is_cash_like_holding(*, holding: dict[str, Any], security: dict[str, Any]) -> bool:
    if _plaid_is_cash_security(security):
        return True
    # Heuristic fallback: cash/sweep positions are often represented with a unit price of ~1 and
    # quantity ~= market value (dollar-denominated). This also catches some "debit/credit" cash legs
    # that don't have a well-formed security classification.
    try:
        inst_px = _as_float(holding.get("institution_price"))
        if inst_px is None:
            inst_px = _as_float(holding.get("price"))
        if inst_px is None:
            return False
        if abs(float(inst_px) - 1.0) > 1e-6:
            return False
        qty = _as_float(holding.get("quantity"))
        mv = _as_float(holding.get("institution_value") or holding.get("value"))
        if qty is None or mv is None:
            return False
        return abs(float(qty) - float(mv)) <= max(0.02, abs(float(mv)) * 1e-6)
    except Exception:
        return False


def _map_plaid_investment_txn_type(raw: dict[str, Any]) -> str:
    t = _as_str(raw.get("type")).lower()
    st = _as_str(raw.get("subtype")).lower()
    name = _as_str(raw.get("name") or raw.get("original_description") or "").lower()
    # Plaid uses broad categories; keep mapping conservative.
    if t in {"buy"}:
        return "BUY"
    if t in {"sell"}:
        return "SELL"
    # Some institutions mislabel cash dividends as "fee"/"cash" but provide subtype/name hints.
    if t in {"dividend"} or "dividend" in st or "cash div" in st or "cash div" in name or "dividend" in name:
        return "DIV"
    if t in {"interest"} or "interest" in st or "interest" in name:
        return "INT"
    # Withholding often appears as a separate "fee"/"tax" entry with name hints.
    if t in {"tax"} or "withhold" in st or "withhold" in name or "foreign tax" in name:
        return "WITHHOLDING"
    if t in {"fee"} or "fee" in st:
        return "FEE"
    if t in {"cash", "transfer"} or "transfer" in st or "wire" in st:
        return "TRANSFER"
    return "OTHER"


def _signed_amount_for_type(tx_type: str, amount: float) -> float:
    """
    Normalize sign conventions to match Investor transaction semantics:
      - BUY: negative cash outflow
      - SELL: positive cash inflow
      - DIV/INT: positive
      - FEE: negative
      - WITHHOLDING: positive credit (matches existing sync_runner convention)
      - TRANSFER: normalize to Investor (outflow negative, inflow positive)
    """
    a = float(amount)
    t = (tx_type or "").upper()
    if t == "BUY":
        return -abs(a)
    if t == "SELL":
        return abs(a)
    if t in {"DIV", "INT"}:
        return abs(a)
    if t == "FEE":
        return -abs(a)
    if t == "WITHHOLDING":
        return abs(a)
    if t == "TRANSFER":
        # Plaid's sign conventions vary across institutions; for Chase investment feeds we've observed:
        #   - outflows often arrive as positive
        #   - inflows often arrive as negative
        # Normalize to Investor: outflow -> negative, inflow -> positive.
        return -abs(a) if a > 0 else abs(a)
    return a


def _plaid_txnsync_cashflow_type(*, name: str, category_hint: str | None = None) -> str | None:
    """
    Best-effort mapping for Plaid `/transactions/sync` rows that represent *cashflows* in an investment account.

    We intentionally keep this conservative to avoid double-counting security trades/dividends already sourced
    from `/investments/transactions/get`.
    """
    n = (name or "").strip().upper()
    if not n:
        return None
    # Exclude internal sweep mechanics; these distort Cash Out and are not "external" cashflows.
    if "DEPOSIT SWEEP" in n or "CASH SWEEP" in n or "DEPOSIT PROGRAM" in n or "INTRA-DAY" in n:
        return None
    # Exclude income-like entries (dividends/interest) that should come from investments transactions.
    if "DIVIDEND" in n or "CASH DIV" in n or "INTEREST" in n:
        return None

    if "WITHHOLD" in n or "W/H" in n or "WITHHOLDING" in n or "FOREIGN TAX" in n:
        return "WITHHOLDING"
    if category_hint:
        ch = category_hint.strip().upper()
        if "TAX" in ch:
            return "WITHHOLDING"

    # Cash movement between brokerage and bank/checking.
    if "BANKLINK" in n or "ACH" in n or "WIRE" in n or "TRANSFER" in n:
        return "TRANSFER"
    return None


class PlaidChaseAdapter(BrokerAdapter):
    """
    Plaid-backed Chase connector intended for automated Expense transactions ingestion.

    Connection:
      - provider=PLAID
      - broker=CHASE
      - connector=CHASE_PLAID

    Credentials (AdapterConnectionContext.credentials):
      - PLAID_ACCESS_TOKEN (required)
      - PLAID_ITEM_ID (stored for reference; not required to call APIs)

    Connection metadata_json (persisted):
      - plaid_env (optional; defaults to env PLAID_ENV or sandbox)
      - plaid_transactions_cursor (optional; cursor for /transactions/sync)
      - plaid_item_id (optional; mirrors PLAID_ITEM_ID)
    """

    @property
    def page_size(self) -> int:
        return 500

    def _client(self, ctx: Any) -> PlaidClient:
        meta = getattr(ctx, "metadata_json", None) or getattr(getattr(ctx, "connection", None), "metadata_json", {}) or {}
        env = _as_str(meta.get("plaid_env")) or None
        return PlaidClient(env=env)

    def fetch_accounts(self, connection: Any) -> list[dict[str, Any]]:
        client = self._client(connection)
        access_token = (getattr(connection, "credentials", {}) or {}).get("PLAID_ACCESS_TOKEN") or ""
        if not access_token:
            raise ProviderError("Missing credentials: PLAID_ACCESS_TOKEN. Connect via Plaid first.")
        try:
            rows = client.get_accounts(access_token=str(access_token))
        except PlaidApiError as e:
            if e.info.is_item_login_required:
                raise ProviderError("ITEM_LOGIN_REQUIRED: Plaid re-auth required. Re-link the connection via Plaid.")
            raise
        out: list[dict[str, Any]] = []
        item_id = _as_str((getattr(connection, "credentials", {}) or {}).get("PLAID_ITEM_ID")) or _as_str(
            (getattr(connection, "metadata_json", {}) or {}).get("plaid_item_id")
        )
        for r in rows:
            account_id = _as_str(r.get("account_id"))
            name = _as_str(r.get("official_name") or r.get("name") or account_id)
            if not account_id:
                continue
            provider_account_id = f"PLAID:{item_id}:{account_id}" if item_id else f"PLAID:{account_id}"
            balances = r.get("balances") if isinstance(r.get("balances"), dict) else {}
            bal_current = _as_float(balances.get("current")) if balances else None
            bal_available = _as_float(balances.get("available")) if balances else None
            bal_currency = _as_str(
                balances.get("iso_currency_code") or balances.get("unofficial_currency_code") or "USD"
            ).upper()
            out.append(
                {
                    "provider_account_id": provider_account_id,
                    "name": name,
                    "account_type": _plaid_account_type(r),
                    "mask": _as_str(r.get("mask")) or None,
                    "raw_type": _as_str(r.get("type")) or None,
                    "raw_subtype": _as_str(r.get("subtype")) or None,
                    "balance_current": bal_current,
                    "balance_available": bal_available,
                    "balance_currency": bal_currency or "USD",
                }
            )
        # Cache for mapping during txn normalization.
        try:
            if getattr(connection, "run_settings", None) is not None:
                connection.run_settings["_plaid_accounts_cache"] = out
        except Exception:
            pass
        return out

    def fetch_transactions(
        self,
        connection: Any,
        start_date: dt.date,
        end_date: dt.date,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        client = self._client(connection)
        access_token = (getattr(connection, "credentials", {}) or {}).get("PLAID_ACCESS_TOKEN") or ""
        if not access_token:
            raise ProviderError("Missing credentials: PLAID_ACCESS_TOKEN. Connect via Plaid first.")

        meta = getattr(connection, "metadata_json", None) or getattr(getattr(connection, "connection", None), "metadata_json", {}) or {}
        stored_cursor = _as_str(meta.get("plaid_transactions_cursor"))

        # Within a run, `cursor` is used for pagination (Plaid has_more). On the first page, use the saved cursor.
        cursor_in = stored_cursor if cursor is None else cursor

        # Build a quick lookup of Plaid account metadata to avoid mis-routing investment-account txns into Expenses.
        acct_meta_by_plaid_id: dict[str, dict[str, Any]] = {}
        try:
            cache = (getattr(connection, "run_settings", {}) or {}).get("_plaid_accounts_cache") or []
            if isinstance(cache, list):
                for a in cache:
                    if not isinstance(a, dict):
                        continue
                    pid = _as_str(a.get("provider_account_id"))
                    if not pid:
                        continue
                    plaid_id = pid.split(":")[-1].strip()
                    if plaid_id:
                        acct_meta_by_plaid_id[plaid_id] = a
        except Exception:
            acct_meta_by_plaid_id = {}

        items: list[dict[str, Any]] = []

        connector_u = (
            _as_str(getattr(getattr(connection, "connection", None), "connector", None))
            or _as_str(getattr(connection, "connector", None))
        ).upper()
        force_get = False
        try:
            force_get = bool((getattr(connection, "run_settings", {}) or {}).get("plaid_force_transactions_get"))
        except Exception:
            force_get = False

        range_days = 0
        try:
            range_days = max(0, int((end_date - start_date).days))
        except Exception:
            range_days = 0

        use_get = connector_u == "AMEX_PLAID" and cursor is None and (force_get or range_days >= 120)
        if use_get:
            items.extend(
                self._fetch_transactions_get(
                    connection,
                    start_date=start_date,
                    end_date=end_date,
                    acct_meta_by_plaid_id=acct_meta_by_plaid_id,
                    meta=meta,
                )
            )
            try:
                if getattr(connection, "run_settings", None) is not None:
                    connection.run_settings["plaid_transactions_get_used"] = True
            except Exception:
                pass
            if not items and range_days >= 120:
                items.append(
                    {
                        "record_kind": "ADAPTER_WARNING",
                        "message": (
                            "Plaid transactions/get returned 0 transactions for AMEX "
                            f"({start_date.isoformat()} → {end_date.isoformat()})."
                        ),
                    }
                )
            return items, None

        try:
            data = client.transactions_sync(access_token=str(access_token), cursor=cursor_in, count=int(self.page_size))
        except PlaidApiError as e:
            if e.info.is_item_login_required:
                raise ProviderError("ITEM_LOGIN_REQUIRED: Plaid re-auth required. Re-link the connection via Plaid.")
            raise

        added = data.get("added") if isinstance(data, dict) else None
        modified = data.get("modified") if isinstance(data, dict) else None
        removed = data.get("removed") if isinstance(data, dict) else None
        has_more = bool(data.get("has_more")) if isinstance(data, dict) else False
        next_cursor = _as_str(data.get("next_cursor")) if isinstance(data, dict) else ""
        update_status = _as_str(data.get("transactions_update_status")) if isinstance(data, dict) else ""

        # Plaid initial sync can take time to hydrate the full 24-month history. Expose status so the runner/UI
        # can inform the user and avoid marking the one-time "24m backfill" as complete prematurely.
        update_status_u = update_status.upper()
        historical_complete = True
        if update_status_u:
            historical_complete = ("COMPLETE" in update_status_u) and ("IN_PROGRESS" not in update_status_u)

        self._emit_expense_rows(
            connection=connection,
            rows=added,
            items=items,
            meta=meta,
            acct_meta_by_plaid_id=acct_meta_by_plaid_id,
        )
        self._emit_expense_rows(
            connection=connection,
            rows=modified,
            items=items,
            meta=meta,
            acct_meta_by_plaid_id=acct_meta_by_plaid_id,
        )
        # `removed` is a list of {transaction_id}. Ignore for MVP; we treat sync as append-only.
        _ = removed

        # Optional: investment transactions ingestion (used for performance cashflows and tax lots).
        # This is done once per run on the first pagination call (cursor is None).
        try:
            meta = getattr(connection, "metadata_json", None) or getattr(getattr(connection, "connection", None), "metadata_json", {}) or {}
            enable_inv = bool(meta.get("plaid_enable_investments") is True)
        except Exception:
            enable_inv = False
        try:
            already = bool((getattr(connection, "run_settings", {}) or {}).get("_plaid_investment_txns_emitted") is True)
        except Exception:
            already = False

        if enable_inv and not already and cursor is None:
            try:
                inv_items = self._fetch_investment_transactions(connection, start_date=start_date, end_date=end_date)
                items.extend(inv_items)
                try:
                    if getattr(connection, "run_settings", None) is not None:
                        connection.run_settings["plaid_investments_txns_fetch_ok"] = True
                except Exception:
                    pass
            except ProviderError as e:
                # Do not fail the whole sync on investments errors; surface as a warning record.
                items.append({"record_kind": "ADAPTER_WARNING", "message": f"Plaid investments: {e}"})
                try:
                    if getattr(connection, "run_settings", None) is not None:
                        connection.run_settings["plaid_investments_txns_fetch_ok"] = False
                except Exception:
                    pass
            except Exception as e:
                items.append({"record_kind": "ADAPTER_WARNING", "message": f"Plaid investments: {type(e).__name__}: {e}"})
                try:
                    if getattr(connection, "run_settings", None) is not None:
                        connection.run_settings["plaid_investments_txns_fetch_ok"] = False
                except Exception:
                    pass

            # Additionally, some Chase brokerage cash movements (e.g., IRA → checking transfers, tax withholding)
            # are surfaced via Plaid `/transactions/sync` on the *investment* account, but do not appear in
            # `/investments/transactions/get`. Emit a conservative subset of those as investment cashflows.
            def _emit_inv_cashflows(rows: Any) -> None:
                if not isinstance(rows, list):
                    return
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    tx_id = _as_str(r.get("transaction_id"))
                    acct_id_raw = _as_str(r.get("account_id"))
                    posted = _as_str(r.get("date"))
                    if not tx_id or not acct_id_raw or not posted:
                        continue
                    raw_type = _as_str((acct_meta_by_plaid_id.get(acct_id_raw) or {}).get("raw_type")).lower()
                    if raw_type != "investment":
                        continue

                    amt = _as_float(r.get("amount"))
                    if amt is None:
                        continue
                    # Plaid sign convention: positive = outflow. Convert to our canonical: debit negative.
                    signed = float(-float(amt))
                    currency = (
                        _as_str(r.get("iso_currency_code") or r.get("unofficial_currency_code") or "USD").upper()
                        or "USD"
                    )
                    if currency != "USD":
                        continue

                    desc = _as_str(r.get("name") or r.get("merchant_name") or "")
                    desc = " ".join(desc.split()) or "Investment cashflow"

                    cat_hint = None
                    pfc = r.get("personal_finance_category")
                    if isinstance(pfc, dict):
                        cat_hint = _as_str(pfc.get("primary") or pfc.get("detailed")) or None

                    tx_type = _plaid_txnsync_cashflow_type(name=desc, category_hint=cat_hint)
                    if not tx_type:
                        continue

                    item_id = _as_str((getattr(connection, "credentials", {}) or {}).get("PLAID_ITEM_ID")) or _as_str(
                        meta.get("plaid_item_id")
                    )
                    provider_account_id = f"PLAID:{item_id}:{acct_id_raw}" if item_id else f"PLAID:{acct_id_raw}"

                    items.append(
                        {
                            "record_kind": "TRANSACTION",
                            "provider_transaction_id": f"PLAID_TXN:{tx_id}",
                            "provider_account_id": provider_account_id,
                            "date": posted[:10],
                            "type": tx_type,
                            "amount": signed,
                            "ticker": "UNKNOWN",
                            "qty": None,
                            "description": desc,
                            "currency": currency,
                            "additional_detail": None,
                            "cashflow_kind": "PLAID_TXNSYNC",
                            "raw": {
                                "plaid_transaction_id": tx_id,
                                "plaid_account_id": acct_id_raw,
                                "pending": bool(r.get("pending")),
                            },
                        }
                    )

            _emit_inv_cashflows(added)
            _emit_inv_cashflows(modified)
            try:
                if getattr(connection, "run_settings", None) is not None:
                    connection.run_settings["_plaid_investment_txns_emitted"] = True
            except Exception:
                pass

        # On the final page (has_more == False), emit a cursor update marker so sync_runner can persist it.
        if not has_more and next_cursor:
            items.append(
                {
                    "record_kind": "SYNC_CURSOR",
                    "cursor_kind": "PLAID_TRANSACTIONS",
                    "cursor": next_cursor,
                    "transactions_update_status": update_status or None,
                    "historical_complete": bool(historical_complete),
                }
            )

        # For pagination: keep calling while has_more.
        page_next = next_cursor if has_more and next_cursor else None
        return items, page_next

    def _emit_expense_rows(
        self,
        *,
        connection: Any,
        rows: Any,
        items: list[dict[str, Any]],
        meta: dict[str, Any],
        acct_meta_by_plaid_id: dict[str, dict[str, Any]],
    ) -> None:
        if not isinstance(rows, list):
            return
        for r in rows:
            if not isinstance(r, dict):
                continue
            tx_id = _as_str(r.get("transaction_id"))
            pending_tx_id = _as_str(r.get("pending_transaction_id"))
            if pending_tx_id:
                tx_id = pending_tx_id
            acct_id_raw = _as_str(r.get("account_id"))
            posted = _as_str(r.get("date"))
            if not posted or not acct_id_raw:
                continue
            # Use Plaid sign convention: positive = outflow. Convert to our canonical: debit negative.
            amt = _as_float(r.get("amount"))
            if amt is None:
                continue
            signed = Decimal(str(-float(amt)))
            currency = _as_str(r.get("iso_currency_code") or r.get("unofficial_currency_code") or "USD").upper()
            desc = _as_str(r.get("merchant_name") or r.get("name") or "")
            if not desc:
                desc = "Unknown"

            # Stable provider_account_id format matches fetch_accounts output.
            item_id = _as_str((getattr(connection, "credentials", {}) or {}).get("PLAID_ITEM_ID")) or _as_str(
                meta.get("plaid_item_id")
            )
            provider_account_id = f"PLAID:{item_id}:{acct_id_raw}" if item_id else f"PLAID:{acct_id_raw}"

            # Personal finance category is helpful as a hint; it is not deterministically applied.
            cat_hint = None
            pfc = r.get("personal_finance_category")
            if isinstance(pfc, dict):
                cat_hint = _as_str(pfc.get("primary") or pfc.get("detailed")) or None

            # Do not treat investment-account transactions as "expenses"; they belong in the investment ledger.
            raw_type = ""
            try:
                raw_type = _as_str((acct_meta_by_plaid_id.get(acct_id_raw) or {}).get("raw_type")).lower()
            except Exception:
                raw_type = ""
            if raw_type == "investment":
                continue

            items.append(
                {
                    "record_kind": "EXPENSE_TXN",
                    "provider_transaction_id": tx_id or None,
                    "provider_account_id": provider_account_id,
                    "date": posted[:10],
                    "amount": str(signed),
                    "currency": currency,
                    "description": desc,
                    "category_hint": cat_hint,
                    # Keep raw for optional debugging (no secrets).
                    "raw": {
                        "pending": bool(r.get("pending")),
                        "plaid_transaction_id": _as_str(r.get("transaction_id")),
                        "pending_transaction_id": pending_tx_id or None,
                        "merchant_id": _as_str(r.get("merchant_id")),
                        "account_id": acct_id_raw,
                        "account_owner": _as_str(r.get("account_owner")),
                        "authorized_user": _as_str(r.get("authorized_user") or r.get("authorized_user_name")),
                        "authorized_user_id": _as_str(r.get("authorized_user_id")),
                    },
                }
            )

    def _fetch_transactions_get(
        self,
        connection: Any,
        *,
        start_date: dt.date,
        end_date: dt.date,
        acct_meta_by_plaid_id: dict[str, dict[str, Any]],
        meta: dict[str, Any],
    ) -> list[dict[str, Any]]:
        client = self._client(connection)
        access_token = (getattr(connection, "credentials", {}) or {}).get("PLAID_ACCESS_TOKEN") or ""
        if not access_token:
            raise ProviderError("Missing credentials: PLAID_ACCESS_TOKEN. Connect via Plaid first.")

        items: list[dict[str, Any]] = []
        offset = 0
        page_size = int(self.page_size)
        total: int | None = None

        while True:
            try:
                data = client.transactions_get(
                    access_token=str(access_token),
                    start_date=start_date,
                    end_date=end_date,
                    offset=offset,
                    count=page_size,
                )
            except PlaidApiError as e:
                if e.info.is_item_login_required:
                    raise ProviderError("ITEM_LOGIN_REQUIRED: Plaid re-auth required. Re-link the connection via Plaid.")
                raise
            rows = data.get("transactions") if isinstance(data, dict) else None
            if not rows:
                break
            self._emit_expense_rows(
                connection=connection,
                rows=rows,
                items=items,
                meta=meta,
                acct_meta_by_plaid_id=acct_meta_by_plaid_id,
            )
            if total is None:
                try:
                    total = int(data.get("total_transactions") or 0)
                except Exception:
                    total = None
            offset += len(rows)
            if total is not None and offset >= total:
                break
            if len(rows) < page_size:
                break
        return items

    def _fetch_investment_transactions(self, connection: Any, *, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
        client = self._client(connection)
        access_token = (getattr(connection, "credentials", {}) or {}).get("PLAID_ACCESS_TOKEN") or ""
        if not access_token:
            raise ProviderError("Missing credentials: PLAID_ACCESS_TOKEN. Connect via Plaid first.")

        item_id = _as_str((getattr(connection, "credentials", {}) or {}).get("PLAID_ITEM_ID")) or _as_str(
            (getattr(connection, "metadata_json", {}) or {}).get("plaid_item_id")
        )

        out: list[dict[str, Any]] = []
        offset = 0
        page_size = 500
        total: int | None = None
        securities_by_id: dict[str, dict[str, Any]] = {}

        while True:
            try:
                data = client.investments_transactions_get(
                    access_token=str(access_token),
                    start_date=start_date,
                    end_date=end_date,
                    offset=offset,
                    count=page_size,
                )
            except PlaidApiError as e:
                if e.info.is_item_login_required:
                    raise ProviderError("ITEM_LOGIN_REQUIRED: Chase re-auth required. Re-link the connection via Plaid.")
                # If investments aren't enabled/available, surface a clear message.
                raise ProviderError(f"{e.info.error_code}: {e.info.error_message}")

            txns = data.get("investment_transactions")
            if not isinstance(txns, list):
                break
            secs = data.get("securities")
            if isinstance(secs, list):
                for s in secs:
                    if isinstance(s, dict) and _as_str(s.get("security_id")):
                        securities_by_id[_as_str(s.get("security_id"))] = s
            try:
                total = int(data.get("total_investment_transactions")) if data.get("total_investment_transactions") is not None else total
            except Exception:
                total = total

            for r in txns:
                if not isinstance(r, dict):
                    continue
                inv_id = _as_str(r.get("investment_transaction_id"))
                acct_id_raw = _as_str(r.get("account_id"))
                if not inv_id or not acct_id_raw:
                    continue
                # Currency guardrail: Investor transaction table is implicitly USD.
                ccy = _as_str(r.get("iso_currency_code") or "USD").upper() or "USD"
                if ccy != "USD":
                    continue

                d = _as_str(r.get("date"))
                if not d:
                    continue

                tx_type = _map_plaid_investment_txn_type(r)
                amt = _as_float(r.get("amount"))
                if amt is None:
                    continue
                amt_signed = _signed_amount_for_type(tx_type, amt)

                qty = _as_float(r.get("quantity"))
                price = _as_float(r.get("price"))
                sec_id = _as_str(r.get("security_id"))
                sym = None
                if sec_id and sec_id in securities_by_id:
                    sym = _plaid_security_symbol(securities_by_id[sec_id])
                if not sym:
                    # Cash-like transfers or unknown securities.
                    sym = "UNKNOWN"

                provider_account_id = f"PLAID:{item_id}:{acct_id_raw}" if item_id else f"PLAID:{acct_id_raw}"
                desc = _as_str(r.get("name") or r.get("original_description") or r.get("type") or "Investment txn")
                desc = " ".join(desc.split())

                out.append(
                    {
                        "record_kind": "TRANSACTION",
                        "provider_transaction_id": f"PLAID_INV:{inv_id}",
                        "provider_account_id": provider_account_id,
                        "date": d[:10],
                        "type": tx_type,
                        "amount": float(amt_signed),
                        "ticker": sym,
                        "qty": qty,
                        "description": desc,
                        "currency": ccy,
                        "additional_detail": None,
                        "cashflow_kind": "INVESTMENT",
                        "raw": {
                            "plaid_investment_transaction_id": inv_id,
                            "plaid_account_id": acct_id_raw,
                            "security_id": sec_id or None,
                            "price": price,
                            "fees": _as_float(r.get("fees")),
                            "subtype": _as_str(r.get("subtype")) or None,
                        },
                    }
                )

            offset += len(txns)
            if total is not None and offset >= int(total):
                break
            if len(txns) < page_size:
                break
        return out

    def fetch_holdings(self, connection: Any, as_of: dt.datetime | None = None) -> dict[str, Any]:
        run_settings = getattr(connection, "run_settings", None) or {}
        forced_path = str(run_settings.get("holdings_file_path") or "").strip()
        if forced_path:
            try:
                from src.adapters.chase_offline.adapter import (
                    _parse_chase_performance_report_csv,
                    _parse_chase_statement_pdf,
                )

                p0 = Path(os.path.expanduser(forced_path))
                if p0.exists() and p0.is_file():
                    payload: dict[str, Any] | None = None
                    if p0.suffix.lower() == ".pdf":
                        payload = _parse_chase_statement_pdf(p0)
                    elif p0.suffix.lower() in {".csv", ".tsv", ".txt"}:
                        try:
                            head = p0.read_text(encoding="utf-8-sig", errors="ignore")[:20000]
                        except Exception:
                            head = ""
                        payload = _parse_chase_performance_report_csv(p0) if head else None

                    if isinstance(payload, dict):
                        provider_map = run_settings.get("plaid_account_map") or []
                        default_provider_id = str(run_settings.get("plaid_default_provider_account_id") or "").strip()

                        def _extract_last4(text: str) -> str:
                            if not text:
                                return ""
                            patterns = [
                                r"(?:\*{2,}|\.{2,}|x{2,})\s*(\d{4})",
                                r"ACCOUNT(?:\s*NUMBER|\s*#)?[^0-9]*(\d{4})",
                                r"\bACCT(?:\s*NUMBER|\s*#)?[^0-9]*(\d{4})",
                            ]
                            for pat in patterns:
                                m = re.findall(pat, text.upper())
                                if m:
                                    return m[-1]
                            return ""

                        def _extract_last4_from_name(name: str) -> str:
                            if not name:
                                return ""
                            # Prefer trailing 4-digit sequences; skip obvious date fragments.
                            name_u = str(name)
                            # Exclude yyyymmdd date fragments.
                            date_hits = {m.start() for m in re.finditer(r"(?:20\d{2})(?:\d{2})(?:\d{2})", name_u)}
                            matches: list[tuple[int, str]] = []
                            for m in re.finditer(r"(?<!\d)(\d{4})(?!\d)", name_u):
                                if any(m.start() >= dh and m.start() < dh + 8 for dh in date_hits):
                                    continue
                                matches.append((m.start(), m.group(1)))
                            if matches:
                                return matches[-1][1]
                            return ""

                        last4 = _extract_last4_from_name(p0.name)
                        txt = ""
                        if not last4:
                            try:
                                txt = p0.read_text(encoding="utf-8-sig", errors="ignore")
                                last4 = _extract_last4(txt)
                            except Exception:
                                last4 = ""

                        text_u = ""
                        if not last4:
                            try:
                                text_u = (txt or "").upper()
                            except Exception:
                                text_u = ""

                        mapped_provider_id = ""
                        if last4 and provider_map:
                            candidates = [row for row in provider_map if str(row.get("last4") or "") == last4]
                            if candidates:
                                candidates.sort(
                                    key=lambda r: str(r.get("created_at") or "")
                                )
                                mapped_provider_id = str(candidates[-1].get("provider_account_id") or "").strip()
                        if not mapped_provider_id and text_u:
                            desired_type = "IRA" if "IRA" in text_u else ""
                            if desired_type:
                                candidates = [
                                    row for row in provider_map if str(row.get("account_type") or "").upper() == desired_type
                                ]
                                if candidates:
                                    candidates.sort(key=lambda r: str(r.get("created_at") or ""))
                                    mapped_provider_id = str(candidates[-1].get("provider_account_id") or "").strip()
                        if not mapped_provider_id:
                            mapped_provider_id = default_provider_id
                        if not mapped_provider_id and provider_map:
                            mapped_provider_id = str(provider_map[0].get("provider_account_id") or "").strip()

                        if mapped_provider_id:
                            if isinstance(payload.get("items"), list):
                                for it in payload["items"]:
                                    if isinstance(it, dict):
                                        it["provider_account_id"] = mapped_provider_id
                            if isinstance(payload.get("snapshots"), list):
                                for snap in payload["snapshots"]:
                                    if isinstance(snap, dict) and isinstance(snap.get("items"), list):
                                        for it in snap["items"]:
                                            if isinstance(it, dict):
                                                it["provider_account_id"] = mapped_provider_id
                            if isinstance(payload.get("cash_balances"), list):
                                for it in payload["cash_balances"]:
                                    if isinstance(it, dict):
                                        it["provider_account_id"] = mapped_provider_id
                        return payload
            except Exception as e:
                return {
                    "as_of": (as_of or utcnow()).isoformat(),
                    "items": [],
                    "warnings": [f"Failed to parse holdings statement: {type(e).__name__}: {e}"],
                }

        # Best-effort; many Chase items won't support investments. Treat failures as warnings upstream.
        client = self._client(connection)
        access_token = (getattr(connection, "credentials", {}) or {}).get("PLAID_ACCESS_TOKEN") or ""
        if not access_token:
            return {"as_of": (as_of or utcnow()).isoformat(), "items": []}
        try:
            data = client.investments_holdings_get(access_token=str(access_token))
        except PlaidApiError as e:
            if e.info.is_item_login_required:
                raise ProviderError("ITEM_LOGIN_REQUIRED: Plaid re-auth required. Re-link the connection via Plaid.")
            # Some institutions simply don't support investments; do not hard-fail.
            return {"as_of": (as_of or utcnow()).isoformat(), "items": [], "warnings": [str(e)]}

        # Minimal holdings normalization into the same shape used elsewhere.
        items: list[dict[str, Any]] = []
        cash_balances: list[dict[str, Any]] = []
        accounts = data.get("accounts") if isinstance(data, dict) else None
        holdings = data.get("holdings") if isinstance(data, dict) else None
        securities = data.get("securities") if isinstance(data, dict) else None
        sec_by_id: dict[str, dict[str, Any]] = {}
        if isinstance(securities, list):
            for s in securities:
                if isinstance(s, dict) and _as_str(s.get("security_id")):
                    sec_by_id[_as_str(s.get("security_id"))] = s
        acct_by_id: dict[str, dict[str, Any]] = {}
        if isinstance(accounts, list):
            for a in accounts:
                if isinstance(a, dict) and _as_str(a.get("account_id")):
                    acct_by_id[_as_str(a.get("account_id"))] = a

        item_id = _as_str((getattr(connection, "credentials", {}) or {}).get("PLAID_ITEM_ID")) or _as_str(
            (getattr(connection, "metadata_json", {}) or {}).get("plaid_item_id")
        )

        as_of_dt = as_of or utcnow()
        # Derive cash from holdings rows rather than account balances:
        # For Plaid investment accounts, `balances.current` is not reliably "cash" (can be total account value),
        # and many institutions model cash as a sweep "security" (e.g., QCERQ). We'll surface those as a single
        # synthetic `CASH:USD` row per account (and as a CashBalance) to avoid double-counting.
        cash_by_provider_account_id: dict[str, float] = {}

        if isinstance(holdings, list):
            for h in holdings:
                if not isinstance(h, dict):
                    continue
                acct_id = _as_str(h.get("account_id"))
                sec_id = _as_str(h.get("security_id"))
                qty = _as_float(h.get("quantity"))
                cb = _as_float(h.get("cost_basis"))
                mv = _as_float(h.get("institution_value") or h.get("value"))
                sym = None
                sec = sec_by_id.get(sec_id) or {}
                if sec:
                    sym = _as_str(sec.get("ticker_symbol") or sec.get("cusip") or sec.get("isin"))
                if not sym:
                    sym = f"PLAID_SEC:{sec_id}" if sec_id else None
                if not sym or not acct_id:
                    continue
                provider_account_id = f"PLAID:{item_id}:{acct_id}" if item_id else f"PLAID:{acct_id}"

                if _plaid_is_cash_like_holding(holding=h, security=sec):
                    amt = mv if mv is not None else qty
                    if amt is not None:
                        cash_by_provider_account_id[provider_account_id] = float(
                            cash_by_provider_account_id.get(provider_account_id) or 0.0
                        ) + float(amt)
                    continue

                items.append(
                    {
                        "provider_account_id": provider_account_id,
                        "symbol": sym.upper(),
                        "qty": qty,
                        "market_value": mv,
                        "cost_basis_total": cb,
                    }
                )

        for provider_account_id, amt in sorted(cash_by_provider_account_id.items(), key=lambda kv: kv[0]):
            items.append(
                {
                    "provider_account_id": provider_account_id,
                    "symbol": "CASH:USD",
                    "qty": float(amt),
                    "market_value": float(amt),
                    "asset_type": "CASH",
                }
            )
            cash_balances.append(
                {
                    "provider_account_id": provider_account_id,
                    "currency": "USD",
                    "amount": float(amt),
                    "as_of_date": as_of_dt.date().isoformat(),
                }
            )
        out: dict[str, Any] = {"as_of": as_of_dt.isoformat(), "items": items}
        if cash_balances:
            out["cash_balances"] = cash_balances
        return out

    def test_connection(self, connection: Any) -> dict[str, Any]:
        try:
            accounts = self.fetch_accounts(connection)
            ok = bool(accounts)
            return {"ok": ok, "message": "OK" if ok else "No accounts returned.", "accounts_seen": len(accounts)}
        except ProviderError as e:
            return {"ok": False, "message": str(e)[:250]}
        except Exception as e:
            return {"ok": False, "message": f"{type(e).__name__}: {e}"[:250]}
