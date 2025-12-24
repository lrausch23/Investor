from __future__ import annotations

import datetime as dt
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class ProviderError(Exception):
    pass


class RangeTooLargeError(ProviderError):
    pass


class BrokerAdapter(ABC):
    @abstractmethod
    def fetch_accounts(self, connection: Any) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def fetch_transactions(
        self,
        connection: Any,
        start_date: dt.date,
        end_date: dt.date,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        raise NotImplementedError

    @abstractmethod
    def fetch_holdings(self, connection: Any, as_of: dt.datetime | None = None) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def test_connection(self, connection: Any) -> dict[str, Any]:
        raise NotImplementedError

    @property
    def page_size(self) -> int:
        return 100


class InteractiveBrokersAdapter(BrokerAdapter):
    def fetch_accounts(self, connection: Any) -> list[dict[str, Any]]:
        return []

    def fetch_transactions(
        self, connection: Any, start_date: dt.date, end_date: dt.date, cursor: str | None = None
    ) -> tuple[list[dict[str, Any]], str | None]:
        return [], None

    def fetch_holdings(self, connection: Any, as_of: dt.datetime | None = None) -> dict[str, Any]:
        from src.utils.time import utcnow

        return {"as_of": (as_of or utcnow()).isoformat(), "items": []}

    def test_connection(self, connection: Any) -> dict[str, Any]:
        return {"ok": False, "message": "Live IB adapter not implemented in MVP."}


class RaymondJamesAdapter(BrokerAdapter):
    def fetch_accounts(self, connection: Any) -> list[dict[str, Any]]:
        return []

    def fetch_transactions(
        self, connection: Any, start_date: dt.date, end_date: dt.date, cursor: str | None = None
    ) -> tuple[list[dict[str, Any]], str | None]:
        return [], None

    def fetch_holdings(self, connection: Any, as_of: dt.datetime | None = None) -> dict[str, Any]:
        from src.utils.time import utcnow

        return {"as_of": (as_of or utcnow()).isoformat(), "items": []}

    def test_connection(self, connection: Any) -> dict[str, Any]:
        return {"ok": False, "message": "Live RJ adapter not implemented in MVP."}


class YodleeIBFixtureAdapter(BrokerAdapter):
    """
    Local-only adapter used for development/tests without network.

    Connection metadata:
      - fixture_accounts: list[dict] (optional)
      - fixture_transactions_pages: list[list[dict]] (optional)
      - fixture_holdings: dict (optional)

    Each transaction dict is treated as already-normalized, with fields:
      date, amount, type, symbol/ticker, description, qty, provider_transaction_id?, provider_account_id?
    """

    def fetch_accounts(self, connection: Any) -> list[dict[str, Any]]:
        meta = getattr(connection, "metadata_json", {}) or {}
        if meta.get("fixture_dir"):
            p = Path(str(meta["fixture_dir"])) / "accounts.json"
            if p.exists():
                return json.loads(p.read_text())
        accounts = meta.get("fixture_accounts") or []
        return list(accounts)

    def fetch_transactions(
        self, connection: Any, start_date: dt.date, end_date: dt.date, cursor: str | None = None
    ) -> tuple[list[dict[str, Any]], str | None]:
        meta = getattr(connection, "metadata_json", {}) or {}
        if meta.get("fixture_dir"):
            p = Path(str(meta["fixture_dir"])) / "transactions_pages.json"
            if p.exists():
                pages = json.loads(p.read_text())
            else:
                pages = meta.get("fixture_transactions_pages") or []
        else:
            pages = meta.get("fixture_transactions_pages") or []
        idx = int(cursor) if cursor is not None else 0
        if idx >= len(pages):
            return [], None
        items = list(pages[idx] or [])
        # Filter by date range for realism.
        out = []
        for it in items:
            try:
                d = dt.date.fromisoformat(str(it.get("date")))
            except Exception:
                continue
            if d < start_date or d > end_date:
                continue
            out.append(it)
        next_cursor = str(idx + 1) if (idx + 1) < len(pages) else None
        return out, next_cursor

    def fetch_holdings(self, connection: Any, as_of: dt.datetime | None = None) -> dict[str, Any]:
        meta = getattr(connection, "metadata_json", {}) or {}
        if meta.get("fixture_dir"):
            p = Path(str(meta["fixture_dir"])) / "holdings.json"
            if p.exists():
                holdings = json.loads(p.read_text())
            else:
                holdings = meta.get("fixture_holdings") or {"items": []}
        else:
            holdings = meta.get("fixture_holdings") or {"items": []}
        out = dict(holdings)
        from src.utils.time import utcnow

        out["as_of"] = (as_of or utcnow()).isoformat()
        return out

    def test_connection(self, connection: Any) -> dict[str, Any]:
        try:
            creds = getattr(connection, "credentials", None) or {}
            has_token = bool(creds.get("IB_YODLEE_TOKEN"))
            has_qid = bool(creds.get("IB_YODLEE_QUERY_ID"))
            accounts = self.fetch_accounts(connection)
            if not accounts:
                return {"ok": False, "message": "No accounts found in fixtures.", "credentials_present": has_token and has_qid}
            return {"ok": True, "message": "OK (fixtures)", "credentials_present": has_token and has_qid}
        except Exception as e:
            return {"ok": False, "message": f"FAIL (fixtures): {type(e).__name__}: {e}"}
