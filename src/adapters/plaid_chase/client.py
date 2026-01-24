from __future__ import annotations

import datetime as dt
import json
import os
from dataclasses import dataclass
from typing import Any, Optional

from src.core.net import assert_url_allowed, http_request, network_enabled
from src.importers.adapters import ProviderError


def _as_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _env_upper(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip().upper()


def _plaid_base_url(env: str) -> str:
    e = (env or "production").strip().lower()
    if e in {"prod", "production"}:
        host = "production.plaid.com"
    elif e in {"dev", "development"}:
        # Plaid no longer reliably resolves a dedicated "development" hostname.
        # Use sandbox host for both "sandbox" and "development" environments.
        host = "sandbox.plaid.com"
    else:
        host = "sandbox.plaid.com"
    return f"https://{host}"


@dataclass(frozen=True)
class PlaidErrorInfo:
    error_code: str
    error_type: str
    error_message: str
    request_id: str | None = None

    @property
    def is_item_login_required(self) -> bool:
        return (self.error_code or "").upper() == "ITEM_LOGIN_REQUIRED"


class PlaidApiError(ProviderError):
    def __init__(self, info: PlaidErrorInfo):
        super().__init__(f"{info.error_code}: {info.error_message}".strip(": "))
        self.info = info


class PlaidClient:
    """
    Minimal Plaid client wrapper using Investor's network allowlist + retry logic.

    This keeps outbound requests consistent with other live connectors (Yodlee / Finnhub),
    and avoids leaking secrets in error messages.
    """

    def __init__(
        self,
        *,
        env: str | None = None,
        client_id: str | None = None,
        secret: str | None = None,
    ) -> None:
        self.env = (env or os.environ.get("PLAID_ENV") or "production").strip().lower()
        self.client_id = (client_id or os.environ.get("PLAID_CLIENT_ID") or "").strip()
        self.secret = (secret or os.environ.get("PLAID_SECRET") or "").strip()
        base_override = (os.environ.get("PLAID_BASE_URL") or "").strip()
        self.base_url = base_override or _plaid_base_url(self.env)
        self.verify_tls = not ((os.environ.get("PLAID_INSECURE_SKIP_VERIFY") or "").strip().lower() in {"1", "true", "yes", "y", "on"})

    def _require_ready(self) -> None:
        if not network_enabled():
            raise ProviderError("Network disabled; set NETWORK_ENABLED=1 to enable live connectors.")
        if not self.client_id or not self.secret:
            raise ProviderError("PLAID_CLIENT_ID and PLAID_SECRET are required.")
        # Enforce allowlist early with the final host.
        assert_url_allowed(self.base_url)

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_ready()
        url = f"{self.base_url}{path}"
        # Allowlist enforcement for full URL.
        assert_url_allowed(url)
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        resp = http_request(
            url,
            method="POST",
            headers={"Content-Type": "application/json"},
            body=body,
            timeout_s=45.0,
            max_retries=2,
            backoff_s=0.5,
            verify_tls=bool(self.verify_tls),
            raise_for_status=False,
        )
        try:
            data = json.loads(resp.content.decode("utf-8", errors="replace") or "{}")
        except Exception:
            data = {}

        if int(resp.status_code) >= 400 or ("error_code" in data and data.get("error_code")):
            info = PlaidErrorInfo(
                error_code=_as_str(data.get("error_code") or f"HTTP_{resp.status_code}"),
                error_type=_as_str(data.get("error_type") or ""),
                error_message=_as_str(data.get("error_message") or "Plaid request failed"),
                request_id=_as_str(data.get("request_id")) or None,
            )
            raise PlaidApiError(info)
        if not isinstance(data, dict):
            raise ProviderError("Plaid response was not a JSON object.")
        return data

    def create_link_token(
        self,
        *,
        client_user_id: str,
        redirect_uri: str | None = None,
        products: Optional[list[str]] = None,
        country_codes: Optional[list[str]] = None,
    ) -> str:
        """
        Create a Plaid Link token for OAuth-capable institutions (Chase).
        """
        prods = products or ["transactions", "investments"]
        ccs = country_codes or ["US"]
        payload: dict[str, Any] = {
            "client_id": self.client_id,
            "secret": self.secret,
            "client_name": "Investor",
            "language": "en",
            "products": prods,
            "country_codes": ccs,
            "user": {"client_user_id": str(client_user_id)},
        }
        if redirect_uri:
            payload["redirect_uri"] = redirect_uri
        data = self._post_json("/link/token/create", payload)
        link_token = _as_str(data.get("link_token"))
        if not link_token:
            raise ProviderError("Plaid did not return link_token.")
        return link_token

    def exchange_public_token(self, *, public_token: str) -> tuple[str, str]:
        payload = {"client_id": self.client_id, "secret": self.secret, "public_token": public_token}
        data = self._post_json("/item/public_token/exchange", payload)
        access_token = _as_str(data.get("access_token"))
        item_id = _as_str(data.get("item_id"))
        if not access_token or not item_id:
            raise ProviderError("Plaid did not return access_token/item_id.")
        return access_token, item_id

    def get_accounts(self, *, access_token: str) -> list[dict[str, Any]]:
        payload = {"client_id": self.client_id, "secret": self.secret, "access_token": access_token}
        data = self._post_json("/accounts/get", payload)
        rows = data.get("accounts")
        if not isinstance(rows, list):
            return []
        return [r for r in rows if isinstance(r, dict)]

    def transactions_sync(
        self,
        *,
        access_token: str,
        cursor: str | None,
        count: int = 500,
    ) -> dict[str, Any]:
        """
        Cursor-based incremental sync for bank/credit transactions.

        We intentionally ignore start/end date here: Plaid sync is cursor-driven and returns deltas.
        """
        payload: dict[str, Any] = {
            "client_id": self.client_id,
            "secret": self.secret,
            "access_token": access_token,
            "cursor": cursor or "",
            "count": int(count),
        }
        return self._post_json("/transactions/sync", payload)

    def investments_holdings_get(self, *, access_token: str) -> dict[str, Any]:
        """
        Best-effort holdings pull. Many Chase items may not support investments.
        """
        payload = {"client_id": self.client_id, "secret": self.secret, "access_token": access_token}
        return self._post_json("/investments/holdings/get", payload)

    def investments_transactions_get(
        self,
        *,
        access_token: str,
        start_date: dt.date,
        end_date: dt.date,
        offset: int = 0,
        count: int = 500,
    ) -> dict[str, Any]:
        """
        Date-range investment transactions pull (paginated by offset/count).
        """
        payload: dict[str, Any] = {
            "client_id": self.client_id,
            "secret": self.secret,
            "access_token": access_token,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "options": {"offset": int(offset), "count": int(count)},
        }
        return self._post_json("/investments/transactions/get", payload)

    def transactions_get(
        self,
        *,
        access_token: str,
        start_date: dt.date,
        end_date: dt.date,
        offset: int = 0,
        count: int = 500,
    ) -> dict[str, Any]:
        """
        Date-range bank/credit transactions pull (paginated by offset/count).
        """
        payload: dict[str, Any] = {
            "client_id": self.client_id,
            "secret": self.secret,
            "access_token": access_token,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "options": {"offset": int(offset), "count": int(count)},
        }
        return self._post_json("/transactions/get", payload)

    def liabilities_get(self, *, access_token: str) -> dict[str, Any]:
        """
        Fetch liabilities (credit cards, student loans, mortgages) for the item.
        """
        payload = {"client_id": self.client_id, "secret": self.secret, "access_token": access_token}
        return self._post_json("/liabilities/get", payload)


def parse_plaid_date(s: str) -> dt.date | None:
    v = (s or "").strip()
    if not v:
        return None
    try:
        return dt.date.fromisoformat(v[:10])
    except Exception:
        return None
