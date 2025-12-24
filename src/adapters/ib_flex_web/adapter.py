from __future__ import annotations

import datetime as dt
import hashlib
import logging
import os
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from src.core.net import http_get
from src.importers.adapters import BrokerAdapter, ProviderError, RangeTooLargeError
from src.utils.time import utcnow
from src.utils.rate_limit import mask_secret, rate_limit_sleep, token_serial_lock

# Reuse the offline adapter's robust normalization helpers (same conventions).
from src.adapters.ib_flex_offline.adapter import (  # noqa: E402
    _as_float_or_none,
    _classify_activity_row,
    _extract_cash_amount,
    _extract_currency,
    _get_any,
    _sha256_bytes,
)


log = logging.getLogger(__name__)


def _strip_ns(tag: str) -> str:
    if not tag:
        return ""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _parse_ib_dt_raw(value: str | None) -> Optional[dt.datetime]:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    # IB Flex uses "YYYYMMDD;HHMMSS" for DateTime-like fields.
    if ";" in s:
        d, t = s.split(";", 1)
        d = d.strip()
        t = t.strip()
        if len(d) >= 8 and d[:8].isdigit():
            try:
                base = dt.datetime.strptime(d[:8], "%Y%m%d").date()
            except Exception:
                base = None
            if base:
                hh = int(t[0:2]) if len(t) >= 2 and t[0:2].isdigit() else 0
                mm = int(t[2:4]) if len(t) >= 4 and t[2:4].isdigit() else 0
                ss = int(t[4:6]) if len(t) >= 6 and t[4:6].isdigit() else 0
                return dt.datetime(base.year, base.month, base.day, hh, mm, ss, tzinfo=dt.timezone.utc)
    # Date-only
    if len(s) >= 8 and s[:8].isdigit():
        try:
            d = dt.datetime.strptime(s[:8], "%Y%m%d").date()
            return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
        except Exception:
            return None
    try:
        d = dt.date.fromisoformat(s[:10])
        return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
    except Exception:
        return None


def _parse_ib_date(value: str | None) -> Optional[dt.date]:
    dtv = _parse_ib_dt_raw(value)
    return dtv.date() if dtv else None


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _redact(s: str, secrets: list[str]) -> str:
    out = s
    for sec in secrets:
        if sec:
            out = out.replace(sec, "[REDACTED]")
    return out


def _base_url() -> str:
    # IB Flex Web Service host varies by environment; ndcdyn is commonly used in the IB Portal UI.
    # Can be overridden for testing (must still pass allowlist).
    return (os.environ.get("IB_FLEX_WEB_BASE_URL") or "https://ndcdyn.interactivebrokers.com/Universal/servlet/").strip()


def _candidate_base_urls() -> list[str]:
    """
    Try a small allowlisted set of known IB Flex Web Service bases.

    Override via:
      - IB_FLEX_WEB_BASE_URLS="https://ndcdyn.../Universal/servlet/,https://gdcdyn.../Universal/servlet/"
      - IB_FLEX_WEB_BASE_URL="https://.../Universal/servlet/"
    """
    raw = (os.environ.get("IB_FLEX_WEB_BASE_URLS") or "").strip()
    bases: list[str]
    if raw:
        bases = [p.strip() for p in raw.split(",") if p.strip()]
    else:
        bases = [
            _base_url(),
            "https://www.interactivebrokers.com/Universal/servlet/",
        ]
    out: list[str] = []
    for b in bases:
        s = (b or "").strip()
        if not s:
            continue
        if not s.endswith("/"):
            s += "/"
        if s not in out:
            out.append(s)
    return out


_QUERY_SPLIT_RE = re.compile(r"[\s,;]+")


def _split_query_tokens(raw: str) -> list[str]:
    s = (raw or "").strip()
    if not s:
        return []
    # Allow users to paste comma/semicolon/whitespace-separated lists into a single field.
    parts = [p.strip() for p in _QUERY_SPLIT_RE.split(s) if p.strip()]
    # De-dupe while preserving order.
    out: list[str] = []
    for p in parts:
        if p not in out:
            out.append(p)
    return out


@dataclass(frozen=True)
class FlexReport:
    query_id: str
    reference_code: str
    payload: bytes
    payload_hash: str
    payload_path: Optional[str]


class IBFlexWebAdapter(BrokerAdapter):
    """
    Live IB Flex Web Service adapter (manual refresh; no broker sync automation).

    Credentials (AdapterConnectionContext.credentials):
      - IB_FLEX_TOKEN (required)
      - IB_FLEX_QUERY_ID (required)

    Connection metadata_json:
      - extra_query_ids: optional list[str] (additional Flex query ids)
    """

    @property
    def page_size(self) -> int:
        # One report per page (per query id).
        return 1

    def _token(self, connection: Any) -> str:
        creds = getattr(connection, "credentials", None) or {}
        token = (creds.get("IB_FLEX_TOKEN") or creds.get("IB_YODLEE_TOKEN") or "").strip()
        if not token:
            raise ProviderError("Missing IB Flex token for this connection.")
        return token

    def _warn(self, connection: Any, msg: str) -> None:
        rs = getattr(connection, "run_settings", None) or {}
        warns = rs.setdefault("adapter_warnings", [])
        if isinstance(warns, list):
            warns.append(str(msg))

    def _is_rate_limit_1018(self, code: str | None, msg: str | None) -> bool:
        c = (code or "").strip()
        m = (msg or "").lower()
        return c == "1018" or ("too many requests" in m) or (" 1018" in m) or m.startswith("1018")

    def _query_audit(self, connection: Any, *, token: str, query_id: str, update: dict[str, Any]) -> None:
        """
        Per-query audit information persisted into SyncRun.coverage_json by sync_runner.
        Secrets are masked (token/query id last4 only).
        """
        rs = getattr(connection, "run_settings", None) or {}
        rows = rs.setdefault("_ib_flex_web_query_audit", [])
        if not isinstance(rows, list):
            rows = []
            rs["_ib_flex_web_query_audit"] = rows
        key = {"token": mask_secret(token), "query": "****" + str(query_id)[-4:]}
        for r in rows:
            if isinstance(r, dict) and r.get("token") == key["token"] and r.get("query") == key["query"]:
                r.update(update)
                return
        rows.append({**key, **update})

    def _get_user_info(self, *, token: str, connection: Any) -> list[dict[str, str]]:
        rs = getattr(connection, "run_settings", None) or {}
        cached = rs.get("_ib_flex_userinfo")
        if isinstance(cached, list):
            return [c for c in cached if isinstance(c, dict)]

        qs = {"t": token, "v": "3"}
        data: bytes = b""
        errors: list[str] = []
        only_404 = True
        for base in _candidate_base_urls():
            try:
                url = urllib.parse.urljoin(base, "FlexStatementService.GetUserInfo") + "?" + urllib.parse.urlencode(qs)
                resp = http_get(url, timeout_s=30.0, max_retries=2)
                data = resp.content or b""
                errors = []
                only_404 = False
                break
            except Exception as e:
                # Never include secrets; include host/path from http_get errors when available.
                try:
                    host = urllib.parse.urlparse(base).hostname or base
                except Exception:
                    host = base
                e_s = _redact(str(e), [token])
                errors.append(f"{host}: {type(e).__name__}: {e_s}")
                if "status=404" not in e_s:
                    only_404 = False
                continue
        if not data:
            preview = " ; ".join(errors[:3])
            more = f" (+{len(errors)-3} more)" if len(errors) > 3 else ""
            if errors and only_404:
                raise ProviderError(
                    "IB Flex GetUserInfo endpoint was not found (HTTP 404) on all candidate hosts. "
                    "This environment cannot resolve Flex Query *names* to ids. "
                    "Use numeric Flex Query IDs (e.g. 1354277) for the primary and extra queries. "
                    f"Details: {preview}{more}"
                )
            raise ProviderError(f"IB Flex GetUserInfo failed for all candidate hosts: {preview}{more}")
        try:
            root = ET.fromstring(data)
        except Exception:
            raise ProviderError("IB Flex GetUserInfo returned non-XML response.")

        status = (root.findtext(".//Status") or "").strip()
        if status.lower() != "success":
            code = (root.findtext(".//ErrorCode") or "").strip()
            msg = (root.findtext(".//ErrorMessage") or "").strip()
            txt = f"IB Flex GetUserInfo failed ({code}): {msg}" if code or msg else "IB Flex GetUserInfo failed."
            raise ProviderError(txt)

        out: list[dict[str, str]] = []
        for el in root.iter():
            attrs = getattr(el, "attrib", {}) or {}
            # Most commonly: <Query id="123" name="MyQuery"/>
            qid = (
                attrs.get("id")
                or attrs.get("queryId")
                or attrs.get("queryID")
                or attrs.get("queryid")
                or attrs.get("query_id")
            )
            name = attrs.get("name") or attrs.get("queryName") or attrs.get("query_name")
            if qid and name:
                qid_s = str(qid).strip()
                name_s = str(name).strip()
                if qid_s and name_s:
                    out.append({"id": qid_s, "name": name_s})
        # De-dupe by id.
        uniq: dict[str, dict[str, str]] = {}
        for r in out:
            if r.get("id") and r["id"] not in uniq:
                uniq[r["id"]] = r
        out = list(uniq.values())
        rs["_ib_flex_userinfo"] = out
        return out

    def _resolve_query_id(self, *, token: str, query: str, connection: Any) -> str:
        q = (query or "").strip()
        if not q:
            raise ProviderError("Missing IB Flex Query identifier for this connection.")
        if q.isdigit():
            return q
        # Treat as query name; resolve via GetUserInfo.
        infos = self._get_user_info(token=token, connection=connection)
        if infos:
            q_lower = q.lower()
            for r in infos:
                if str(r.get("name") or "").strip().lower() == q_lower:
                    return str(r.get("id") or "").strip()
        # Provide a short hint with available names (no secrets).
        names = [str(r.get("name") or "").strip() for r in infos if str(r.get("name") or "").strip()]
        names = sorted(set(names))
        preview = ", ".join(names[:10])
        more = f" (+{len(names)-10} more)" if len(names) > 10 else ""
        raise ProviderError(
            "IB Flex Query is invalid or not found. "
            "Use the Flex Query *name* shown in IB Portal (Reports → Flex Queries), or the numeric query id if available. "
            f"Available queries: {preview}{more}"
        )

    def _query_ids(self, connection: Any) -> list[str]:
        creds = getattr(connection, "credentials", None) or {}
        primary_raw = (creds.get("IB_FLEX_QUERY_ID") or creds.get("IB_YODLEE_QUERY_ID") or "").strip()
        primary = _split_query_tokens(primary_raw)
        if not primary:
            raise ProviderError("Missing IB Flex Query ID for this connection.")
        meta = getattr(connection, "metadata_json", {}) or {}
        extra = meta.get("extra_query_ids") or []
        out: list[str] = list(primary)
        if isinstance(extra, list):
            for q in extra:
                for tok in _split_query_tokens(str(q or "")):
                    if tok and tok not in out:
                        out.append(tok)
        return out

    def _store_payloads(self, connection: Any) -> bool:
        rs = getattr(connection, "run_settings", None) or {}
        return bool(rs.get("store_payloads"))

    def _payload_dir(self, connection: Any) -> Path:
        cid = getattr(connection, "id", None) or getattr(getattr(connection, "connection", None), "id", None)
        conn_id = int(cid) if cid is not None else 0
        p = Path("data") / "external" / f"conn_{conn_id}" / "payloads"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _send_request(self, *, base_url: str, token: str, query_id: str, start: dt.date, end: dt.date) -> str:
        # FlexStatementService.SendRequest?t=TOKEN&q=QUERYID&v=3[&s=YYYYMMDD&e=YYYYMMDD]
        qs = {
            "t": token,
            "q": query_id,
            "v": "3",
            "s": _yyyymmdd(start),
            "e": _yyyymmdd(end),
        }
        url = urllib.parse.urljoin(base_url, "FlexStatementService.SendRequest") + "?" + urllib.parse.urlencode(qs)
        resp = http_get(url, timeout_s=30.0, max_retries=2)
        data = resp.content or b""
        try:
            root = ET.fromstring(data)
        except Exception:
            raise ProviderError("IB Flex SendRequest returned non-XML response.")

        tag = _strip_ns(root.tag).lower()
        if tag not in {"flexstatementresponse", "flexstatementservice", "flexstatementresponse"}:
            # Some responses wrap; still try to locate fields.
            pass
        status = (root.findtext(".//Status") or "").strip()
        if status.lower() != "success":
            code = (root.findtext(".//ErrorCode") or "").strip()
            msg = (root.findtext(".//ErrorMessage") or "").strip()
            txt = f"IB Flex SendRequest failed ({code}): {msg}" if code or msg else "IB Flex SendRequest failed."
            if self._is_rate_limit_1018(code, msg):
                raise ProviderError("IB Flex SendRequest failed (1018): Too many requests have been made from this token.")
            if code == "1014":
                txt += " Hint: The configured Flex Query identifier is invalid. Use the Flex Query *name* from IB Portal (Reports → Flex Queries), or the numeric id if available."
            low = (msg or "").lower()
            if "range" in low and "too" in low:
                raise RangeTooLargeError(txt)
            raise ProviderError(txt)
        ref = (root.findtext(".//ReferenceCode") or "").strip()
        if not ref:
            raise ProviderError("IB Flex SendRequest succeeded but returned no ReferenceCode.")
        return ref

    def _get_statement(self, *, base_url: str, token: str, reference_code: str) -> bytes:
        # FlexStatementService.GetStatement?t=TOKEN&q=REFERENCE&v=3
        qs = {"t": token, "q": reference_code, "v": "3"}
        url = urllib.parse.urljoin(base_url, "FlexStatementService.GetStatement") + "?" + urllib.parse.urlencode(qs)
        resp = http_get(url, timeout_s=60.0, max_retries=2)
        return resp.content or b""

    def _get_or_download_report(
        self,
        connection: Any,
        *,
        query_id: str,
        start: dt.date,
        end: dt.date,
    ) -> FlexReport:
        rs = getattr(connection, "run_settings", None) or {}
        cache = rs.setdefault("_ib_flex_web_cache", {})
        if not isinstance(cache, dict):
            cache = {}
            rs["_ib_flex_web_cache"] = cache
        key = f"{query_id}:{start.isoformat()}:{end.isoformat()}"
        existing = cache.get(key)
        if isinstance(existing, FlexReport):
            return existing
        token = self._token(connection)
        rep = self._download_report(connection, token=token, query_id=query_id, start=start, end=end)
        cache[key] = rep
        return rep

    def _get_cached_reports(
        self,
        connection: Any,
        *,
        start: dt.date,
        end: dt.date,
    ) -> list[FlexReport]:
        # Kept for backward compatibility (older callers may still use it), but intentionally does NOT
        # download all queries in one shot. The web connector now downloads one query per cursor page.
        token = self._token(connection)
        query_ids_raw = self._query_ids(connection)
        reports: list[FlexReport] = []
        for q_raw in query_ids_raw:
            qid = self._resolve_query_id(token=token, query=q_raw, connection=connection)
            reports.append(self._get_or_download_report(connection, query_id=qid, start=start, end=end))
            break
        if not reports:
            raise ProviderError("No Flex reports could be downloaded (no queries configured).")
        return reports

    def _download_report(self, connection: Any, *, token: str, query_id: str, start: dt.date, end: dt.date) -> FlexReport:
        secrets = [token, query_id]
        last_env_msg: str | None = None
        last_err: Exception | None = None
        token_masked = mask_secret(token)
        query_masked = "****" + str(query_id)[-4:]
        rate_limit_sleep_total = 0.0

        # Rate limits are per token; serialize SendRequest+polling in-process for this token.
        with token_serial_lock(token):
            for base in _candidate_base_urls():
                # SendRequest retry/backoff on 1018.
                ref: str | None = None
                retry_count = 0
                base_unusable = False
                for attempt in range(1, 6):  # 5 attempts total
                    try:
                        rate_limit_sleep_total += rate_limit_sleep(token=token, action="send_request", min_interval_s=5.0)
                        ref = self._send_request(base_url=base, token=token, query_id=query_id, start=start, end=end)
                        break
                    except RangeTooLargeError:
                        raise
                    except ProviderError as e:
                        # Endpoint/host issues: try the next base URL.
                        if "status=404" in str(e) or "status=403" in str(e):
                            last_err = e
                            base_unusable = True
                            break
                        if "1018" not in str(e):
                            raise
                        if attempt >= 5:
                            self._warn(connection, f"IB Flex rate limit (1018): skipped query {query_masked} after retries.")
                            self._query_audit(
                                connection,
                                token=token,
                                query_id=query_id,
                                update={"status": "SKIPPED_RATE_LIMIT", "retry_count": retry_count, "rate_limit_sleep_s": round(rate_limit_sleep_total, 3)},
                            )
                            raise
                        backoff = float(5 * (2 ** (attempt - 1)))
                        retry_count += 1
                        self._query_audit(
                            connection,
                            token=token,
                            query_id=query_id,
                            update={"status": "RETRYING_RATE_LIMIT", "retry_count": retry_count, "last_backoff_s": backoff, "rate_limit_sleep_s": round(rate_limit_sleep_total, 3)},
                        )
                        time.sleep(backoff)
                        continue

                if base_unusable:
                    continue
                if not ref:
                    continue

                # GetStatement polling
                max_polls = 10
                polls = 0
                poll_retry_count = 0
                while polls < max_polls:
                    rate_limit_sleep_total += rate_limit_sleep(token=token, action="poll", min_interval_s=2.0)
                    data = self._get_statement(base_url=base, token=token, reference_code=ref)
                    # If we got a status envelope, decide whether to continue.
                    try:
                        root = ET.fromstring(data)
                        rtag = _strip_ns(root.tag).lower()
                        if rtag == "flexstatementresponse":
                            status = (root.findtext(".//Status") or "").strip()
                            code = (root.findtext(".//ErrorCode") or "").strip()
                            err = (root.findtext(".//ErrorMessage") or "").strip()
                            last_env_msg = f"{status} {code} {err}".strip()
                            low = (err or "").lower()
                            if self._is_rate_limit_1018(code, err):
                                # Exponential backoff on 1018, max 5 attempts.
                                if poll_retry_count >= 5:
                                    self._warn(connection, f"IB Flex rate limit (1018): skipped query {query_masked} during polling.")
                                    self._query_audit(
                                        connection,
                                        token=token,
                                        query_id=query_id,
                                        update={"status": "SKIPPED_RATE_LIMIT", "polls": polls, "poll_retries": poll_retry_count, "rate_limit_sleep_s": round(rate_limit_sleep_total, 3)},
                                    )
                                    raise ProviderError("IB Flex GetStatement rate limit (1018) exceeded retries.")
                                backoff = float(5 * (2 ** poll_retry_count))
                                poll_retry_count += 1
                                self._query_audit(
                                    connection,
                                    token=token,
                                    query_id=query_id,
                                    update={"status": "RETRYING_RATE_LIMIT", "poll_retries": poll_retry_count, "last_backoff_s": backoff, "rate_limit_sleep_s": round(rate_limit_sleep_total, 3)},
                                )
                                time.sleep(backoff)
                                continue
                            if "not ready" in low or code in {"1019"}:
                                polls += 1
                                continue
                            if status.lower() == "success" and not err and not code:
                                polls += 1
                                continue
                            raise ProviderError(f"IB Flex GetStatement failed ({code}): {err}")
                    except ET.ParseError:
                        # Not an envelope -> treat as report.
                        pass
                    except ProviderError:
                        raise
                    except Exception:
                        # If we can't parse as XML, still treat as report content.
                        pass

                    payload_hash = _sha256_hex(data)
                    payload_path: Optional[str] = None
                    if self._store_payloads(connection):
                        try:
                            pdir = self._payload_dir(connection)
                            fname = f"ib_flex_web_{query_id}_{start.isoformat()}_{end.isoformat()}_{payload_hash[:12]}.xml"
                            p = pdir / fname
                            if not p.exists():
                                p.write_bytes(data)
                            payload_path = str(p)
                        except Exception:
                            payload_path = None
                    self._query_audit(
                        connection,
                        token=token,
                        query_id=query_id,
                        update={
                            "status": "SUCCESS",
                            "polls": polls,
                            "retry_count": retry_count,
                            "poll_retries": poll_retry_count,
                            "rate_limit_sleep_s": round(rate_limit_sleep_total, 3),
                        },
                    )
                    log.debug("IB Flex report downloaded (token %s, query %s)", token_masked, query_masked)
                    return FlexReport(
                        query_id=query_id,
                        reference_code=ref,
                        payload=data,
                        payload_hash=payload_hash,
                        payload_path=payload_path,
                    )

                # Poll exhausted for this base; abort this query only.
                self._warn(connection, f"IB Flex report not ready after {max_polls} polls; skipped query {query_masked}.")
                self._query_audit(
                    connection,
                    token=token,
                    query_id=query_id,
                    update={
                        "status": "NOT_READY",
                        "polls": max_polls,
                        "retry_count": retry_count,
                        "poll_retries": poll_retry_count,
                        "rate_limit_sleep_s": round(rate_limit_sleep_total, 3),
                    },
                )
                last_err = ProviderError(last_env_msg or "report not ready")
                continue

        if last_env_msg:
            raise ProviderError(_redact(f"IB Flex report not ready after polling ({last_env_msg}).", secrets))
        if last_err is not None:
            bases = ", ".join(_candidate_base_urls())
            raise ProviderError(_redact(f"IB Flex download failed across base URLs [{bases}]: {type(last_err).__name__}: {last_err}", secrets))
        raise ProviderError(_redact("IB Flex download failed (unknown).", secrets))

    def _accounts_from_report_xml(self, payload: bytes) -> set[str]:
        out: set[str] = set()
        try:
            root = ET.fromstring(payload)
        except Exception:
            return out
        for el in root.iter():
            attrs = getattr(el, "attrib", {}) or {}
            for k in ("accountId", "accountID", "clientAccountID", "ClientAccountID"):
                v = attrs.get(k)
                if v:
                    s = str(v).strip()
                    if s and s != "-":
                        out.add(s)
        return out

    def fetch_accounts(self, connection: Any) -> list[dict[str, Any]]:
        rs = getattr(connection, "run_settings", None) or {}
        # Best effort: use the effective range the sync runner computed, so we can reuse cached reports.
        start_s = rs.get("effective_start_date")
        end_s = rs.get("effective_end_date")
        today = utcnow().date()
        try:
            start = dt.date.fromisoformat(str(start_s)) if start_s else today
            end = dt.date.fromisoformat(str(end_s)) if end_s else today
        except Exception:
            start, end = today, today

        # Do not submit all Flex queries up-front. Prefer any already-cached report (range negotiation may have
        # fetched one) and otherwise try queries until we can infer account ids.
        token = self._token(connection)
        query_ids_raw = self._query_ids(connection)
        names: set[str] = set()
        cache = rs.get("_ib_flex_web_cache") or {}
        if isinstance(cache, dict) and cache:
            for v in cache.values():
                if isinstance(v, FlexReport):
                    names |= self._accounts_from_report_xml(v.payload)
                if names:
                    break
        if not names:
            for q_raw in query_ids_raw:
                try:
                    qid = self._resolve_query_id(token=token, query=q_raw, connection=connection)
                except ProviderError as e:
                    self._warn(connection, f"Skipped Flex Query '{q_raw}' while inferring accounts: {e}")
                    continue
                try:
                    rep = self._get_or_download_report(connection, query_id=qid, start=start, end=end)
                except ProviderError as e:
                    self._warn(connection, f"Failed Flex Query '{q_raw}' while inferring accounts: {e}")
                    continue
                names |= self._accounts_from_report_xml(rep.payload)
                if names:
                    break
        if not names:
            # Fallback name (single account); sync runner will still map transactions into this account if needed.
            return [{"provider_account_id": "IBFLEX-1", "name": "IB Flex Web", "account_type": "TAXABLE"}]
        out: list[dict[str, Any]] = []
        for name in sorted(names):
            out.append({"provider_account_id": f"IBFLEX:{name}", "name": name, "account_type": "TAXABLE"})
        return out

    def test_connection(self, connection: Any) -> dict[str, Any]:
        try:
            token = self._token(connection)
            query_ids = self._query_ids(connection)
        except Exception as e:
            return {"ok": False, "message": str(e)}
        today = utcnow().date()
        try:
            # Fetch a single report for today; resolve query *names* via GetUserInfo when needed.
            qid = self._resolve_query_id(token=token, query=query_ids[0], connection=connection)
            rep = self._download_report(connection, token=token, query_id=qid, start=today, end=today)
            try:
                ET.fromstring(rep.payload)
            except Exception:
                return {"ok": False, "message": "Fetched report but it was not valid XML."}
            return {"ok": True, "message": "OK (IB Flex Web Service)", "payload_hash_prefix": rep.payload_hash[:12]}
        except Exception as e:
            # Ensure token/query id are not leaked.
            msg = _redact(f"{type(e).__name__}: {e}", [token] + query_ids)
            return {"ok": False, "message": msg}

    def _parse_report_transactions(self, rep: FlexReport) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        """
        Returns:
          - items: normalized transaction-like records for sync runner
          - holdings: dict(as_of, items, payload_hashes)
          - metrics: coverage-ish counters for this report
        """
        metrics: dict[str, Any] = {
            "payload_hash": rep.payload_hash,
            "query_id": rep.query_id,
            "trades_seen": 0,
            "cash_rows_seen": 0,
            "holdings_items_seen": 0,
            "closed_lot_rows_seen": 0,
            "wash_sale_rows_seen": 0,
        }
        items: list[dict[str, Any]] = []

        # Report payload marker for sync_runner idempotency.
        items.append(
            {
                "record_kind": "REPORT_PAYLOAD",
                "payload_hash": rep.payload_hash,
                "source": "IB_FLEX_WEB",
                "query_id": rep.query_id,
                "reference_code": rep.reference_code,
                "payload_path": rep.payload_path,
                "bytes": len(rep.payload),
            }
        )

        try:
            root = ET.fromstring(rep.payload)
        except Exception:
            raise ProviderError("IB Flex report payload was not XML.")

        # Holdings snapshot (Open Positions).
        holdings_items: list[dict[str, Any]] = []
        for el in root.iter():
            tag = _strip_ns(el.tag)
            if tag not in {"OpenPosition", "OpenPositions", "Position", "OpenPositionsSummary"}:
                continue
            # Only leaf records with symbol/qty fields.
            attrs = getattr(el, "attrib", {}) or {}
            sym = (attrs.get("symbol") or attrs.get("Symbol") or attrs.get("underlyingSymbol") or "").strip()
            qty_s = attrs.get("position") or attrs.get("quantity") or attrs.get("qty") or attrs.get("Position") or attrs.get("Quantity")
            mv_s = attrs.get("marketValue") or attrs.get("positionValue") or attrs.get("value") or attrs.get("MarketValue")
            if not sym or qty_s in (None, ""):
                continue
            qty = _as_float_or_none(qty_s)
            if qty is None:
                continue
            mv = _as_float_or_none(mv_s) if mv_s not in (None, "") else None
            basis = _as_float_or_none(attrs.get("costBasis") or attrs.get("costBasisMoney") or attrs.get("CostBasis"))
            acct = (attrs.get("accountId") or attrs.get("clientAccountID") or attrs.get("ClientAccountID") or "").strip()
            provider_account_id = f"IBFLEX:{acct}" if acct else "IBFLEX-1"
            sym_u = sym.strip().upper()
            # Cash positions often show symbol="USD" or similar; normalize to CASH:USD for internal cash fallback.
            asset_class = (attrs.get("assetClass") or attrs.get("AssetClass") or "").strip().upper()
            if asset_class == "CASH" or sym_u in {"USD", "CASH", "CASHUSD"}:
                sym_u = "CASH:USD"
            holdings_items.append(
                {
                    "provider_account_id": provider_account_id,
                    "symbol": sym_u,
                    "qty": float(qty),
                    "market_value": float(mv) if mv is not None else None,
                    "cost_basis_total": float(basis) if basis is not None else None,
                    "source": "IB Flex (Web)",
                }
            )
        metrics["holdings_items_seen"] = len([h for h in holdings_items if not str(h.get("symbol") or "").startswith("CASH:")])

        # Trades + multi-detail trade rows (CLOSED_LOT/WASH_SALE).
        for el in root.iter():
            tag = _strip_ns(el.tag)
            if tag != "Trade":
                continue
            attrs = getattr(el, "attrib", {}) or {}
            level = (attrs.get("levelOfDetail") or attrs.get("LevelOfDetail") or "EXECUTION").strip().upper()
            acct = (attrs.get("accountId") or attrs.get("clientAccountID") or attrs.get("ClientAccountID") or "").strip()
            provider_account_id = f"IBFLEX:{acct}" if acct else "IBFLEX-1"
            symbol = (attrs.get("symbol") or attrs.get("Symbol") or "").strip().upper()
            trade_date = _parse_ib_date(str(attrs.get("tradeDate") or attrs.get("TradeDate") or attrs.get("dateTime") or attrs.get("DateTime") or "") or None)
            if trade_date is None:
                continue
            qty = _as_float_or_none(attrs.get("quantity") or attrs.get("Quantity"))
            # Build a row dict compatible with offline helpers.
            row: dict[str, Any] = {
                "ClientAccountID": acct,
                "DateTime": attrs.get("dateTime") or attrs.get("DateTime") or "",
                "TradeDate": attrs.get("tradeDate") or attrs.get("TradeDate") or "",
                "Symbol": symbol,
                "Quantity": qty,
                "Buy/Sell": attrs.get("buySell") or attrs.get("BuySell") or attrs.get("side") or attrs.get("Side") or "",
                "NetCash": attrs.get("netCash") or attrs.get("NetCash") or attrs.get("proceeds") or attrs.get("Proceeds") or attrs.get("tradeMoney") or attrs.get("TradeMoney") or "",
                "Description": attrs.get("description") or attrs.get("Description") or "",
                "Type": attrs.get("transactionType") or attrs.get("type") or attrs.get("Type") or "",
                "LevelOfDetail": level,
                "TransactionID": attrs.get("transactionID") or attrs.get("TransactionID") or attrs.get("transactionId") or "",
                "TradeID": attrs.get("tradeID") or attrs.get("TradeID") or attrs.get("tradeId") or "",
                "CostBasis": attrs.get("costBasis") or attrs.get("CostBasis") or "",
                "FifoPnlRealized": attrs.get("fifoPnlRealized") or attrs.get("FifoPnlRealized") or "",
                "OpenDateTime": attrs.get("openDateTime") or attrs.get("OpenDateTime") or "",
                "HoldingPeriodDateTime": attrs.get("holdingPeriodDateTime") or attrs.get("HoldingPeriodDateTime") or "",
                "WhenRealized": attrs.get("whenRealized") or attrs.get("WhenRealized") or "",
                "WhenReopened": attrs.get("whenReopened") or attrs.get("WhenReopened") or "",
                "CurrencyPrimary": attrs.get("currency") or attrs.get("currencyPrimary") or attrs.get("CurrencyPrimary") or "USD",
                "FXRateToBase": attrs.get("fxRateToBase") or attrs.get("FXRateToBase") or "",
                "Conid": attrs.get("conid") or attrs.get("Conid") or "",
            }
            desc = str(row.get("Description") or "")
            is_trade = True
            cash = _extract_cash_amount(row, is_trade=is_trade)
            tx_type = _classify_activity_row(row, qty=qty, cash=cash, description=desc)
            metrics["trades_seen"] += 1

            txid = str(row.get("TransactionID") or row.get("TradeID") or "").strip()
            if not txid:
                key = f"{provider_account_id}|{trade_date.isoformat()}|{level}|{symbol}|{qty or ''}|{cash or ''}|{desc}"
                txid = f"WEB:{rep.payload_hash}:{_sha256_bytes(key.encode('utf-8'))}"

            if level in {"CLOSED_LOT", "WASH_SALE"}:
                cost_basis = _as_float_or_none(row.get("CostBasis"))
                fifo_realized = _as_float_or_none(row.get("FifoPnlRealized"))
                proceeds = (cost_basis + fifo_realized) if (cost_basis is not None and fifo_realized is not None) else None
                items.append(
                    {
                        "record_kind": "BROKER_CLOSED_LOT" if level == "CLOSED_LOT" else "BROKER_WASH_SALE",
                        "provider_account_id": provider_account_id,
                        "symbol": symbol,
                        "date": trade_date.isoformat(),
                        "qty": abs(float(qty or 0.0)),
                        "cost_basis": cost_basis,
                        "realized_pl_fifo": fifo_realized,
                        "proceeds_derived": proceeds,
                        "currency": str(row.get("CurrencyPrimary") or "USD").strip().upper(),
                        "fx_rate_to_base": _as_float_or_none(row.get("FXRateToBase")),
                        "conid": row.get("Conid") or None,
                        "ib_transaction_id": row.get("TransactionID") or None,
                        "ib_trade_id": row.get("TradeID") or None,
                        "datetime_raw": row.get("DateTime") or None,
                        "open_datetime_raw": row.get("OpenDateTime") or None,
                        "holding_period_datetime_raw": row.get("HoldingPeriodDateTime") or None,
                        "when_realized_raw": row.get("WhenRealized") or None,
                        "when_reopened_raw": row.get("WhenReopened") or None,
                        "source_file": f"IB_FLEX_WEB:{rep.query_id}",
                        "source_row": None,
                        "source_file_hash": rep.payload_hash,
                        "raw_row": attrs,
                        "provider_transaction_id": txid,
                    }
                )
                if level == "CLOSED_LOT":
                    metrics["closed_lot_rows_seen"] += 1
                else:
                    metrics["wash_sale_rows_seen"] += 1
                continue

            # Executions -> main Transaction import path.
            amount = float(cash or 0.0) if cash is not None else 0.0
            qty_out: float | None = abs(float(qty)) if qty not in (None, "") else None
            if tx_type == "BUY":
                amount = -abs(amount)
                if qty_out is not None:
                    qty_out = abs(qty_out)
            elif tx_type == "SELL":
                amount = abs(amount)
                if qty_out is not None:
                    qty_out = abs(qty_out)
            items.append(
                {
                    "date": trade_date.isoformat(),
                    "type": tx_type,
                    "ticker": symbol or None,
                    "qty": qty_out,
                    "amount": float(amount),
                    "description": desc,
                    "provider_transaction_id": txid,
                    "provider_account_id": provider_account_id,
                    "source_file_hash": rep.payload_hash,
                    "currency": (_extract_currency(row) or "USD").strip().upper(),
                }
            )

        # Cash transactions.
        for el in root.iter():
            tag = _strip_ns(el.tag)
            if tag not in {"CashTransaction", "CashTransactions"}:
                continue
            attrs = getattr(el, "attrib", {}) or {}
            # Leaf records must have an Amount.
            amt_s = attrs.get("amount") or attrs.get("Amount")
            if amt_s in (None, ""):
                continue
            level = (attrs.get("levelOfDetail") or attrs.get("LevelOfDetail") or "").strip().upper()
            if level == "SUMMARY":
                continue
            acct = (attrs.get("accountId") or attrs.get("clientAccountID") or attrs.get("ClientAccountID") or "").strip()
            provider_account_id = f"IBFLEX:{acct}" if acct else "IBFLEX-1"
            dt_raw = str(attrs.get("dateTime") or attrs.get("DateTime") or attrs.get("date") or attrs.get("Date") or "")
            d = _parse_ib_date(dt_raw) or _parse_ib_date(str(attrs.get("reportDate") or attrs.get("ReportDate") or "") or None)
            if d is None:
                continue
            symbol = (attrs.get("symbol") or attrs.get("Symbol") or "").strip().upper() or None
            raw_type = (attrs.get("type") or attrs.get("Type") or attrs.get("transactionType") or "").strip()
            desc = (attrs.get("description") or attrs.get("Description") or "").strip()
            row = {
                "ClientAccountID": acct,
                "Date/Time": dt_raw,
                "Amount": amt_s,
                "Type": raw_type,
                "Description": desc or raw_type,
                "Symbol": symbol or "",
                "CurrencyPrimary": attrs.get("currency") or attrs.get("CurrencyPrimary") or "USD",
                "LevelOfDetail": level or "DETAIL",
            }
            amt = _as_float_or_none(amt_s)
            if amt is None:
                continue
            tx_type = _classify_activity_row(row, qty=None, cash=float(amt), description=str(desc or raw_type))
            # App conventions for cashflows
            if tx_type == "WITHHOLDING":
                amt = abs(float(amt))
            elif tx_type == "FEE":
                amt = -abs(float(amt))
            txid = (
                str(attrs.get("transactionID") or attrs.get("TransactionID") or attrs.get("tradeID") or attrs.get("TradeID") or "").strip()
            )
            if not txid:
                key = f"{provider_account_id}|{d.isoformat()}|{tx_type}|{symbol or ''}|{amt}|{desc or raw_type}"
                txid = f"WEB:{rep.payload_hash}:{_sha256_bytes(key.encode('utf-8'))}"
            metrics["cash_rows_seen"] += 1
            items.append(
                {
                    "date": d.isoformat(),
                    "type": tx_type,
                    "ticker": symbol,
                    "qty": None,
                    "amount": float(amt),
                    "description": desc or raw_type,
                    "provider_transaction_id": txid,
                    "provider_account_id": provider_account_id,
                    "source_file_hash": rep.payload_hash,
                    "currency": (_extract_currency(row) or "USD").strip().upper(),
                }
            )

        holdings = {"as_of": utcnow().isoformat(), "items": holdings_items, "payload_hashes": [rep.payload_hash]}
        return items, holdings, metrics

    def fetch_transactions(
        self,
        connection: Any,
        start_date: dt.date,
        end_date: dt.date,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        # Pagination: cursor is the raw query index. We download/parse at most ONE report per call, so
        # multiple query ids don't all submit at once.
        idx = int(cursor) if cursor is not None else 0
        token = self._token(connection)
        query_ids_raw = self._query_ids(connection)
        rs = getattr(connection, "run_settings", None) or {}

        while idx < len(query_ids_raw):
            q_raw = query_ids_raw[idx]
            try:
                qid = self._resolve_query_id(token=token, query=q_raw, connection=connection)
            except ProviderError as e:
                self._warn(
                    connection,
                    f"Skipped Flex Query '{q_raw}': {e}. (Tip: use numeric Query IDs if name resolution is unavailable.)",
                )
                idx += 1
                continue
            try:
                rep = self._get_or_download_report(connection, query_id=qid, start=start_date, end=end_date)
            except RangeTooLargeError:
                raise
            except ProviderError as e:
                self._warn(connection, f"Flex Query '{q_raw}' failed: {e}")
                idx += 1
                continue

            items, holdings, metrics = self._parse_report_transactions(rep)
            # Cache holdings by payload hash so fetch_holdings can reuse without extra network.
            hcache = rs.setdefault("_ib_flex_web_holdings_cache", {})
            if isinstance(hcache, dict):
                hcache[rep.payload_hash] = holdings
            rs.setdefault("_ib_flex_web_metrics", []).append(metrics)
            next_idx = idx + 1
            next_cursor = str(next_idx) if next_idx < len(query_ids_raw) else None
            return items, next_cursor

        return [], None

    def fetch_holdings(self, connection: Any, as_of: dt.datetime | None = None) -> dict[str, Any]:
        rs = getattr(connection, "run_settings", None) or {}
        # If transactions pages already parsed, reuse cached holdings.
        hcache = rs.get("_ib_flex_web_holdings_cache") or {}
        # De-dupe holdings by (provider_account_id, symbol): multiple Flex queries may return OpenPositions
        # sections that overlap. For UI/portfolio metrics, we treat positions as already aggregated per symbol.
        items_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        payload_hashes: list[str] = []
        if isinstance(hcache, dict):
            for h, snap in hcache.items():
                if not isinstance(snap, dict):
                    continue
                payload_hashes.append(str(h))
                for it in snap.get("items") or []:
                    if isinstance(it, dict):
                        acct = str(it.get("provider_account_id") or "").strip()
                        sym = str(it.get("symbol") or it.get("ticker") or "").strip().upper()
                        if not acct or not sym:
                            continue
                        k = (acct, sym)
                        # Prefer the first seen; if later one has more populated values, replace.
                        if k not in items_by_key:
                            items_by_key[k] = dict(it)
                        else:
                            cur = items_by_key[k]
                            cur_mv = cur.get("market_value")
                            it_mv = it.get("market_value")
                            cur_qty = cur.get("qty")
                            it_qty = it.get("qty")
                            # Replace if current is missing key numeric fields but new has them.
                            if (cur_mv in (None, "") and it_mv not in (None, "")) or (cur_qty in (None, "") and it_qty not in (None, "")):
                                items_by_key[k] = dict(it)
        out = {"as_of": (as_of or utcnow()).isoformat(), "items": list(items_by_key.values()), "payload_hashes": payload_hashes}
        return out
