from __future__ import annotations

import datetime as dt
import json
import re
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
from typing import Any, Iterable, Optional


_WS_RE = re.compile(r"\s+")
_NOISE_PREFIX_RE = re.compile(
    r"^(POS PURCHASE|POS|DEBIT CARD PURCHASE|DEBIT PURCHASE|CARD PURCHASE|PURCHASE|PAYMENT|ONLINE TRANSFER)\b[:\-]?\s*",
    re.IGNORECASE,
)
_REF_RE = re.compile(r"\b(REF|REFERENCE|TRACE|AUTH|ID)[:\s#-]*[A-Z0-9-]{6,}\b", re.IGNORECASE)
_DIGIT_RUN_RE = re.compile(r"(?<!\d)\d{6,}(?!\d)")
_MASKED_ACCT_RE = re.compile(r"\.\.\.\s*\d{3,4}\b")
_TXN_LABEL_RE = re.compile(r"\bTRANSACTION\s*#?\s*:?\s*", re.IGNORECASE)
_HASH_ID_RE = re.compile(r"#\s*\d{3,}\b")
_TRAILING_MMDD_RE = re.compile(r"\b\d{1,2}/\d{1,2}\b\s*$")
_BANK_ID_LABEL_RE = re.compile(r"\b(PPD\s+ID|WEB\s+ID|TRACE\s+ID|CONFIRMATION\s+ID)\s*:?\s*", re.IGNORECASE)
_BANK_ENDING_IN_RE = re.compile(r"\b(ENDING\s+IN)\s+\d{3,4}\b", re.IGNORECASE)
_REDACTED_TOKEN_RE = re.compile(r"[0-9A-Z]*\*{3,}[0-9A-Z]*", re.IGNORECASE)
_ALNUM_MIXED_TOKEN_RE = re.compile(
    r"\b(?=[A-Z0-9]{6,}\b)(?=[A-Z0-9]*[A-Z])(?=[A-Z0-9]*\d)[A-Z0-9]+\b",
    re.IGNORECASE,
)
_LETTER_DIGITS_TOKEN_RE = re.compile(r"\b[A-Z]\d{4,6}\b", re.IGNORECASE)
_BANK_ST_TOKEN_RE = re.compile(r"\bST-[A-Z0-9]{6,}\b", re.IGNORECASE)
_MONTH_TOKEN_RE = re.compile(
    r"\b(JAN(UARY)?|FEB(RUARY)?|MAR(CH)?|APR(IL)?|MAY|JUN(E)?|JUL(Y)?|AUG(UST)?|SEP(TEMBER)?|OCT(OBER)?|NOV(EMBER)?|DEC(EMBER)?)\b",
    re.IGNORECASE,
)
_INSURANCE_HINT_RE = re.compile(r"\b(INSPRM|INSURANCE|PREMIUMS?|INSP)\b", re.IGNORECASE)
_CARD_PREFIX_RE = re.compile(r"^(APL\s*PAY|APLPAY|APLPAY\s+|APLPAY\s+-\s+|APLPAY:|APLPAY\s*:|AplPay)\b[:\-\s]*", re.IGNORECASE)
_LEADING_PROCESSOR_RE = re.compile(r"^(TST|DD|PP|DNH|SQSP)\s*\*?\s*", re.IGNORECASE)
_DIGIT_LETTER_TOKEN_RE = re.compile(r"\b\d{1,6}[A-Z]{2,}\b", re.IGNORECASE)  # e.g. 00JAX, 0PALM, 45001PALM
_LETTER_DIGITS_LONG_TOKEN_RE = re.compile(r"\b[A-Z]{1,4}\d{2,6}\b", re.IGNORECASE)  # e.g. Q35, F6717, JFK4


def parse_date(value: str) -> dt.date:
    s = (value or "").strip()
    if not s:
        raise ValueError("Missing date")
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        pass
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return dt.datetime.strptime(s.split()[0], fmt).date()
        except Exception:
            continue
    raise ValueError(f"Invalid date: {value!r}")


def parse_decimal(value: str) -> Decimal:
    s = (value or "").strip()
    if not s:
        raise ValueError("Missing amount")
    s = s.replace("$", "").replace(",", "")
    return Decimal(s)


def money_2dp(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def normalize_description(raw: str) -> str:
    s = (raw or "").strip()
    s = _NOISE_PREFIX_RE.sub("", s)
    s = _REF_RE.sub("", s)
    s = _DIGIT_RUN_RE.sub("", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


_MERCHANT_MAP: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bAMZN\b|\bAMAZON\b|AMAZON\.COM|AMZN MKTP", re.IGNORECASE), "Amazon"),
    (re.compile(r"\bPRIME\s+VIDEO\b|\bPRIMEVIDEO\b|\bPRIME\s+VIDEO\s+CHANNELS?\b", re.IGNORECASE), "Amazon"),
    (re.compile(r"\bKINDLE\b|\bKINDLE\s+SVCS\b|KINDLE SVCS", re.IGNORECASE), "Amazon"),
    (re.compile(r"\bAMZN\s+DIGITAL\b|\bAMAZON\s+DIGITAL\b", re.IGNORECASE), "Amazon"),
    (re.compile(r"\bAUDIBLE\b|\bAMAZON\s+MUSIC\b|\bAMAZON\s+PRIME\b|\bPRIME\s+MEMBERSHIP\b", re.IGNORECASE), "Amazon"),
    (re.compile(r"\bGOOGLE\b.*\bFI\b", re.IGNORECASE), "Google Fi"),
    (re.compile(r"\bGOOGLE\b.*\bCLOUD\b", re.IGNORECASE), "Google Cloud"),
    (re.compile(r"\bGOOGLE\b.*\bVOICE\b", re.IGNORECASE), "Google Voice"),
    (re.compile(r"\bDOORDASH\b", re.IGNORECASE), "DoorDash"),
    (re.compile(r"\bGODADDY\b", re.IGNORECASE), "GoDaddy"),
    (re.compile(r"\bDROPBOX\b", re.IGNORECASE), "Dropbox"),
    (re.compile(r"\bSQSP\b|\bSQUARESPACE\b", re.IGNORECASE), "Squarespace"),
    (re.compile(r"\bPADDLE\b", re.IGNORECASE), "Paddle"),
    (re.compile(r"\bMCDONALD", re.IGNORECASE), "McDonald's"),
    (re.compile(r"\bCHICK[\s-]*FIL[\s-]*A\b", re.IGNORECASE), "Chick-fil-A"),
    (re.compile(r"\bPUBLIX\b", re.IGNORECASE), "Publix"),
    (re.compile(r"\bCIRCLE\s+K\b", re.IGNORECASE), "Circle K"),
    (re.compile(r"\bCHIPOTLE\b", re.IGNORECASE), "Chipotle"),
    (re.compile(r"\bZAXBY", re.IGNORECASE), "Zaxby's"),
    (re.compile(r"\bOPENAI\b", re.IGNORECASE), "OpenAI"),
    (re.compile(r"\bANTHROPIC\b", re.IGNORECASE), "Anthropic"),
    (re.compile(r"\bSTARBUCKS\b", re.IGNORECASE), "Starbucks"),
    (re.compile(r"\bUBER\b", re.IGNORECASE), "Uber"),
    (re.compile(r"\bLYFT\b", re.IGNORECASE), "Lyft"),
    (re.compile(r"\bSTATE\s+FARM\b", re.IGNORECASE), "State Farm"),
    (re.compile(r"\bGEICO\b", re.IGNORECASE), "GEICO"),
    (re.compile(r"\b(HEALTH\s+FIRST|MED\s+HEALTH\s+FIRST)\b", re.IGNORECASE), "Health First"),
    (re.compile(r"\bEMBRACE\s+PET\s+INSUR", re.IGNORECASE), "Embrace Pet Insurance"),
    (re.compile(r"\bUNITED\s+WORLD\s+HTH\b|\bUNITED\s+WORLD\s+HEALTH\b", re.IGNORECASE), "United World Health"),
    (re.compile(r"\bMONTHLY\s+INSTALLMENTS?\b", re.IGNORECASE), "Apple"),
    (re.compile(r"\bAPPLE\b|ITUNES|APP STORE", re.IGNORECASE), "Apple"),
    (re.compile(r"\bNETFLIX\b", re.IGNORECASE), "Netflix"),
    (re.compile(r"\bSPOTIFY\b", re.IGNORECASE), "Spotify"),
]


def normalize_merchant(description_norm: str) -> str:
    s = (description_norm or "").strip()
    if not s:
        return "Unknown"
    # Credit card statements often encode processor prefixes and payee codes with "*".
    # Replace "*" with space so rules and fallbacks match consistently (does not affect txn_id hashing).
    s = s.replace("*", " ")
    s = _WS_RE.sub(" ", s).strip()
    # First pass: match well-known merchants before stripping processor prefixes like SQSP/DD/TST,
    # otherwise we can lose the only identifying token.
    for pat, merchant in _MERCHANT_MAP:
        if pat.search(s):
            return merchant
    s = _CARD_PREFIX_RE.sub("", s)
    s = _LEADING_PROCESSOR_RE.sub("", s)
    s = _WS_RE.sub(" ", s).strip()
    for pat, merchant in _MERCHANT_MAP:
        if pat.search(s):
            return merchant
    # Heuristic: fee/adjustment patterns often include "FEE - VENDOR"; use the vendor side.
    parts = re.split(r"\s+-\s+", s, maxsplit=1)
    if len(parts) == 2:
        left = parts[0].strip().lower()
        right = parts[1].strip()
        if right and any(k in left for k in ["fee", "interest", "adjustment", "plan fee", "late fee"]):
            return right[:200]
    # Fallback heuristic: strip per-transaction noise tokens (store ids, redactions, etc.)
    cleaned = s
    cleaned = _REDACTED_TOKEN_RE.sub("", cleaned)
    cleaned = _BANK_ST_TOKEN_RE.sub("", cleaned)
    cleaned = _HASH_ID_RE.sub("", cleaned)
    cleaned = _LETTER_DIGITS_TOKEN_RE.sub("", cleaned)
    cleaned = _LETTER_DIGITS_LONG_TOKEN_RE.sub("", cleaned)
    cleaned = _ALNUM_MIXED_TOKEN_RE.sub("", cleaned)
    cleaned = _DIGIT_LETTER_TOKEN_RE.sub("", cleaned)
    cleaned = _WS_RE.sub(" ", cleaned).strip()
    # Fallback heuristic: take the first "word group" before separators.
    cut = re.split(r"\s+-\s+| / | \\\\ | \s{2,}|\s+\d{2,}\b", cleaned, maxsplit=1)[0].strip()
    if not cut:
        return "Unknown"
    return cut[:200]


def normalize_bank_merchant(description_raw: str) -> str:
    """
    Bank-statement oriented merchant normalization. This is used for merchant rollups only
    (it does not affect txn_id hashing, which is based on `normalize_description`).
    """
    s = (description_raw or "").strip()
    if not s:
        return "Unknown"
    # Strip common bank noise that causes merchant explosion in rollups.
    s = _DIGIT_RUN_RE.sub("", s)
    s = _TXN_LABEL_RE.sub("", s)
    s = _HASH_ID_RE.sub("", s)
    s = _MASKED_ACCT_RE.sub("", s)
    s = _BANK_ENDING_IN_RE.sub(r"\1", s)
    s = _BANK_ID_LABEL_RE.sub("", s)
    s = _BANK_ST_TOKEN_RE.sub("", s)
    s = _LETTER_DIGITS_TOKEN_RE.sub("", s)
    s = _ALNUM_MIXED_TOKEN_RE.sub("", s)
    s = _REDACTED_TOKEN_RE.sub("", s)
    if _INSURANCE_HINT_RE.search(s):
        s = _MONTH_TOKEN_RE.sub("", s)
    s = _TRAILING_MMDD_RE.sub("", s)
    s = _WS_RE.sub(" ", s).strip()
    if not s:
        return "Unknown"
    # Special-case: Condo association ACH payees often appear with minor variants that explode merchants:
    # "CONDOMINIUM ASSO CONDOMINIU ..." vs "Condominium Asso L ...". Treat as a single payee label.
    s_cf = s.casefold()
    if s_cf.startswith("condominium asso ") or s_cf == "condominium asso":
        return "Condominium Asso"
    if s_cf.startswith("condominium assoc ") or s_cf == "condominium assoc":
        return "Condominium Asso"
    # Common bank exporter truncation: a dangling final single-letter token (e.g., "... Ow L") that
    # should not split merchants in rollups/recurring detection.
    s = re.sub(r"\s+[A-Z]\b\s*$", "", s).strip()
    return normalize_merchant(s)


def stable_txn_id(
    *,
    institution: str,
    account_name: str,
    posted_date: dt.date,
    amount: Decimal,
    description_norm: str,
    currency: str,
    external_id: Optional[str],
) -> str:
    payload = {
        "institution": institution.strip().upper(),
        "account": account_name.strip().upper(),
        "posted_date": posted_date.isoformat(),
        "amount": str(money_2dp(amount)),
        "currency": currency.strip().upper(),
        "desc": description_norm.strip().upper(),
        "external_id": (external_id or "").strip(),
    }
    b = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(b).hexdigest()


def redact_value(value: str, patterns: Iterable[re.Pattern[str]]) -> str:
    out = value
    for p in patterns:
        out = p.sub("***", out)
    return out


def extract_last4_digits(value: str) -> str | None:
    s = "".join(ch for ch in (value or "") if ch.isdigit())
    if len(s) < 4:
        return None
    return s[-4:]


def redact_row(row: dict[str, Any], patterns: list[str]) -> dict[str, Any]:
    compiled = [re.compile(p) for p in patterns]
    out: dict[str, Any] = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
            continue
        if isinstance(v, (int, float, Decimal)):
            out[k] = v
            continue
        if isinstance(v, str):
            out[k] = redact_value(v, compiled)
            continue
        out[k] = v
    return out
