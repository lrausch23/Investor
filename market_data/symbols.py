from __future__ import annotations

import re
from dataclasses import dataclass


_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_CASH_RE = re.compile(r"^CASH:([A-Z]{3})$")

# ISO-4217 subset (plus a few widely-used codes). This avoids misclassifying 3-letter equity tickers
# like AMD/APP/VOO as currencies.
_CURRENCY_CODES: set[str] = {
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "CHF",
    "CAD",
    "AUD",
    "NZD",
    "CNY",
    "HKD",
    "SGD",
    "SEK",
    "NOK",
    "DKK",
    "MXN",
    "BRL",
    "INR",
    "KRW",
    "TWD",
    "ZAR",
    "PLN",
    "CZK",
    "HUF",
    "ILS",
    "AED",
    "SAR",
}

# Common crypto base tickers on Yahoo (BTC-USD, ETH-USD, etc.). Kept intentionally small.
_CRYPTO_CODES: set[str] = {
    "BTC",
    "ETH",
    "SOL",
    "XRP",
    "DOGE",
    "ADA",
    "DOT",
    "AVAX",
    "LINK",
    "MATIC",
    "LTC",
    "BCH",
}

_INVALID_TICKERS: set[str] = {
    "TOTAL",
    "UNKNOWN",
}


@dataclass(frozen=True)
class NormalizedSymbol:
    original: str
    provider_ticker: str | None
    kind: str  # yahoo | fx | synthetic_cash | invalid
    note: str | None = None


def sanitize_ticker(ticker: str) -> str:
    """
    Filename-safe canonicalization.

    Examples:
    - "BRK-B" -> "BRK_B"
    - "BRK.B" -> "BRK_B"
    - "BTC-USD" -> "BTC_USD"
    - "CASH:USD" -> "CASH_USD"
    """
    t = (ticker or "").strip().upper()
    t = re.sub(r"[^A-Z0-9]+", "_", t)
    t = re.sub(r"_+", "_", t).strip("_")
    return t or "UNKNOWN"


def normalize_ticker(ticker: str, *, base_currency: str = "USD") -> NormalizedSymbol:
    """
    Normalize an internal/security ticker to a Yahoo Finance-compatible symbol, when possible.

    - Class shares: "BRK.B" -> "BRK-B"
    - Cash pseudo-tickers: "CASH:USD" -> synthetic constant series
    - Currencies: "EUR" -> "EURUSD=X" (base currency default USD)
    """
    raw = (ticker or "").strip()
    if not raw:
        return NormalizedSymbol(original=ticker, provider_ticker=None, kind="invalid", note="Empty ticker.")

    t = raw.strip().upper()
    if t in _INVALID_TICKERS:
        return NormalizedSymbol(original=ticker, provider_ticker=None, kind="invalid", note=f"{t} is not a priceable symbol.")

    base = (base_currency or "USD").strip().upper()
    if not _CURRENCY_RE.match(base):
        base = "USD"

    # Common broker shorthands for Berkshire class shares.
    if t == "BRKA":
        return NormalizedSymbol(original=ticker, provider_ticker="BRK-A", kind="yahoo", note="Mapped broker shorthand BRKA to BRK-A.")
    if t == "BRKB":
        return NormalizedSymbol(original=ticker, provider_ticker="BRK-B", kind="yahoo", note="Mapped broker shorthand BRKB to BRK-B.")

    m = _CASH_RE.match(t)
    if m:
        ccy = m.group(1)
        if ccy == base:
            return NormalizedSymbol(original=ticker, provider_ticker=None, kind="synthetic_cash", note=f"Cash in base currency {base}.")
        return NormalizedSymbol(original=ticker, provider_ticker=f"{ccy}{base}=X", kind="fx", note=f"FX {ccy}/{base}.")

    if t == base or t == "USD" or t == "CASH":
        return NormalizedSymbol(original=ticker, provider_ticker=None, kind="synthetic_cash", note="Base currency cash.")

    if _CURRENCY_RE.match(t) and t in _CURRENCY_CODES:
        # Treat as currency code (FX vs base currency).
        return NormalizedSymbol(original=ticker, provider_ticker=f"{t}{base}=X", kind="fx", note=f"FX {t}/{base}.")

    # Crypto shorthand: "ETH" -> "ETH-USD" (only for known crypto codes).
    if t in _CRYPTO_CODES and "-" not in t and "=" not in t:
        return NormalizedSymbol(original=ticker, provider_ticker=f"{t}-{base}", kind="yahoo", note="Assumed crypto ticker vs base currency.")

    # Common broker formats.
    if "/" in t and len(t.split("/", 1)[0]) >= 2 and len(t.split("/", 1)[1]) >= 2:
        # "BTC/USD" -> "BTC-USD"
        a, b = t.split("/", 1)
        return NormalizedSymbol(original=ticker, provider_ticker=f"{a}-{b}", kind="yahoo", note="Converted slash format to Yahoo crypto format.")

    # Yahoo uses '-' for class share tickers (e.g., BRK-B).
    if "." in t and not t.endswith(".") and not t.startswith("."):
        t2 = t.replace(".", "-")
        return NormalizedSymbol(original=ticker, provider_ticker=t2, kind="yahoo", note="Converted dot class-share ticker to Yahoo dash.")

    return NormalizedSymbol(original=ticker, provider_ticker=t, kind="yahoo", note=None)
