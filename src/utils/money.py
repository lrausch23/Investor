from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal(int(value))
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    s = str(value).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def format_usd(value: Any, digits: int = 2, dash: str = "—") -> str:
    """
    Jinja-friendly USD formatter.

    - `None` -> em dash
    - numeric -> "$1,234.56" (or "$1,235" if digits=0)
    - non-numeric string -> returned as-is
    """
    d = _to_decimal(value)
    if d is None:
        if value is None:
            return dash
        s = str(value).strip()
        return s if s else dash

    digits = max(0, int(digits))
    q = Decimal(1) if digits == 0 else Decimal("1").scaleb(-digits)
    d = d.quantize(q, rounding=ROUND_HALF_UP)

    sign = "-" if d < 0 else ""
    d_abs = -d if d < 0 else d
    return f"{sign}${d_abs:,.{digits}f}"

