from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass


def modified_dietz_return(
    *,
    begin_value: float,
    end_value: float,
    net_external_flow: float,
    flow_weight: float = 0.5,
) -> float | None:
    """
    Modified Dietz return for a period with (net) external flow.

    - `net_external_flow` is portfolio-perspective: contributions positive, withdrawals negative.
    - `flow_weight` approximates average timing (0.5 means mid-period).
    """
    denom = float(begin_value) + float(flow_weight) * float(net_external_flow)
    if abs(denom) <= 1e-12:
        return None
    return (float(end_value) - float(begin_value) - float(net_external_flow)) / denom


def chain_link(returns: list[float]) -> float | None:
    if not returns:
        return None
    prod = 1.0
    for r in returns:
        prod *= (1.0 + float(r))
    return prod - 1.0


def _npv(rate: float, cashflows: list[tuple[dt.date, float]]) -> float:
    if rate <= -0.999999:
        return float("inf")
    d0 = cashflows[0][0]
    out = 0.0
    for d, amt in cashflows:
        years = (d - d0).days / 365.0
        out += float(amt) / ((1.0 + rate) ** years)
    return out


def xirr(cashflows: list[tuple[dt.date, float]]) -> float | None:
    """
    Investor-perspective XIRR:
    - deposits (cash into portfolio) are negative
    - withdrawals (cash out to investor/IRS) are positive
    - ending value is positive
    """
    cfs = [(d, float(a)) for d, a in cashflows if d is not None and a is not None]
    cfs.sort(key=lambda x: x[0])
    if len(cfs) < 2:
        return None
    has_pos = any(a > 0 for _d, a in cfs)
    has_neg = any(a < 0 for _d, a in cfs)
    if not (has_pos and has_neg):
        return None

    # Newton-Raphson, multiple initial guesses.
    for guess in (0.1, 0.05, 0.2, 0.0, -0.2):
        r = float(guess)
        for _ in range(60):
            f = _npv(r, cfs)
            if abs(f) < 1e-7:
                return r
            eps = 1e-6
            df = (_npv(r + eps, cfs) - f) / eps
            if df == 0 or not math.isfinite(df):
                break
            r2 = r - f / df
            if r2 <= -0.999999 or not math.isfinite(r2):
                break
            if abs(r2 - r) < 1e-10:
                return r2
            r = r2

    # Bisection fallback.
    lo = -0.95
    hi = 10.0
    f_lo = _npv(lo, cfs)
    f_hi = _npv(hi, cfs)
    if not (math.isfinite(f_lo) and math.isfinite(f_hi)):
        return None
    if f_lo * f_hi > 0:
        return None
    for _ in range(300):
        mid = (lo + hi) / 2.0
        f_mid = _npv(mid, cfs)
        if not math.isfinite(f_mid):
            hi = mid
            continue
        if abs(f_mid) < 1e-7:
            return mid
        if f_lo * f_mid <= 0:
            hi = mid
            f_hi = f_mid
        else:
            lo = mid
            f_lo = f_mid
        if abs(hi - lo) < 1e-10:
            return (lo + hi) / 2.0
    return None


@dataclass(frozen=True)
class RiskStats:
    vol: float | None
    sharpe: float | None
    sortino: float | None
    max_drawdown: float | None
    beta: float | None
    alpha: float | None
    corr: float | None


def _mean(xs: list[float]) -> float | None:
    if not xs:
        return None
    return sum(xs) / float(len(xs))


def _sample_std(xs: list[float]) -> float | None:
    if len(xs) < 2:
        return None
    m = _mean(xs)
    assert m is not None
    var = sum((x - m) ** 2 for x in xs) / float(len(xs) - 1)
    if var < 0:
        return None
    return math.sqrt(var)


def max_drawdown_from_returns(returns: list[float]) -> float | None:
    if not returns:
        return None
    peak = 1.0
    eq = 1.0
    mdd = 0.0
    for r in returns:
        eq *= (1.0 + float(r))
        if eq > peak:
            peak = eq
        dd = (eq / peak) - 1.0
        if dd < mdd:
            mdd = dd
    return float(mdd)


def risk_stats_from_returns(
    *,
    portfolio_returns: list[float],
    benchmark_returns: list[float] | None = None,
    risk_free_rate_annual: float = 0.0,
    periods_per_year: float = 12.0,
) -> RiskStats:
    mu = _mean(portfolio_returns)
    sigma = _sample_std(portfolio_returns)
    vol = (sigma * math.sqrt(periods_per_year)) if sigma is not None else None
    rf_p = float(risk_free_rate_annual) / float(periods_per_year)
    sharpe = None
    if mu is not None and sigma is not None and sigma > 0:
        sharpe = ((mu - rf_p) / sigma) * math.sqrt(periods_per_year)
    downside = [min(0.0, r - rf_p) for r in portfolio_returns]
    dstd = _sample_std(downside)
    sortino = None
    if mu is not None and dstd is not None and dstd > 0:
        sortino = ((mu - rf_p) / dstd) * math.sqrt(periods_per_year)
    mdd = max_drawdown_from_returns(portfolio_returns)

    beta = alpha = corr = None
    if benchmark_returns is not None and len(benchmark_returns) == len(portfolio_returns) and len(portfolio_returns) >= 2:
        xb = benchmark_returns
        y = portfolio_returns
        mx = _mean(xb)
        my = _mean(y)
        assert mx is not None and my is not None
        cov = sum((xi - mx) * (yi - my) for xi, yi in zip(xb, y)) / float(len(y) - 1)
        vx = sum((xi - mx) ** 2 for xi in xb) / float(len(xb) - 1)
        vy = sum((yi - my) ** 2 for yi in y) / float(len(y) - 1)
        if vx > 0:
            beta = cov / vx
            alpha = my - beta * mx
        if vx > 0 and vy > 0:
            corr = cov / math.sqrt(vx * vy)

    return RiskStats(vol=vol, sharpe=sharpe, sortino=sortino, max_drawdown=mdd, beta=beta, alpha=alpha, corr=corr)

