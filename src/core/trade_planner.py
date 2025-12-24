from __future__ import annotations

import datetime as dt
from collections import defaultdict
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.core.lot_selection import (
    SelectedLot,
    select_lots_fifo,
    select_lots_lifo,
    select_lots_tax_min,
)
from src.core.policy_engine import compute_drift_report, compute_drift_report_with_overrides, policy_constraints
from src.core.portfolio import HoldingView, holdings_snapshot
from src.core.tax_engine import TaxAssumptions, estimate_tax_delta, realized_delta_from_lot_picks
from src.core.types import LotPick, PlannerResult, TaxImpactRow, TaxImpactSummary, TradeRecommendation
from src.core.wash_sale import taxpayer_entities_by_scope, wash_risk_for_loss_sale
from src.db.models import Account, Bucket, BucketAssignment, BucketPolicy, Security, TaxpayerEntity


class PlannerConfig(BaseModel):
    lot_selection_method: Literal["SPECIFIC_ID_TAX_MIN", "FIFO", "LIFO"] = "SPECIFIC_ID_TAX_MIN"
    avoid_definite_wash_loss_sales: bool = True
    allow_st_gains: bool = False
    prefer_lower_cost: bool = True
    min_trade_value: float = 250.0
    wash_window_days: int = 30
    tax: TaxAssumptions = Field(default_factory=TaxAssumptions)


def _pick_trade_account_for_buy(session: Session, *, taxpayer_id: int) -> Optional[Account]:
    accounts = session.query(Account).filter(Account.taxpayer_entity_id == taxpayer_id).order_by(Account.id).all()
    if not accounts:
        return None
    return accounts[0]


def _buckets(session: Session, *, policy_id: int) -> dict[str, Bucket]:
    rows = session.query(Bucket).filter(Bucket.policy_id == policy_id).all()
    return {b.code: b for b in rows}


def _bucket_assignment(session: Session, *, policy_id: int) -> dict[str, str]:
    rows = session.query(BucketAssignment).filter(BucketAssignment.policy_id == policy_id).all()
    return {r.ticker: r.bucket_code for r in rows}


def _securities(session: Session) -> dict[str, Security]:
    rows = session.query(Security).all()
    return {s.ticker: s for s in rows}


def _suggest_substitutes(
    session: Session,
    *,
    policy_id: int,
    sale_ticker: str,
    bucket_code: Optional[str],
    securities: dict[str, Security],
    limit: int = 5,
) -> list[str]:
    if not bucket_code:
        return []
    sale_sec = securities.get(sale_ticker)
    if sale_sec is None:
        return []
    sale_group = sale_sec.substitute_group_id
    sale_class = sale_sec.asset_class
    assign = _bucket_assignment(session, policy_id=policy_id)
    candidates = []
    for t, b in assign.items():
        if b != bucket_code or t == sale_ticker:
            continue
        sec = securities.get(t)
        if sec is None:
            continue
        if sec.asset_class != sale_class:
            continue
        if sale_group is not None and sec.substitute_group_id == sale_group:
            continue
        candidates.append(sec)
    candidates.sort(key=lambda s: float(s.expense_ratio or 0.0))
    return [c.ticker for c in candidates[:limit]]


def _target_bucket_values(total_value: float, buckets: dict[str, Bucket]) -> dict[str, float]:
    return {code: float(b.target_pct) * total_value for code, b in buckets.items()}


def _current_bucket_values(holdings: list[HoldingView], cash_total: float) -> dict[str, float]:
    by_bucket: dict[str, float] = {"B1": cash_total, "B2": 0.0, "B3": 0.0, "B4": 0.0, "UNASSIGNED": 0.0}
    for h in holdings:
        code = h.bucket_code or "UNASSIGNED"
        by_bucket[code] = by_bucket.get(code, 0.0) + float(h.market_value)
    return by_bucket


def _choose_buy_ticker(
    *,
    session: Session,
    policy_id: int,
    bucket_code: str,
    securities: dict[str, Security],
    allowed_asset_classes: list[str],
    prefer_lower_cost: bool,
) -> Optional[str]:
    assign = _bucket_assignment(session, policy_id=policy_id)
    candidates = []
    for ticker, bcode in assign.items():
        if bcode != bucket_code:
            continue
        sec = securities.get(ticker)
        if sec is None:
            continue
        if allowed_asset_classes and sec.asset_class not in allowed_asset_classes:
            continue
        candidates.append(sec)

    # Fallback: if no explicit assignments exist, choose any security whose asset_class is allowed.
    if not candidates and allowed_asset_classes:
        for sec in securities.values():
            if sec.asset_class in allowed_asset_classes:
                candidates.append(sec)
    if not candidates:
        return None
    candidates.sort(key=lambda s: float(s.expense_ratio or 0.0))
    chosen = candidates[0]
    if prefer_lower_cost:
        alt = (chosen.metadata_json or {}).get("low_cost_ticker")
        if isinstance(alt, str) and alt.strip():
            alt_t = alt.strip().upper()
            alt_sec = securities.get(alt_t)
            if alt_sec is not None and assign.get(alt_t) == bucket_code:
                if allowed_asset_classes and alt_sec.asset_class not in allowed_asset_classes:
                    return chosen.ticker
                if float(alt_sec.expense_ratio or 0.0) <= float(chosen.expense_ratio or 0.0):
                    chosen = alt_sec
    return chosen.ticker


def _sell_lots(
    *,
    holdings: HoldingView,
    sell_value: float,
    sale_date: dt.date,
    cfg: PlannerConfig,
    wash_risk_for_ticker: str,
) -> tuple[float, list[SelectedLot], list[str]]:
    warnings: list[str] = []
    price = float(holdings.price or 1.0)
    sell_qty = min(float(holdings.qty), float(sell_value) / price) if price > 0 else 0.0
    if sell_qty <= 0:
        return 0.0, [], warnings

    wash_risk_by_lot = {l.id: wash_risk_for_ticker for l in holdings.lots}
    if cfg.lot_selection_method == "FIFO":
        picks = select_lots_fifo(lots=holdings.lots, sell_qty=sell_qty, sale_price=price, sale_date=sale_date)
    elif cfg.lot_selection_method == "LIFO":
        picks = select_lots_lifo(lots=holdings.lots, sell_qty=sell_qty, sale_price=price, sale_date=sale_date)
    else:
        picks = select_lots_tax_min(
            lots=holdings.lots,
            sell_qty=sell_qty,
            sale_price=price,
            sale_date=sale_date,
            wash_risk_by_lot_id=wash_risk_by_lot,
            avoid_definite_wash_loss_sales=cfg.avoid_definite_wash_loss_sales,
        )

    picked_qty = sum(p.qty for p in picks)
    if picked_qty <= 0:
        if wash_risk_for_ticker == "DEFINITE" and cfg.avoid_definite_wash_loss_sales:
            warnings.append(f"Skipped loss-sale lots for {holdings.ticker} due to definite wash risk.")
        return 0.0, [], warnings
    return picked_qty, picks, warnings


def _max_single_name_pct(policy_constraints_json: dict[str, Any]) -> float:
    c = (policy_constraints_json or {}).get("constraints", {})
    v = c.get("max_single_name_pct")
    try:
        return float(v)
    except Exception:
        return 0.15


def plan_trades(
    *,
    session: Session,
    policy_id: int,
    goal: dict[str, Any],
    scope: str,
    config: PlannerConfig,
) -> PlannerResult:
    policy = session.query(BucketPolicy).filter(BucketPolicy.id == policy_id).one()
    buckets = _buckets(session, policy_id=policy_id)
    secmap = _securities(session)
    constraints_json = policy_constraints(session, policy_id=policy_id)
    max_single_name = _max_single_name_pct(constraints_json)

    as_of = dt.date.fromisoformat(goal.get("as_of") or dt.date.today().isoformat())

    warnings: list[str] = []
    if scope == "BOTH":
        warnings.append("MVP assumes no cross-taxpayer cash transfers; planner runs independently per taxpayer and aggregates results.")

    taxpayers = taxpayer_entities_by_scope(session, scope=scope)
    if not taxpayers:
        warnings.append("No taxpayers found in this scope; run Setup > Create Defaults.")

    all_trades: list[TradeRecommendation] = []
    all_lot_picks: list[LotPick] = []
    substitute_suggestions: dict[str, list[str]] = {}

    st_by_tp = defaultdict(float)
    lt_by_tp = defaultdict(float)

    pre_drift = compute_drift_report(session, policy_id=policy_id, scope=scope)

    for tp in taxpayers:
        tp_holdings, tp_cash, snap_warnings = holdings_snapshot(
            session, policy_id=policy_id, scope="BOTH", taxpayer_entity_id=tp.id, as_of=as_of
        )
        warnings.extend([f"{tp.name}: {w}" for w in snap_warnings])

        cash_total = sum(float(c.amount) for c in tp_cash)
        current = _current_bucket_values(tp_holdings, cash_total=cash_total)
        total_value = sum(v for k, v in current.items() if k != "UNASSIGNED") + current.get("UNASSIGNED", 0.0)
        if total_value <= 0:
            warnings.append(f"{tp.name}: total value is zero; nothing to plan.")
            continue

        target = _target_bucket_values(total_value, buckets=buckets)

        def bucket_delta(code: str) -> float:
            return target.get(code, 0.0) - current.get(code, 0.0)

        # Determine objective bucket(s).
        goal_type = goal.get("type")
        if goal_type == "raise_cash":
            amount = float(goal.get("cash_amount") or 0.0)
            target["B1"] = current.get("B1", 0.0) + max(0.0, amount)
        elif goal_type == "reduce_alpha":
            b4 = buckets.get("B4")
            if b4:
                max_val = float(b4.max_pct) * total_value
                target["B4"] = min(target.get("B4", 0.0), max_val)
        elif goal_type == "harvest_losses":
            # Harvesting is handled as a sell-driven objective and does not set bucket targets directly.
            pass

        # SELLs: generate cash by reducing buckets above target.
        planned_buys: list[dict] = []
        planned_sells: list[dict] = []

        if goal_type == "harvest_losses":
            loss_target = float(goal.get("harvest_loss_target") or 0.0)
            losses_harvested = 0.0
            candidates = [h for h in tp_holdings if h.bucket_code and h.lots]
            # Sort by most negative unrealized per $ (approx) using average cost.
            def _loss_score(h: HoldingView) -> float:
                sec = secmap.get(h.ticker)
                price = float(h.price or 1.0)
                if not h.lots or price <= 0:
                    return 0.0
                basis = sum((float(l.adjusted_basis_total) if l.adjusted_basis_total is not None else float(l.basis_total)) for l in h.lots)
                mv = float(h.market_value)
                return (mv - basis) / mv if mv else 0.0

            candidates.sort(key=_loss_score)  # most negative first
            for h in candidates:
                if loss_target > 0 and abs(losses_harvested) >= abs(loss_target):
                    break

                # Wash-risk check based on executed buys only (proposed buys are empty for pure harvest step).
                risk, _matches = wash_risk_for_loss_sale(
                    session,
                    taxpayer_entity_id=tp.id,
                    sale_ticker=h.ticker,
                    sale_date=as_of,
                    proposed_buys=[],
                    window_days=config.wash_window_days,
                )
                picked_qty, picks, sel_warnings = _sell_lots(
                    holdings=h,
                    sell_value=h.market_value,  # harvest as much as possible from this ticker
                    sale_date=as_of,
                    cfg=config,
                    wash_risk_for_ticker=risk,
                )
                warnings.extend([f"{tp.name}: {w}" for w in sel_warnings])
                if picked_qty <= 0:
                    continue
                # Only proceed if picks include a net loss.
                realized = realized_delta_from_lot_picks(
                    sale_date=as_of, sale_price=float(h.price or 1.0), picks=[p.__dict__ for p in picks]
                )
                if realized.st + realized.lt >= 0:
                    continue

                sell_value = picked_qty * float(h.price or 1.0)
                planned_sells.append(
                    {
                        "account_id": h.account_id,
                        "account_name": h.account_name,
                        "ticker": h.ticker,
                        "qty": picked_qty,
                        "est_price": float(h.price or 1.0),
                        "est_value": sell_value,
                        "bucket_code": h.bucket_code,
                        "picks": picks,
                        "taxpayer_id": tp.id,
                    }
                )
                losses_harvested += (realized.st + realized.lt)

        else:
            # For rebalance / raise_cash / reduce_alpha, sell from buckets with negative delta (over target).
            sell_needs: dict[str, float] = {}
            for code in ("B2", "B3", "B4"):
                d = bucket_delta(code)
                if d < -config.min_trade_value:
                    sell_needs[code] = -d

            # Reduce alpha may require more B4 selling.
            if goal_type == "reduce_alpha":
                d = target.get("B4", 0.0) - current.get("B4", 0.0)
                if d < -config.min_trade_value:
                    sell_needs["B4"] = max(sell_needs.get("B4", 0.0), -d)

            # Raise cash: if B1 target increased, sell to fund it.
            if goal_type == "raise_cash":
                d = target.get("B1", 0.0) - current.get("B1", 0.0)
                if d > config.min_trade_value:
                    needed = d
                    # Sell pro-rata from non-B1 buckets by current value
                    denom = max(1.0, current.get("B2", 0.0) + current.get("B3", 0.0) + current.get("B4", 0.0))
                    for code in ("B2", "B3", "B4"):
                        sell_needs[code] = sell_needs.get(code, 0.0) + needed * (current.get(code, 0.0) / denom)

            by_bucket_holdings: dict[str, list[HoldingView]] = defaultdict(list)
            for h in tp_holdings:
                by_bucket_holdings[h.bucket_code or "UNASSIGNED"].append(h)
            for code, hs in by_bucket_holdings.items():
                hs.sort(key=lambda x: float(x.market_value), reverse=True)

            for code, need_value in sell_needs.items():
                remaining_value = need_value
                for h in by_bucket_holdings.get(code, []):
                    if remaining_value <= config.min_trade_value:
                        break
                    # If all holdings are unassigned, skip.
                    if h.lots is None or not h.lots:
                        continue
                    risk, _matches = wash_risk_for_loss_sale(
                        session,
                        taxpayer_entity_id=tp.id,
                        sale_ticker=h.ticker,
                        sale_date=as_of,
                        proposed_buys=[],
                        window_days=config.wash_window_days,
                    )
                    picked_qty, picks, sel_warnings = _sell_lots(
                        holdings=h,
                        sell_value=min(h.market_value, remaining_value),
                        sale_date=as_of,
                        cfg=config,
                        wash_risk_for_ticker=risk,
                    )
                    warnings.extend([f"{tp.name}: {w}" for w in sel_warnings])
                    if picked_qty <= 0:
                        continue
                    sell_value = picked_qty * float(h.price or 1.0)
                    planned_sells.append(
                        {
                            "account_id": h.account_id,
                            "account_name": h.account_name,
                            "ticker": h.ticker,
                            "qty": picked_qty,
                            "est_price": float(h.price or 1.0),
                            "est_value": sell_value,
                            "bucket_code": h.bucket_code,
                            "picks": picks,
                            "taxpayer_id": tp.id,
                        }
                    )
                    remaining_value -= sell_value

        # BUYs: use bucket deficits (or optionally reinvest after sells).
        current_after_sells = dict(current)
        cash_after_sells = current.get("B1", 0.0) + sum(s["est_value"] for s in planned_sells)
        current_after_sells["B1"] = cash_after_sells
        for s in planned_sells:
            bcode = s["bucket_code"] or "UNASSIGNED"
            current_after_sells[bcode] = current_after_sells.get(bcode, 0.0) - float(s["est_value"])

        if goal_type in ("rebalance", "raise_cash", "reduce_alpha"):
            buy_needs: dict[str, float] = {}
            for code in ("B2", "B3", "B4"):
                d = target.get(code, 0.0) - current_after_sells.get(code, 0.0)
                if d > config.min_trade_value:
                    buy_needs[code] = d

            for code, need in buy_needs.items():
                ticker = _choose_buy_ticker(
                    session=session,
                    policy_id=policy_id,
                    bucket_code=code,
                    securities=secmap,
                    allowed_asset_classes=list(buckets[code].allowed_asset_classes_json or []),
                    prefer_lower_cost=config.prefer_lower_cost,
                )
                if ticker is None:
                    warnings.append(f"{tp.name}: no tickers assigned to {code}; cannot generate buy trade.")
                    continue
                sec = secmap.get(ticker)
                price = float((sec.metadata_json or {}).get("last_price") or 1.0) if sec else 1.0
                if price <= 0:
                    price = 1.0
                qty = need / price
                acct = _pick_trade_account_for_buy(session, taxpayer_id=tp.id)
                if acct is None:
                    warnings.append(f"{tp.name}: no accounts available for buys.")
                    continue
                planned_buys.append(
                    {
                        "account_id": acct.id,
                        "account_name": acct.name,
                        "ticker": ticker,
                        "qty": qty,
                        "est_price": price,
                        "est_value": qty * price,
                        "bucket_code": code,
                        "taxpayer_id": tp.id,
                    }
                )

        # Convert into recommendations + lot picks + tax deltas per taxpayer.
        for s in planned_sells:
            picks: list[SelectedLot] = s["picks"]
            # recompute wash risk including proposed buys in this plan (within same taxpayer)
            relevant_buys = [b for b in planned_buys if b["taxpayer_id"] == s["taxpayer_id"]]
            wash, _matches = wash_risk_for_loss_sale(
                session,
                taxpayer_entity_id=s["taxpayer_id"],
                sale_ticker=s["ticker"],
                sale_date=as_of,
                proposed_buys=relevant_buys,
                window_days=config.wash_window_days,
            )
            realized = realized_delta_from_lot_picks(
                sale_date=as_of, sale_price=float(s["est_price"]), picks=[p.__dict__ for p in picks]
            )
            st_by_tp[s["taxpayer_id"]] += realized.st
            lt_by_tp[s["taxpayer_id"]] += realized.lt

            requires_override = False
            if realized.st > 0 and not config.allow_st_gains:
                requires_override = True
                warnings.append(f"{tp.name}: planned SELL of {s['ticker']} realizes ST gains; override required to finalize.")
            if any(p.unrealized < 0 for p in picks) and wash == "DEFINITE":
                requires_override = True
                sugg = _suggest_substitutes(
                    session,
                    policy_id=policy_id,
                    sale_ticker=s["ticker"],
                    bucket_code=s.get("bucket_code"),
                    securities=secmap,
                )
                if sugg:
                    substitute_suggestions[s["ticker"]] = sugg
                    warnings.append(
                        f"{tp.name}: planned loss sale of {s['ticker']} has definite wash risk; consider delay/reduce or substitute: {', '.join(sugg)}."
                    )
                else:
                    warnings.append(
                        f"{tp.name}: planned loss sale of {s['ticker']} has definite wash risk; consider delay/reduce or substitute in a different group."
                    )

            all_trades.append(
                TradeRecommendation(
                    action="SELL",
                    account_id=s["account_id"],
                    account_name=s["account_name"],
                    ticker=s["ticker"],
                    qty=float(s["qty"]),
                    est_price=float(s["est_price"]),
                    est_value=float(s["est_value"]),
                    bucket_code=s.get("bucket_code"),
                    rationale=f"Rebalance/goal: reduce {s.get('bucket_code')} exposure; lots selected to minimize tax; wash={wash}.",
                    requires_override=requires_override,
                )
            )
            for p in picks:
                all_lot_picks.append(
                    LotPick(
                        ticker=s["ticker"],
                        lot_id=p.lot_id,
                        acquisition_date=p.acquisition_date.isoformat(),
                        qty=float(p.qty),
                        basis_allocated=float(p.basis_allocated),
                        unrealized=float(p.unrealized),
                        term=p.term,
                        wash_risk=wash if p.unrealized < 0 else "N/A",
                    )
                )

        # Post-trade single-name concentration check for buys (MVP uses starting MV + buy value).
        mv_by_ticker = defaultdict(float)
        for h in tp_holdings:
            mv_by_ticker[h.ticker] += float(h.market_value)
        for s in planned_sells:
            mv_by_ticker[s["ticker"]] -= float(s["est_value"])
        for b in planned_buys:
            mv_by_ticker[b["ticker"]] += float(b["est_value"])

        for b in planned_buys:
            total_after = total_value  # assumes reinvestment from cash, no external flows
            pct = (mv_by_ticker[b["ticker"]] / total_after) if total_after > 0 else 0.0
            requires_override = pct > max_single_name
            if requires_override:
                warnings.append(
                    f"{tp.name}: buy of {b['ticker']} exceeds max_single_name_pct ({pct:.2%} > {max_single_name:.2%}); override required."
                )
            all_trades.append(
                TradeRecommendation(
                    action="BUY",
                    account_id=b["account_id"],
                    account_name=b["account_name"],
                    ticker=b["ticker"],
                    qty=float(b["qty"]),
                    est_price=float(b["est_price"]),
                    est_value=float(b["est_value"]),
                    bucket_code=b.get("bucket_code"),
                    rationale=f"Rebalance/goal: add to {b.get('bucket_code')} using lowest-cost assigned ticker (MVP heuristic).",
                    requires_override=requires_override,
                )
            )

    # Build tax impact summary from deltas (MVP: no dividend/interest deltas from trades).
    tax_rows: list[TaxImpactRow] = []
    for tp in taxpayers:
        st = float(st_by_tp[tp.id])
        lt = float(lt_by_tp[tp.id])
        est = estimate_tax_delta(
            st_gains=st,
            lt_gains=lt,
            ordinary_income=0.0,
            qualified_dividends=0.0,
            nonqualified_dividends=0.0,
            interest=0.0,
            assumptions=config.tax,
        )
        tax_rows.append(
            TaxImpactRow(
                taxpayer=tp.name,
                st_delta=st,
                lt_delta=lt,
                ordinary_delta=0.0,
                estimated_tax_delta=est,
            )
        )

    # Projected post-trade drift (bucket overrides approach).
    bucket_overrides = {row.code: row.value for row in pre_drift.bucket_rows}
    for t in all_trades:
        code = t.bucket_code or "UNASSIGNED"
        if t.action == "SELL":
            bucket_overrides[code] = bucket_overrides.get(code, 0.0) - float(t.est_value)
            bucket_overrides["B1"] = bucket_overrides.get("B1", 0.0) + float(t.est_value)
        else:
            bucket_overrides[code] = bucket_overrides.get(code, 0.0) + float(t.est_value)
            bucket_overrides["B1"] = bucket_overrides.get("B1", 0.0) - float(t.est_value)

    post = compute_drift_report_with_overrides(
        session=session, policy_id=policy_id, scope=scope, bucket_value_overrides=bucket_overrides
    )

    # If projected drift violates min/max, flag override.
    for row in post.bucket_rows:
        b = buckets.get(row.code)
        if b is None:
            continue
        if row.actual_pct < float(b.min_pct) or row.actual_pct > float(b.max_pct):
            warnings.append(
                f"Projected {row.code} allocation {row.actual_pct:.2%} violates policy range ({float(b.min_pct):.2%}-{float(b.max_pct):.2%})."
            )

    outputs_json = {
        "policy_id": policy_id,
        "policy_effective_date": policy.effective_date.isoformat(),
        "warnings": warnings + post.warnings,
        "trades": [t.model_dump() for t in all_trades],
        "lot_picks": [p.model_dump() for p in all_lot_picks],
        "tax_impact": {"rows": [r.model_dump() for r in tax_rows], "assumptions": config.tax.as_json()},
        "substitute_suggestions": substitute_suggestions,
        "post_trade_drift": post.model_dump(),
    }
    inputs_json = {
        "as_of": as_of.isoformat(),
        "scope": scope,
        "config": config.model_dump(),
        "pre_trade_drift": pre_drift.model_dump(),
    }

    return PlannerResult(
        goal_json=goal,
        inputs_json=inputs_json,
        outputs_json=outputs_json,
        trades=all_trades,
        lot_picks=all_lot_picks,
        tax_impact=TaxImpactSummary(rows=tax_rows, assumptions=config.tax.as_json()),
        post_trade=post,
        warnings=warnings,
    )
