from __future__ import annotations

import inspect

import pandas as pd
import pytest

from src.regime import thematic_sleeve as tcs


def _frame(prices: list[float]) -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-02", periods=len(prices))
    return pd.DataFrame(
        {
            "open": prices,
            "high": [price * 1.01 for price in prices],
            "low": [price * 0.99 for price in prices],
            "price": prices,
            "volume": [2_000_000] * len(prices),
        },
        index=dates,
    )


def _fixture_frames() -> dict[str, pd.DataFrame]:
    days = 420

    def path(start: float, end: float) -> list[float]:
        return [start + (end - start) * idx / (days - 1) for idx in range(days)]

    frames = {
        "WIN1": _frame(path(100.0, 620.0)),
        "WIN2": _frame(path(100.0, 260.0)),
    }
    for idx in range(8):
        prices: list[float] = []
        for day in range(days):
            if day < 40:
                price = 100.0 - day * 0.25
            elif day < 70:
                price = 90.0 - (day - 40) * 0.9
            else:
                price = 63.0 + (day - 70) * 0.02
            prices.append(price)
        frames[f"LOS{idx}"] = _frame(prices)
    return frames


def _test_config(**updates) -> tcs.ThematicConvexitySleeveConfig:
    base = {
        "starting_cash": 100_000.0,
        "active_themes": {"test_theme": tuple(["WIN1", "WIN2", *[f"LOS{idx}" for idx in range(8)]])},
        "min_dollar_adv": 0.0,
        "min_listing_days": 1,
        "momentum_12_1_min_percentile": 0.0,
        "min_names_per_theme_at_entry": 3,
        "max_names_per_theme": 10,
        "min_cash_to_deploy": 1.0,
        "per_name_entry_pct": 2.0,
        "tranche_pct": 2.0,
        "per_theme_max_pct": 20.0,
        "sleeve_max_pct_of_portfolio": 35.0,
        "confirm_days": 5,
        "oos_start": "2021-01-01",
    }
    base.update(updates)
    return tcs.ThematicConvexitySleeveConfig(**base)


def test_right_skew_convexity() -> None:
    payload = tcs.run_thematic_sleeve_backtest(_fixture_frames(), _test_config(), windows=[])
    per_name = payload["per_name_pnl"]
    returns = pd.Series([row["total_return"] for row in per_name if row["total_return"] is not None])
    positives = sorted([row["pnl"] for row in per_name if row["pnl"] > 0], reverse=True)
    top_n = max(1, round(len(positives) * 0.20))
    top_share = sum(positives[:top_n]) / sum(positives)

    assert returns.median() <= 0.05
    assert top_share >= 0.60
    assert returns.skew() > 0.5
    assert payload["convexity"]["right_skew_ok"] is True


def test_bounded_per_name_loss() -> None:
    payload = tcs.run_thematic_sleeve_backtest(_fixture_frames(), _test_config(), windows=[])
    buffer = payload["config"]["initial_giveback_pct"] / 100.0 + 0.10
    for row in payload["per_name_pnl"]:
        deployed = float(row["deployed_capital"] or 0.0)
        loss = -float(row["pnl"] or 0.0)
        if deployed > 0 and loss > 0:
            assert loss <= deployed * buffer
            assert loss <= deployed


def test_exits_are_rule_coded_only() -> None:
    payload = tcs.run_thematic_sleeve_backtest(_fixture_frames(), _test_config(), windows=[])
    sell_reasons = {
        str(row["exit_type"])
        for row in payload["trades"]
        if str(row.get("side")).lower() == "sell"
    }

    assert sell_reasons
    assert sell_reasons <= tcs.EXIT_REASON_CODES
    assert not any("timing" in reason or "theme_top" in reason or "discretionary" in reason for reason in sell_reasons)
    source = (
        inspect.getsource(tcs)
        .replace("exit_discretionary_timing_exit", "")
        .replace("exit.discretionary_timing_exit", "")
    )
    assert "theme_top" not in source
    assert "discretionary_timing" not in source


def test_discipline_lock_integrity() -> None:
    config = tcs.ThematicConvexitySleeveConfig(active_themes={"alpha": ("AAA", "BBB", "CCC")})
    rotated = config.rotate_themes({"beta": ("DDD", "EEE", "FFF")})

    assert set(config.active_themes) == {"alpha"}
    assert set(rotated.active_themes) == {"beta"}
    before = {field: tcs._locked_snapshot(config)[field] for field in config.LOCKED_FIELDS}
    after = {field: tcs._locked_snapshot(rotated)[field] for field in rotated.LOCKED_FIELDS}
    assert before == after

    with pytest.raises(tcs.LockedParameterError):
        config.with_changes(per_name_entry_pct=2.0)

    with pytest.raises(tcs.LockedParameterError):
        config.with_changes(initial_giveback_pct=25.0)
