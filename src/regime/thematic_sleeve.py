from __future__ import annotations

import datetime as dt
import html
import math
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, ClassVar, Literal, Mapping, Sequence

import pandas as pd

from . import ccel_campaign as ccel
from .alpha_campaign import DEFAULT_BASKET_PATH, DEFAULT_CAMPAIGN_DIR, _git_sha, _json_safe, _read_json, _write_json, load_basket
from .paper_trading.planning import trailing_stop_level
from .portfolio_backtest import PortfolioBacktestConfig
from .portfolio_campaign import _campaign_row, _float, _fmt_num, _fmt_pct, _safe_name
from .portfolio_historical_campaign import (
    FrameLoader,
    _benchmark_relative_rows,
    _buy_hold_curve,
    _html_table,
    _limitations,
    _load_historical_frame,
    _period_returns,
    _slice_frame,
    _stress_results_for_curve,
    build_availability_report,
    historical_stress_windows_for_range,
)
from .sharadar import DEFAULT_SHARADAR_DIR, SharadarFrameLoader, certification_gate_status
from .stress_windows import StressWindow

DEFAULT_TCS_START = "2006-01-01"
DEFAULT_TCS_END = "2025-12-31"
DEFAULT_TCS_OOS_START = "2024-01-01"
DEFAULT_TCS_CAMPAIGN_DIR = DEFAULT_CAMPAIGN_DIR / "thematic_sleeve_v1_2006_2025"
DEFAULT_TCS_REPORT_DIR = Path("output") / "thematic_sleeve_v1_2006_2025_report"
DEFAULT_TCS_REPORT_PATH = DEFAULT_TCS_REPORT_DIR / "management_report.html"
TCS_PROXY_LABEL = "NOT survivorship-free / static-theme proxy"
EXIT_REASON_CODES = {"thesis_break", "oversize_trim", "trailing_stop", "momentum_decay_relegation"}


class LockedParameterError(ValueError):
    """Raised when a caller tries to mutate load-bearing TCS discipline fields."""


def default_static_themes() -> dict[str, tuple[str, ...]]:
    """Static v1 proxy themes over the existing campaign basket.

    This is intentionally a swappable membership map; point-in-time theme data can
    replace it later without changing the sleeve execution rules.
    """

    return {
        "ai_compute": ("NVDA", "AMD", "AAPL", "GOOGL", "META"),
        "digital_platforms": ("AMZN", "NFLX", "TSLA", "GOOGL", "META"),
        "consumer_scale": ("COST", "HD", "WMT", "KO", "AMZN"),
        "industrial_electrification": ("GE", "CAT", "BA", "LIN", "FCX", "NEE"),
        "compounder_health_finance": ("LLY", "UNH", "JNJ", "JPM", "V", "BRK-B"),
        "energy_resilience": ("XOM", "CVX", "COP", "NEM", "FCX", "AEP", "D"),
    }


def _normalize_themes(raw: Mapping[str, Sequence[str]] | None) -> dict[str, tuple[str, ...]]:
    themes: dict[str, tuple[str, ...]] = {}
    for theme, tickers in dict(raw or {}).items():
        names: list[str] = []
        seen: set[str] = set()
        for ticker in tickers or []:
            name = str(ticker or "").strip().upper()
            if name and name not in seen:
                seen.add(name)
                names.append(name)
        if names:
            themes[str(theme)] = tuple(names)
    return themes


@dataclass(frozen=True)
class ThematicConvexitySleeveConfig:
    strategy: str = "thematic_convexity_sleeve"
    version: int = 1
    starting_cash: float = 100_000.0
    entry_cost_bps: float = 5.0
    exit_cost_bps: float = 5.0
    st_tax_rate: float = 0.32
    lt_tax_rate: float = 0.20
    oos_start: str = DEFAULT_TCS_OOS_START
    sleeve_max_pct_of_portfolio: float = 20.0
    per_theme_max_pct: float = 8.0
    per_name_entry_pct: float = 1.5
    per_name_hard_cap_pct: float = 20.0
    min_names_per_theme_at_entry: int = 3
    max_names_per_theme: int = 8
    require_theme_membership: bool = True
    min_dollar_adv: float = 10_000_000.0
    min_listing_days: int = 180
    momentum_12_1_min_percentile: float = 50.0
    quality_gate: str = "pass"
    scale_in_enabled: bool = True
    tranche_pct: float = 1.5
    max_tranches: int = 3
    add_condition: str = "confirmation"
    never_trim_to_rebalance: bool = True
    significant_gain_min_hold_days: int = 365
    promote_to_core_at_pct: float = 6.0
    exit_discretionary_timing_exit: str = "forbidden"
    thesis_break_quality_gate_fails: bool = True
    thesis_break_theme_invalidation_flag: bool = True
    trailing_stop_enabled: bool = True
    trailing_stop_type: str = "ratchet_high_water"
    initial_giveback_pct: float = 30.0
    tightened_giveback_pct: float = 18.0
    tighten_after_gain_pct: float = 100.0
    momentum_decay_enabled: bool = True
    bottom_momentum_quantile: float = 0.25
    confirm_days: int = 21
    oversize_trim_enabled: bool = True
    trim_only_above_pct: float = 20.0
    oversize_trim_tax_aware: bool = True
    no_rebuy_days: int = 31
    min_cash_to_deploy: float = 250.0
    integer_shares: bool = True
    winner_run_enabled: bool = True
    active_themes: Mapping[str, Sequence[str]] = field(default_factory=default_static_themes)
    candidate_lists: Mapping[str, Sequence[str]] = field(default_factory=dict)
    theme_invalidation_flags: Mapping[str, bool] = field(default_factory=dict)
    research_label: str = "TCS_full"

    LOCKED_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {
            "capital.per_name_entry_pct",
            "capital.per_name_hard_cap_pct",
            "hold.never_trim_to_rebalance",
            "exit.discretionary_timing_exit",
            "exit.triggers.thesis_break.quality_gate_fails",
            "exit.triggers.thesis_break.theme_invalidation_flag",
            "exit.triggers.trailing_stop.enabled",
            "exit.triggers.trailing_stop.type",
            "exit.triggers.trailing_stop.initial_giveback_pct",
            "exit.triggers.trailing_stop.tightened_giveback_pct",
            "exit.triggers.trailing_stop.tighten_after_gain_pct",
            "exit.triggers.momentum_decay_relegation.enabled",
            "exit.triggers.momentum_decay_relegation.bottom_momentum_quantile",
            "exit.triggers.momentum_decay_relegation.confirm_days",
            "exit.triggers.oversize_trim.enabled",
            "exit.triggers.oversize_trim.trim_only_above_pct",
            "exit.triggers.oversize_trim.tax_aware",
            "exit.no_rebuy_days",
        }
    )
    _FIELD_LOCK_MAP: ClassVar[dict[str, str]] = {
        "per_name_entry_pct": "capital.per_name_entry_pct",
        "per_name_hard_cap_pct": "capital.per_name_hard_cap_pct",
        "never_trim_to_rebalance": "hold.never_trim_to_rebalance",
        "exit_discretionary_timing_exit": "exit.discretionary_timing_exit",
        "thesis_break_quality_gate_fails": "exit.triggers.thesis_break.quality_gate_fails",
        "thesis_break_theme_invalidation_flag": "exit.triggers.thesis_break.theme_invalidation_flag",
        "trailing_stop_enabled": "exit.triggers.trailing_stop.enabled",
        "trailing_stop_type": "exit.triggers.trailing_stop.type",
        "initial_giveback_pct": "exit.triggers.trailing_stop.initial_giveback_pct",
        "tightened_giveback_pct": "exit.triggers.trailing_stop.tightened_giveback_pct",
        "tighten_after_gain_pct": "exit.triggers.trailing_stop.tighten_after_gain_pct",
        "momentum_decay_enabled": "exit.triggers.momentum_decay_relegation.enabled",
        "bottom_momentum_quantile": "exit.triggers.momentum_decay_relegation.bottom_momentum_quantile",
        "confirm_days": "exit.triggers.momentum_decay_relegation.confirm_days",
        "oversize_trim_enabled": "exit.triggers.oversize_trim.enabled",
        "trim_only_above_pct": "exit.triggers.oversize_trim.trim_only_above_pct",
        "oversize_trim_tax_aware": "exit.triggers.oversize_trim.tax_aware",
        "no_rebuy_days": "exit.no_rebuy_days",
    }

    def __post_init__(self) -> None:
        if self.exit_discretionary_timing_exit != "forbidden":
            raise LockedParameterError("exit.discretionary_timing_exit must remain forbidden.")
        object.__setattr__(self, "active_themes", _normalize_themes(self.active_themes))
        object.__setattr__(self, "candidate_lists", _normalize_themes(self.candidate_lists))
        object.__setattr__(
            self,
            "theme_invalidation_flags",
            {str(theme): bool(value) for theme, value in dict(self.theme_invalidation_flags or {}).items()},
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["active_themes"] = {theme: list(tickers) for theme, tickers in self.active_themes.items()}
        payload["candidate_lists"] = {theme: list(tickers) for theme, tickers in self.candidate_lists.items()}
        payload["proxy_label"] = TCS_PROXY_LABEL
        payload["production_defaults_changed"] = False
        return payload

    def with_changes(self, **kw: Any) -> "ThematicConvexitySleeveConfig":
        locked = [
            key
            for key in kw
            if key in self.LOCKED_FIELDS
            or self._FIELD_LOCK_MAP.get(key) in self.LOCKED_FIELDS
            or (str(key).startswith("exit.") and key != "exit.note")
        ]
        if locked:
            raise LockedParameterError(f"TCS discipline lock blocks changes to: {', '.join(sorted(locked))}")
        return replace(self, **kw)

    def rotate_themes(
        self,
        new_active_themes: Mapping[str, Sequence[str]] | Sequence[str],
        new_candidate_lists: Mapping[str, Sequence[str]] | None = None,
    ) -> "ThematicConvexitySleeveConfig":
        before = _locked_snapshot(self)
        if isinstance(new_active_themes, Mapping):
            themes = _normalize_themes(new_active_themes)
        else:
            candidates = _normalize_themes(new_candidate_lists or self.candidate_lists or self.active_themes)
            selected = {str(theme): candidates.get(str(theme), ()) for theme in new_active_themes}
            themes = _normalize_themes(selected)
        candidate_lists = _normalize_themes(new_candidate_lists) if new_candidate_lists is not None else _normalize_themes(self.candidate_lists)
        rotated = replace(self, active_themes=themes, candidate_lists=candidate_lists)
        after = _locked_snapshot(rotated)
        if before != after:
            raise LockedParameterError("Theme rotation attempted to modify locked TCS discipline fields.")
        return rotated


def _locked_snapshot(config: ThematicConvexitySleeveConfig) -> dict[str, Any]:
    payload = config.to_dict()
    out: dict[str, Any] = {}
    for field_name, locked_name in config._FIELD_LOCK_MAP.items():
        if locked_name in config.LOCKED_FIELDS:
            out[locked_name] = payload.get(field_name)
    return out


def run_thematic_sleeve_campaign(
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_TCS_CAMPAIGN_DIR,
    report_dir: str | Path = DEFAULT_TCS_REPORT_DIR,
    start: str = DEFAULT_TCS_START,
    end: str = DEFAULT_TCS_END,
    oos_start: str = DEFAULT_TCS_OOS_START,
    resume: bool = False,
    frame_loader: FrameLoader | None = None,
    data_source: Literal["proxy", "sharadar"] = "proxy",
    sharadar_store_dir: str | Path = DEFAULT_SHARADAR_DIR,
    render_report: bool = True,
) -> dict[str, Any]:
    started = dt.datetime.now(dt.timezone.utc)
    root = Path(campaign_dir)
    result_dir = root / "results"
    result_dir.mkdir(parents=True, exist_ok=True)
    basket = load_basket(basket_path)
    tickers = [str(ticker).upper() for ticker in basket.get("tickers") or []]
    if not tickers:
        raise ValueError("TCS campaign requires a pinned campaign basket.")

    source = str(data_source or "proxy").lower()
    if source not in {"proxy", "sharadar"}:
        raise ValueError("data_source must be 'proxy' or 'sharadar'.")
    loader = frame_loader or (SharadarFrameLoader(sharadar_store_dir) if source == "sharadar" else _load_historical_frame)
    raw_frames: dict[str, pd.DataFrame] = {}
    load_errors: dict[str, str] = {}
    for ticker in tickers:
        try:
            frame = _slice_frame(loader(ticker, start, end), start, end)
        except Exception as exc:
            load_errors[ticker] = str(exc)
            continue
        if frame.empty:
            load_errors[ticker] = "empty adjusted-price frame for requested window"
            continue
        raw_frames[ticker] = frame
    if not raw_frames:
        raise ValueError("No basket constituents have usable TCS history.")

    benchmarks: dict[str, pd.DataFrame] = {}
    for ticker in ("SPY", "QQQ"):
        try:
            frame = _slice_frame(loader(ticker, start, end), start, end)
        except Exception as exc:
            load_errors[ticker] = str(exc)
            frame = pd.DataFrame()
        if frame.empty:
            load_errors[ticker] = load_errors.get(ticker, "empty benchmark history")
        benchmarks[ticker] = frame
    if benchmarks["SPY"].empty or benchmarks["QQQ"].empty:
        raise ValueError("SPY and QQQ benchmark history are required for TCS.")

    readiness, snapshot_hash = ccel._data_layer_status(
        data_source=source,
        loader=loader,
        tickers=[*tickers, "SPY", "QQQ"],
        start=start,
        end=end,
    )
    windows = historical_stress_windows_for_range(start, end)
    benchmark_curve = _buy_hold_curve(benchmarks["SPY"], starting_cash=100_000.0)
    results: dict[str, dict[str, Any]] = {}
    for arm, cfg in thematic_sleeve_arm_configs(oos_start=oos_start).items():
        path = result_dir / f"{_safe_name(arm)}.json"
        if resume and path.exists():
            payload = ccel._stamp_data_layer(dict(_read_json(path)), data_source=source, readiness=readiness, snapshot_hash=snapshot_hash)
            _write_json(path, payload)
            results[arm] = payload
            continue
        payload = run_thematic_sleeve_backtest(raw_frames, cfg, benchmark_curve=benchmark_curve, windows=windows)
        payload["campaign"] = _campaign_metadata(arm=arm, start=start, end=end, oos_start=oos_start, kind="tcs_proxy")
        payload = ccel._stamp_data_layer(payload, data_source=source, readiness=readiness, snapshot_hash=snapshot_hash)
        _write_json(path, payload)
        results[arm] = payload

    for arm, payload in _reference_arms(
        raw_frames=raw_frames,
        benchmarks=benchmarks,
        start=start,
        end=end,
        oos_start=oos_start,
        windows=windows,
        benchmark_curve=benchmark_curve,
    ).items():
        path = result_dir / f"{_safe_name(arm)}.json"
        if resume and path.exists():
            payload = ccel._stamp_data_layer(dict(_read_json(path)), data_source=source, readiness=readiness, snapshot_hash=snapshot_hash)
            _write_json(path, payload)
            results[arm] = payload
            continue
        payload["campaign"] = _campaign_metadata(arm=arm, start=start, end=end, oos_start=oos_start, kind="reference")
        payload = ccel._stamp_data_layer(payload, data_source=source, readiness=readiness, snapshot_hash=snapshot_hash)
        _write_json(path, payload)
        results[arm] = payload

    for arm, payload in _overlay_arms(results=results, benchmark_curve=benchmark_curve, windows=windows).items():
        path = result_dir / f"{_safe_name(arm)}.json"
        if resume and path.exists():
            payload = ccel._stamp_data_layer(dict(_read_json(path)), data_source=source, readiness=readiness, snapshot_hash=snapshot_hash)
            _write_json(path, payload)
            results[arm] = payload
            continue
        payload["campaign"] = _campaign_metadata(arm=arm, start=start, end=end, oos_start=oos_start, kind="overlay")
        payload = ccel._stamp_data_layer(payload, data_source=source, readiness=readiness, snapshot_hash=snapshot_hash)
        _write_json(path, payload)
        results[arm] = payload

    rows = [_tcs_row(arm, payload, result_dir / f"{_safe_name(arm)}.json") for arm, payload in results.items()]
    rows = sorted(rows, key=_row_order)
    verdict = thematic_sleeve_verdict(rows, results)
    verdict["data_readiness"] = readiness.get("data_readiness")
    verdict["certification_gate_status"] = certification_gate_status(
        str(readiness.get("data_readiness") or "price_only_proxy"),
        after_tax=True,
        out_of_sample=True,
        killed=str(verdict.get("validation_status") or "") == "killed",
    )
    monthly_returns = {
        arm: _period_returns(payload.get("after_tax_equity_curve") or payload.get("equity_curve") or [], "M")
        for arm, payload in results.items()
    }
    yearly_returns = {
        arm: _period_returns(payload.get("after_tax_equity_curve") or payload.get("equity_curve") or [], "Y")
        for arm, payload in results.items()
    }
    finished = dt.datetime.now(dt.timezone.utc)
    coverage = build_availability_report(raw_frames, start=start, end=end, load_errors=load_errors, basket=basket)
    summary = {
        "schema": "regime_thematic_sleeve_campaign.v1",
        "generated_at": finished.isoformat(),
        "git_sha": _git_sha(),
        "wall_clock_seconds": (finished - started).total_seconds(),
        "basket_path": str(basket_path),
        "basket_size": len(tickers),
        "available_ticker_count": len(raw_frames),
        "tickers": tickers,
        "available_tickers": sorted(raw_frames),
        "unavailable_tickers": sorted(load_errors),
        "start": start,
        "end": end,
        "oos_start": oos_start,
        "availability_mode": "panel",
        "data_source": source,
        "data_readiness": readiness.get("data_readiness"),
        "readiness": readiness,
        "data_snapshot_hash": snapshot_hash,
        "proxy_label": TCS_PROXY_LABEL,
        "adjusted_price_note": _tcs_adjusted_price_note(source),
        "rows": rows,
        "benchmark_relative": _benchmark_relative_rows(rows),
        "monthly_returns": monthly_returns,
        "yearly_returns": yearly_returns,
        "coverage": coverage,
        "stress_windows": [window.to_dict() for window in windows],
        "verdict": verdict,
        "single_command": (
            "python -m src.regime.cli thematic-sleeve-campaign run "
            f"--start {start} --end {end} --oos-start {oos_start} --campaign-dir {root} --report-dir {Path(report_dir)} "
            f"{ccel._data_source_cli_args(source, sharadar_store_dir)}--resume"
        ),
        "production_defaults_changed": False,
        "limitations": thematic_sleeve_limitations(start=start, end=end, load_errors=load_errors) + ccel._data_layer_limitations(source, readiness),
    }
    _write_json(root / "summary.json", summary)
    _write_json(
        root / "cache_manifest.json",
        {
            "schema": "regime_thematic_sleeve_cache.v1",
            "generated_at": started.isoformat(),
            "git_sha": _git_sha(),
            "start": start,
            "end": end,
            "oos_start": oos_start,
            "tickers": tickers,
            "available_tickers": sorted(raw_frames),
            "benchmarks": ["SPY", "QQQ"],
            "data_source": source,
            "data_readiness": readiness.get("data_readiness"),
            "readiness": readiness,
            "data_snapshot_hash": snapshot_hash,
            "proxy_label": TCS_PROXY_LABEL,
        },
    )
    if render_report:
        report_path = render_thematic_sleeve_report(campaign_dir=root, output_dir=report_dir)
        summary["report_path"] = str(report_path)
        _write_json(root / "summary.json", summary)
    return summary


def thematic_sleeve_arm_configs(*, oos_start: str = DEFAULT_TCS_OOS_START) -> dict[str, ThematicConvexitySleeveConfig]:
    base = ThematicConvexitySleeveConfig(oos_start=oos_start)
    return {
        "TCS_full": base,
        "TCS_winners_capped": _research_config(base, winner_run_enabled=False, research_label="TCS_winners_capped"),
        "TCS_momentum_decay_off": _research_config(base, momentum_decay_enabled=False, research_label="TCS_momentum_decay_off"),
        "TCS_entry_1pct": _research_config(base, per_name_entry_pct=1.0, tranche_pct=1.0, research_label="TCS_entry_1pct"),
        "TCS_entry_2pct": _research_config(base, per_name_entry_pct=2.0, tranche_pct=2.0, research_label="TCS_entry_2pct"),
        "TCS_cap_15pct": _research_config(base, per_name_hard_cap_pct=15.0, trim_only_above_pct=15.0, research_label="TCS_cap_15pct"),
    }


def _research_config(config: ThematicConvexitySleeveConfig, **updates: Any) -> ThematicConvexitySleeveConfig:
    """Research-only sensitivity helper; ordinary callers must use with_changes."""

    return replace(config, **updates)


def run_thematic_sleeve_backtest(
    market_frames: dict[str, pd.DataFrame],
    config: ThematicConvexitySleeveConfig | None = None,
    *,
    benchmark_curve: pd.DataFrame | None = None,
    windows: list[StressWindow] | None = None,
) -> dict[str, Any]:
    cfg = config or ThematicConvexitySleeveConfig()
    frames = {str(ticker).upper(): ccel._normalize_frame(frame) for ticker, frame in market_frames.items() if not frame.empty}
    frames = {ticker: frame for ticker, frame in frames.items() if not frame.empty}
    if not frames:
        raise ValueError("TCS backtest requires at least one frame.")
    dates = ccel._panel_dates(frames)
    if len(dates) < 2:
        raise ValueError("TCS backtest requires at least two trading dates.")

    cash = float(cfg.starting_cash)
    lots: list[ccel.CCELLot] = []
    trades: list[dict[str, Any]] = []
    audit_events: list[dict[str, Any]] = []
    realizations: list[dict[str, Any]] = []
    no_rebuy_until: dict[str, pd.Timestamp] = {}
    promoted_core: set[str] = set()
    ticker_theme = _ticker_theme_map(cfg)
    tranches: dict[str, int] = {}
    high_water: dict[str, float] = {}
    stop_prices: dict[str, float] = {}
    bottom_since: dict[str, pd.Timestamp] = {}
    deployed_capital: dict[str, float] = {}
    last_prices: dict[str, float] = {}
    equity_curve: list[dict[str, Any]] = []
    next_lot_id = 1
    total_costs = 0.0
    total_turnover = 0.0

    for idx, date in enumerate(dates):
        active = {ticker for ticker, frame in frames.items() if pd.Timestamp(date) in frame.index}
        if not active:
            continue
        prices = dict(last_prices)
        for ticker in active:
            prices[ticker] = float(frames[ticker].loc[date, "price"])
        last_prices.update(prices)

        cash, exit_trades, exit_realized, exit_costs, exit_turnover = _process_exits(
            date=date,
            frames=frames,
            active=active,
            lots=lots,
            cash=cash,
            prices=prices,
            cfg=cfg,
            promoted_core=promoted_core,
            ticker_theme=ticker_theme,
            no_rebuy_until=no_rebuy_until,
            tranches=tranches,
            high_water=high_water,
            stop_prices=stop_prices,
            bottom_since=bottom_since,
        )
        trades.extend(exit_trades)
        audit_events.extend(_audit_from_trades(exit_trades))
        realizations.extend(exit_realized)
        total_costs += exit_costs
        total_turnover += exit_turnover

        _process_promotions(date, lots, cash, prices, cfg, promoted_core, audit_events)

        is_entry_review = idx == 0 or (idx > 0 and pd.Timestamp(dates[idx - 1]).month != pd.Timestamp(date).month)
        if is_entry_review:
            cash, next_lot_id, buy_trades, buy_costs, buy_turnover = _process_entries(
                date=date,
                frames=frames,
                active=active,
                lots=lots,
                cash=cash,
                prices=prices,
                cfg=cfg,
                ticker_theme=ticker_theme,
                no_rebuy_until=no_rebuy_until,
                promoted_core=promoted_core,
                tranches=tranches,
                high_water=high_water,
                stop_prices=stop_prices,
                next_lot_id=next_lot_id,
                deployed_capital=deployed_capital,
            )
            trades.extend(buy_trades)
            audit_events.extend(_audit_from_trades(buy_trades))
            total_costs += buy_costs
            total_turnover += buy_turnover

        cash, next_lot_id, scale_trades, scale_costs, scale_turnover = _process_scale_ins(
            date=date,
            frames=frames,
            active=active,
            lots=lots,
            cash=cash,
            prices=prices,
            cfg=cfg,
            ticker_theme=ticker_theme,
            promoted_core=promoted_core,
            tranches=tranches,
            high_water=high_water,
            stop_prices=stop_prices,
            next_lot_id=next_lot_id,
            deployed_capital=deployed_capital,
        )
        trades.extend(scale_trades)
        audit_events.extend(_audit_from_trades(scale_trades))
        total_costs += scale_costs
        total_turnover += scale_turnover

        position_value = ccel._position_value(lots, prices)
        equity = cash + position_value
        equity_curve.append(
            {
                "date": ccel._date_text(date),
                "equity": equity,
                "cash": cash,
                "position_value": position_value,
                "sleeve_value": _sleeve_value(lots, prices, promoted_core),
                "core_value": _core_value(lots, prices, promoted_core),
                "exposure": position_value / equity if equity > 0 else 0.0,
                "costs_paid": 0.0,
                "turnover": 0.0,
                "open_lot_count": len(lots),
            }
        )

    taxable = ccel.apply_wash_sales(realizations, trades)
    after_tax_curve, tax_summary = ccel.build_after_tax_curve(
        equity_curve,
        taxable,
        lots,
        last_prices,
        st_tax_rate=cfg.st_tax_rate,
        lt_tax_rate=cfg.lt_tax_rate,
    )
    after_tax_metrics = ccel._metrics(after_tax_curve, trades, benchmark_curve=benchmark_curve)
    pre_tax_metrics = ccel._metrics(equity_curve, trades, benchmark_curve=benchmark_curve)
    after_tax_metrics["annualized_turnover"] = ccel._annualized_turnover(total_turnover, len(equity_curve))
    after_tax_metrics["total_turnover"] = total_turnover
    after_tax_metrics["total_costs_paid"] = total_costs
    after_tax_metrics["after_tax_terminal_wealth"] = after_tax_curve[-1]["equity"] if after_tax_curve else None
    after_tax_metrics["pre_tax_terminal_wealth"] = equity_curve[-1]["equity"] if equity_curve else None
    after_tax_metrics["terminal_tax_liability"] = tax_summary.get("terminal_tax_liability")
    after_tax_metrics["taxes_paid"] = tax_summary.get("taxes_paid")
    per_name = _per_name_pnl(trades, lots, last_prices, deployed_capital)
    convexity = _convexity_stats(per_name)
    oos_start = pd.Timestamp(cfg.oos_start) if cfg.oos_start else None
    stress_benchmark = benchmark_curve if benchmark_curve is not None else pd.DataFrame()
    return {
        "schema": "regime_thematic_sleeve_backtest.v1",
        "proxy_label": TCS_PROXY_LABEL,
        "config": cfg.to_dict(),
        "strategy_spec": {"name": "TCS_static_theme_proxy", "research_only": True},
        "strategy_hash": ccel._stable_hash(cfg.to_dict()),
        "git_sha": _git_sha(),
        "metrics": _json_safe(after_tax_metrics),
        "pre_tax_metrics": _json_safe(pre_tax_metrics),
        "in_sample": _json_safe(ccel._segment_metrics(after_tax_curve, trades, benchmark_curve, None, oos_start)),
        "out_of_sample": _json_safe(ccel._segment_metrics(after_tax_curve, trades, benchmark_curve, oos_start, None) if oos_start is not None else None),
        "equity_curve": _json_safe(equity_curve),
        "after_tax_equity_curve": _json_safe(after_tax_curve),
        "trades": _json_safe(trades),
        "audit_events": _json_safe(audit_events),
        "realized_lots": _json_safe(taxable),
        "tax_summary": _json_safe(tax_summary),
        "stress_windows": _json_safe(_stress_results_for_curve(pd.DataFrame(after_tax_curve), trades, stress_benchmark, windows or [])),
        "open_lots": _json_safe([lot.to_dict() for lot in lots]),
        "promoted_core": sorted(promoted_core),
        "no_rebuy_until": {ticker: ccel._date_text(date) for ticker, date in no_rebuy_until.items()},
        "per_name_pnl": _json_safe(per_name),
        "convexity": _json_safe(convexity),
        "production_defaults_changed": False,
    }


def _process_exits(
    *,
    date: pd.Timestamp,
    frames: dict[str, pd.DataFrame],
    active: set[str],
    lots: list[ccel.CCELLot],
    cash: float,
    prices: dict[str, float],
    cfg: ThematicConvexitySleeveConfig,
    promoted_core: set[str],
    ticker_theme: dict[str, str],
    no_rebuy_until: dict[str, pd.Timestamp],
    tranches: dict[str, int],
    high_water: dict[str, float],
    stop_prices: dict[str, float],
    bottom_since: dict[str, pd.Timestamp],
) -> tuple[float, list[dict[str, Any]], list[dict[str, Any]], float, float]:
    trades: list[dict[str, Any]] = []
    realizations: list[dict[str, Any]] = []
    costs = 0.0
    turnover = 0.0
    for ticker in sorted(ccel._held_tickers(lots)):
        price = float(prices.get(ticker, 0.0))
        if ticker not in active or price <= 0:
            continue
        qty = _held_quantity(lots, ticker)
        if qty <= 0:
            continue
        reason: str | None = None
        sell_qty = qty
        theme = ticker_theme.get(ticker)
        if _thesis_break(ticker, theme, date, frames, cfg):
            reason = "thesis_break"
        elif cfg.oversize_trim_enabled:
            total = cash + ccel._position_value(lots, prices)
            weight = qty * price / total if total > 0 else 0.0
            cap = (cfg.per_name_hard_cap_pct if cfg.winner_run_enabled else cfg.promote_to_core_at_pct) / 100.0
            if weight > cap:
                reason = "oversize_trim"
                target_value = cap * total
                sell_qty = max(0.0, (qty * price - target_value) / price)
                if cfg.integer_shares:
                    sell_qty = math.floor(sell_qty)
        if reason is None and ticker not in promoted_core and cfg.trailing_stop_enabled:
            _update_high_water_and_stop(ticker, price, lots, high_water, stop_prices, cfg)
            stop = stop_prices.get(ticker)
            if stop is not None and price <= float(stop):
                reason = "trailing_stop"
        if reason is None and ticker not in promoted_core and cfg.momentum_decay_enabled:
            if _bottom_momentum(ticker, date, frames, active, cfg.bottom_momentum_quantile):
                bottom_since.setdefault(ticker, pd.Timestamp(date))
            else:
                bottom_since.pop(ticker, None)
            since = bottom_since.get(ticker)
            basis = _weighted_basis_for_ticker(lots, ticker)
            if since is not None and (pd.Timestamp(date) - since).days >= cfg.confirm_days and price < basis:
                reason = "momentum_decay_relegation"
        if reason is None or sell_qty <= 0:
            continue
        if reason != "thesis_break" and _significant_short_gain(ticker, price, date, lots, cfg):
            continue
        cash_delta, realized, trade, cost, notional = _sell_ticker_quantity(
            date=date,
            ticker=ticker,
            quantity=min(sell_qty, qty),
            price=price,
            lots=lots,
            reason=reason,
            cfg=cfg,
            tax_aware=reason == "oversize_trim" and cfg.oversize_trim_tax_aware,
        )
        if trade is None:
            continue
        gain = sum(float(row.get("gain") or 0.0) for row in realized)
        if gain < 0:
            no_rebuy_until[ticker] = pd.Timestamp(date) + pd.Timedelta(days=cfg.no_rebuy_days)
        cash += cash_delta
        trades.append(trade)
        realizations.extend(realized)
        costs += cost
        turnover += notional
        if _held_quantity(lots, ticker) <= 1e-9:
            promoted_core.discard(ticker)
            tranches.pop(ticker, None)
            high_water.pop(ticker, None)
            stop_prices.pop(ticker, None)
            bottom_since.pop(ticker, None)
    return cash, trades, realizations, costs, turnover / max(1.0, cfg.starting_cash)


def _process_entries(
    *,
    date: pd.Timestamp,
    frames: dict[str, pd.DataFrame],
    active: set[str],
    lots: list[ccel.CCELLot],
    cash: float,
    prices: dict[str, float],
    cfg: ThematicConvexitySleeveConfig,
    ticker_theme: dict[str, str],
    no_rebuy_until: dict[str, pd.Timestamp],
    promoted_core: set[str],
    tranches: dict[str, int],
    high_water: dict[str, float],
    stop_prices: dict[str, float],
    next_lot_id: int,
    deployed_capital: dict[str, float],
) -> tuple[float, int, list[dict[str, Any]], float, float]:
    trades: list[dict[str, Any]] = []
    costs = 0.0
    turnover = 0.0
    for theme, tickers in cfg.active_themes.items():
        eligible = _eligible_theme_candidates(theme, tickers, date, frames, active, lots, cfg, no_rebuy_until)
        if len(eligible) < cfg.min_names_per_theme_at_entry:
            continue
        held_theme_count = _theme_sleeve_count(theme, lots, ticker_theme, promoted_core)
        slots = max(0, int(cfg.max_names_per_theme) - held_theme_count)
        for ticker in eligible[:slots]:
            if ticker in promoted_core or _held_quantity(lots, ticker) > 0:
                continue
            cash, next_lot_id, trade, cost, notional = _buy_tranche(
                date=date,
                ticker=ticker,
                price=float(prices.get(ticker, 0.0)),
                lots=lots,
                cash=cash,
                prices=prices,
                cfg=cfg,
                next_lot_id=next_lot_id,
                promoted_core=promoted_core,
                ticker_theme=ticker_theme,
                theme=theme,
                reason="entry",
            )
            if trade is None:
                continue
            tranches[ticker] = tranches.get(ticker, 0) + 1
            high_water[ticker] = max(float(prices[ticker]), high_water.get(ticker, 0.0))
            _update_high_water_and_stop(ticker, float(prices[ticker]), lots, high_water, stop_prices, cfg)
            deployed_capital[ticker] = deployed_capital.get(ticker, 0.0) + notional + cost
            trades.append(trade)
            costs += cost
            turnover += notional
    return cash, next_lot_id, trades, costs, turnover / max(1.0, cfg.starting_cash)


def _process_scale_ins(
    *,
    date: pd.Timestamp,
    frames: dict[str, pd.DataFrame],
    active: set[str],
    lots: list[ccel.CCELLot],
    cash: float,
    prices: dict[str, float],
    cfg: ThematicConvexitySleeveConfig,
    ticker_theme: dict[str, str],
    promoted_core: set[str],
    tranches: dict[str, int],
    high_water: dict[str, float],
    stop_prices: dict[str, float],
    next_lot_id: int,
    deployed_capital: dict[str, float],
) -> tuple[float, int, list[dict[str, Any]], float, float]:
    if not cfg.scale_in_enabled or cfg.add_condition != "confirmation":
        return cash, next_lot_id, [], 0.0, 0.0
    trades: list[dict[str, Any]] = []
    costs = 0.0
    turnover = 0.0
    for ticker in sorted(ccel._held_tickers(lots)):
        if ticker not in active or ticker in promoted_core:
            continue
        if tranches.get(ticker, 0) >= int(cfg.max_tranches):
            continue
        price = float(prices.get(ticker, 0.0))
        previous_high = float(high_water.get(ticker, 0.0))
        basis = _weighted_basis_for_ticker(lots, ticker)
        if price <= 0 or previous_high <= 0 or price <= previous_high or price < basis:
            continue
        theme = ticker_theme.get(ticker, "")
        cash, next_lot_id, trade, cost, notional = _buy_tranche(
            date=date,
            ticker=ticker,
            price=price,
            lots=lots,
            cash=cash,
            prices=prices,
            cfg=cfg,
            next_lot_id=next_lot_id,
            promoted_core=promoted_core,
            ticker_theme=ticker_theme,
            theme=theme,
            reason="scale_in",
        )
        if trade is None:
            continue
        tranches[ticker] = tranches.get(ticker, 0) + 1
        deployed_capital[ticker] = deployed_capital.get(ticker, 0.0) + notional + cost
        trades.append(trade)
        costs += cost
        turnover += notional
        high_water[ticker] = max(previous_high, price)
        _update_high_water_and_stop(ticker, price, lots, high_water, stop_prices, cfg)
    return cash, next_lot_id, trades, costs, turnover / max(1.0, cfg.starting_cash)


def _process_promotions(
    date: pd.Timestamp,
    lots: list[ccel.CCELLot],
    cash: float,
    prices: dict[str, float],
    cfg: ThematicConvexitySleeveConfig,
    promoted_core: set[str],
    audit_events: list[dict[str, Any]],
) -> None:
    if not cfg.winner_run_enabled:
        return
    total = cash + ccel._position_value(lots, prices)
    if total <= 0:
        return
    for ticker in sorted(ccel._held_tickers(lots)):
        if ticker in promoted_core:
            continue
        price = float(prices.get(ticker, 0.0))
        qty = _held_quantity(lots, ticker)
        if price <= 0 or qty <= 0:
            continue
        weight = qty * price / total
        if weight >= cfg.promote_to_core_at_pct / 100.0:
            promoted_core.add(ticker)
            audit_events.append(
                {
                    "date": ccel._date_text(date),
                    "ticker": ticker,
                    "action": "promote",
                    "reason": "promote_to_core",
                    "weight": weight,
                }
            )


def _buy_tranche(
    *,
    date: pd.Timestamp,
    ticker: str,
    price: float,
    lots: list[ccel.CCELLot],
    cash: float,
    prices: dict[str, float],
    cfg: ThematicConvexitySleeveConfig,
    next_lot_id: int,
    promoted_core: set[str],
    ticker_theme: dict[str, str],
    theme: str,
    reason: str,
) -> tuple[float, int, dict[str, Any] | None, float, float]:
    if price <= 0 or cash < cfg.min_cash_to_deploy:
        return cash, next_lot_id, None, 0.0, 0.0
    equity = cash + ccel._position_value(lots, prices)
    sleeve_available = max(0.0, equity * cfg.sleeve_max_pct_of_portfolio / 100.0 - _sleeve_value(lots, prices, promoted_core))
    theme_available = max(0.0, equity * cfg.per_theme_max_pct / 100.0 - _theme_value(theme, lots, prices, ticker_theme, promoted_core))
    budget = min(equity * cfg.tranche_pct / 100.0, sleeve_available, theme_available, cash)
    if budget < cfg.min_cash_to_deploy:
        return cash, next_lot_id, None, 0.0, 0.0
    unit_cost = price * (1.0 + cfg.entry_cost_bps / 10_000.0)
    qty = math.floor(budget / unit_cost) if cfg.integer_shares else budget / unit_cost
    affordable = math.floor(cash / unit_cost) if cfg.integer_shares else cash / unit_cost
    qty = min(qty, affordable)
    if qty <= 0:
        return cash, next_lot_id, None, 0.0, 0.0
    cost = qty * price * cfg.entry_cost_bps / 10_000.0
    cash -= qty * price + cost
    lots.append(ccel.CCELLot(next_lot_id, ticker, qty, price + (cost / qty if qty else 0.0), ccel._date_text(date)))
    next_lot_id += 1
    trade = ccel._trade_row(date, ticker, "Buy", qty, price, qty * price, cost, -cost, reason)
    trade["theme"] = theme
    return cash, next_lot_id, trade, cost, qty * price


def _sell_ticker_quantity(
    *,
    date: pd.Timestamp,
    ticker: str,
    quantity: float,
    price: float,
    lots: list[ccel.CCELLot],
    reason: str,
    cfg: ThematicConvexitySleeveConfig,
    tax_aware: bool,
) -> tuple[float, list[dict[str, Any]], dict[str, Any] | None, float, float]:
    quantity = min(float(quantity), _held_quantity(lots, ticker))
    if cfg.integer_shares:
        quantity = math.floor(quantity)
    if quantity <= 0 or price <= 0:
        return 0.0, [], None, 0.0, 0.0
    cost = quantity * price * cfg.exit_cost_bps / 10_000.0
    proceeds_per_share = price - (cost / quantity if quantity else 0.0)
    if tax_aware:
        realized = _sell_tax_aware_lots(lots, ticker, quantity, proceeds_per_share, ccel._date_text(date), reason)
    else:
        realized = ccel._sell_fifo_lots(lots, ticker, quantity, proceeds_per_share, ccel._date_text(date), reason)
    gain = sum(float(row.get("gain") or 0.0) for row in realized)
    notional = quantity * price
    trade = ccel._trade_row(date, ticker, "Sell", quantity, price, notional, cost, gain, reason)
    return notional - cost, realized, trade, cost, notional


def _sell_tax_aware_lots(
    lots: list[ccel.CCELLot],
    ticker: str,
    quantity: float,
    proceeds_per_share: float,
    date: str,
    reason: str,
) -> list[dict[str, Any]]:
    as_of = pd.Timestamp(date)

    def key(lot: ccel.CCELLot) -> tuple[int, int, float, str, int]:
        holding_days = (as_of - pd.Timestamp(lot.acquisition_date)).days
        gain_per_share = proceeds_per_share - lot.basis_per_share
        is_loss = gain_per_share < 0
        is_long = holding_days > 365
        return (0 if is_loss else 1, 0 if is_long else 1, -lot.basis_per_share, lot.acquisition_date, lot.lot_id)

    remaining = float(quantity)
    realized: list[dict[str, Any]] = []
    for lot in sorted([lot for lot in lots if lot.ticker == ticker and lot.quantity > 0], key=key):
        if remaining <= 1e-9:
            break
        take = min(lot.quantity, remaining)
        holding_days = (as_of - pd.Timestamp(lot.acquisition_date)).days
        gain = (float(proceeds_per_share) - lot.basis_per_share) * take
        realized.append(
            {
                "date": date,
                "ticker": ticker,
                "quantity": take,
                "basis_per_share": lot.basis_per_share,
                "proceeds_per_share": proceeds_per_share,
                "gain": gain,
                "term": "LT" if holding_days > 365 else "ST",
                "holding_days": holding_days,
                "acquisition_date": lot.acquisition_date,
                "exit_reason": reason,
            }
        )
        lot.quantity -= take
        remaining -= take
    lots[:] = [lot for lot in lots if lot.quantity > 1e-9]
    return realized


def _eligible_theme_candidates(
    theme: str,
    tickers: Sequence[str],
    date: pd.Timestamp,
    frames: dict[str, pd.DataFrame],
    active: set[str],
    lots: list[ccel.CCELLot],
    cfg: ThematicConvexitySleeveConfig,
    no_rebuy_until: dict[str, pd.Timestamp],
) -> list[str]:
    candidates = [ticker for ticker in (str(item).upper() for item in tickers) if ticker in active and ticker in frames]
    scored: list[tuple[float, str]] = []
    momentums = {ticker: ccel._momentum_12_1(ticker, date, frames) for ticker in candidates}
    valid_scores = [score for score in momentums.values() if score is not None]
    for ticker in candidates:
        if cfg.require_theme_membership and ticker not in tickers:
            continue
        if pd.Timestamp(date) <= no_rebuy_until.get(ticker, pd.Timestamp.min):
            continue
        if _held_quantity(lots, ticker) > 0:
            continue
        if _listing_days(ticker, date, frames) < cfg.min_listing_days:
            continue
        if _dollar_adv(ticker, date, frames) < cfg.min_dollar_adv:
            continue
        if cfg.quality_gate == "pass" and ccel._quality_fails(ticker, date, frames):
            continue
        score = momentums.get(ticker)
        if valid_scores:
            percentile = _percentile_rank(score, valid_scores) if score is not None else 0.0
            if percentile < cfg.momentum_12_1_min_percentile:
                continue
        elif cfg.momentum_12_1_min_percentile > 0:
            continue
        scored.append((-(score if score is not None else 0.0), ticker))
    scored.sort()
    return [ticker for _score, ticker in scored]


def _update_high_water_and_stop(
    ticker: str,
    price: float,
    lots: list[ccel.CCELLot],
    high_water: dict[str, float],
    stop_prices: dict[str, float],
    cfg: ThematicConvexitySleeveConfig,
) -> None:
    basis = _weighted_basis_for_ticker(lots, ticker)
    if basis <= 0 or price <= 0:
        return
    high = max(float(high_water.get(ticker, basis)), price)
    high_water[ticker] = high
    gain_pct = high / basis - 1.0
    giveback = cfg.tightened_giveback_pct if gain_pct >= cfg.tighten_after_gain_pct / 100.0 else cfg.initial_giveback_pct
    synthetic_atr = max(0.01, high * giveback / 100.0 / 2.0)
    candidate = trailing_stop_level(
        entry_price=basis,
        current_price=high,
        atr_14=synthetic_atr,
        existing_stop=stop_prices.get(ticker),
        atr_multiplier=2.0,
        activation_atr=0.0,
    )
    if candidate is None:
        candidate = high * (1.0 - giveback / 100.0)
    stop_prices[ticker] = float(candidate)


def _thesis_break(
    ticker: str,
    theme: str | None,
    date: pd.Timestamp,
    frames: dict[str, pd.DataFrame],
    cfg: ThematicConvexitySleeveConfig,
) -> bool:
    if cfg.thesis_break_quality_gate_fails and ccel._quality_fails(ticker, date, frames):
        return True
    if cfg.thesis_break_theme_invalidation_flag and theme and bool(dict(cfg.theme_invalidation_flags).get(theme)):
        return True
    return False


def _significant_short_gain(
    ticker: str,
    price: float,
    date: pd.Timestamp,
    lots: list[ccel.CCELLot],
    cfg: ThematicConvexitySleeveConfig,
) -> bool:
    basis = _weighted_basis_for_ticker(lots, ticker)
    if basis <= 0:
        return False
    gain_pct = price / basis - 1.0
    if gain_pct < cfg.promote_to_core_at_pct / 100.0:
        return False
    oldest = min((pd.Timestamp(lot.acquisition_date) for lot in lots if lot.ticker == ticker), default=None)
    return oldest is not None and (pd.Timestamp(date) - oldest).days < cfg.significant_gain_min_hold_days


def _bottom_momentum(ticker: str, date: pd.Timestamp, frames: dict[str, pd.DataFrame], active: set[str], quantile: float) -> bool:
    scored: list[tuple[str, float]] = []
    for name in active:
        score = ccel._momentum_12_1(name, date, frames)
        if score is not None:
            scored.append((name, score))
    if len(scored) < 4:
        return False
    scored.sort(key=lambda item: item[1])
    cutoff = max(1, math.ceil(len(scored) * max(0.0, min(1.0, quantile))))
    return ticker in {name for name, _score in scored[:cutoff]}


def _reference_arms(
    *,
    raw_frames: dict[str, pd.DataFrame],
    benchmarks: dict[str, pd.DataFrame],
    start: str,
    end: str,
    oos_start: str,
    windows: list[StressWindow],
    benchmark_curve: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    core_cfg = ccel.CCELConfig(oos_start=oos_start, core_only=True, probation_enabled=False, harvest_enabled=False)
    out["Core_book"] = ccel.run_ccel_backtest(raw_frames, core_cfg, benchmark_curve=benchmark_curve, windows=windows)
    refs = ccel._run_reference_arms(
        raw_frames=raw_frames,
        benchmarks=benchmarks,
        start=start,
        end=end,
        oos_start=oos_start,
        windows=windows,
        benchmark_curve=benchmark_curve,
        regime_enricher=None,
    )
    for arm in ("L1", "QQQ_buy_hold", "SPY_buy_hold"):
        if arm in refs:
            out[arm] = refs[arm]
    return out


def _overlay_arms(
    *,
    results: dict[str, dict[str, Any]],
    benchmark_curve: pd.DataFrame,
    windows: list[StressWindow],
) -> dict[str, dict[str, Any]]:
    core = results.get("Core_book")
    if not core:
        return {}
    overlays: dict[str, dict[str, Any]] = {}
    for source, arm in [
        ("TCS_full", "TCS_overlay_full"),
        ("TCS_winners_capped", "TCS_overlay_winners_capped"),
        ("TCS_momentum_decay_off", "TCS_overlay_momentum_decay_off"),
    ]:
        payload = results.get(source)
        if payload:
            overlays[arm] = _build_overlay_payload(
                arm=arm,
                source_arm=source,
                sleeve_payload=payload,
                core_payload=core,
                benchmark_curve=benchmark_curve,
                windows=windows,
            )
    return overlays


def _build_overlay_payload(
    *,
    arm: str,
    source_arm: str,
    sleeve_payload: dict[str, Any],
    core_payload: dict[str, Any],
    benchmark_curve: pd.DataFrame,
    windows: list[StressWindow],
) -> dict[str, Any]:
    sleeve_config = dict(sleeve_payload.get("config") or {})
    starting_cash = float(sleeve_config.get("starting_cash") or 100_000.0)
    sleeve_cap = float(sleeve_config.get("sleeve_max_pct_of_portfolio") or 20.0) / 100.0
    core_weight = max(0.0, min(1.0, 1.0 - sleeve_cap))
    idle_cash_replaced_by_core = starting_cash * core_weight
    after_tax_curve = _combine_overlay_curves(
        core_payload.get("after_tax_equity_curve") or core_payload.get("equity_curve") or [],
        sleeve_payload.get("after_tax_equity_curve") or sleeve_payload.get("equity_curve") or [],
        core_weight=core_weight,
        idle_cash_replaced_by_core=idle_cash_replaced_by_core,
    )
    pre_tax_curve = _combine_overlay_curves(
        core_payload.get("equity_curve") or [],
        sleeve_payload.get("equity_curve") or [],
        core_weight=core_weight,
        idle_cash_replaced_by_core=idle_cash_replaced_by_core,
    )
    trades = list(core_payload.get("trades") or []) + list(sleeve_payload.get("trades") or [])
    metrics = ccel._metrics(after_tax_curve, trades, benchmark_curve=benchmark_curve)
    pre_tax_metrics = ccel._metrics(pre_tax_curve, trades, benchmark_curve=benchmark_curve)
    sleeve_metrics = dict(sleeve_payload.get("metrics") or {})
    core_metrics = dict(core_payload.get("metrics") or {})
    metrics["after_tax_terminal_wealth"] = after_tax_curve[-1]["equity"] if after_tax_curve else None
    metrics["pre_tax_terminal_wealth"] = pre_tax_curve[-1]["equity"] if pre_tax_curve else None
    metrics["total_costs_paid"] = (
        core_weight * float(core_metrics.get("total_costs_paid") or 0.0)
        + float(sleeve_metrics.get("total_costs_paid") or 0.0)
    )
    metrics["total_turnover"] = (
        core_weight * float(core_metrics.get("total_turnover") or 0.0)
        + float(sleeve_metrics.get("total_turnover") or 0.0)
    )
    metrics["annualized_turnover"] = ccel._annualized_turnover(float(metrics["total_turnover"]), len(after_tax_curve))
    metrics["trade_count"] = int(core_metrics.get("trade_count") or 0) + int(sleeve_metrics.get("trade_count") or 0)
    metrics["terminal_tax_liability"] = (
        core_weight * float(core_metrics.get("terminal_tax_liability") or 0.0)
        + float(sleeve_metrics.get("terminal_tax_liability") or 0.0)
    )
    metrics["taxes_paid"] = (
        core_weight * float(core_metrics.get("taxes_paid") or 0.0)
        + float(sleeve_metrics.get("taxes_paid") or 0.0)
    )
    oos_start = pd.Timestamp(sleeve_config.get("oos_start")) if sleeve_config.get("oos_start") else None
    return {
        "schema": "regime_thematic_sleeve_overlay.v1",
        "proxy_label": TCS_PROXY_LABEL,
        "config": {
            "source_arm": source_arm,
            "core_arm": "Core_book",
            "core_weight": core_weight,
            "sleeve_cap_pct": sleeve_cap * 100.0,
            "idle_cash_replaced_by_core": idle_cash_replaced_by_core,
            "overlay_note": "Derived as core_weight * Core_book plus the TCS standalone account after removing idle cash that should have been allocated to core.",
        },
        "strategy_spec": {"name": arm, "research_only": True, "overlay": True},
        "strategy_hash": ccel._stable_hash({"arm": arm, "source": source_arm, "core_weight": core_weight}),
        "git_sha": _git_sha(),
        "metrics": _json_safe(metrics),
        "pre_tax_metrics": _json_safe(pre_tax_metrics),
        "in_sample": _json_safe(ccel._segment_metrics(after_tax_curve, trades, benchmark_curve, None, oos_start)),
        "out_of_sample": _json_safe(ccel._segment_metrics(after_tax_curve, trades, benchmark_curve, oos_start, None) if oos_start is not None else None),
        "equity_curve": _json_safe(pre_tax_curve),
        "after_tax_equity_curve": _json_safe(after_tax_curve),
        "trades": _json_safe(trades),
        "stress_windows": _json_safe(_stress_results_for_curve(pd.DataFrame(after_tax_curve), trades, benchmark_curve, windows)),
        "component_arms": {"core": "Core_book", "sleeve": source_arm},
        "component_metrics": {
            "core": _json_safe(core_metrics),
            "sleeve": _json_safe(sleeve_metrics),
        },
        "convexity": sleeve_payload.get("convexity") or {},
        "per_name_pnl": sleeve_payload.get("per_name_pnl") or [],
        "production_defaults_changed": False,
    }


def _combine_overlay_curves(
    core_curve: list[dict[str, Any]],
    sleeve_curve: list[dict[str, Any]],
    *,
    core_weight: float,
    idle_cash_replaced_by_core: float,
) -> list[dict[str, Any]]:
    core = pd.DataFrame(core_curve)
    sleeve = pd.DataFrame(sleeve_curve)
    if core.empty or sleeve.empty:
        return []
    core["date"] = pd.to_datetime(core["date"])
    sleeve["date"] = pd.to_datetime(sleeve["date"])
    core = core.set_index("date").sort_index()
    sleeve = sleeve.set_index("date").sort_index()
    aligned = pd.concat(
        [
            pd.to_numeric(core["equity"], errors="coerce").rename("core_equity"),
            pd.to_numeric(sleeve["equity"], errors="coerce").rename("sleeve_equity"),
            pd.to_numeric(core.get("exposure", pd.Series(index=core.index, dtype=float)), errors="coerce").rename("core_exposure"),
            pd.to_numeric(sleeve.get("exposure", pd.Series(index=sleeve.index, dtype=float)), errors="coerce").rename("sleeve_exposure"),
        ],
        axis=1,
    ).sort_index().ffill().dropna(subset=["core_equity", "sleeve_equity"])
    rows: list[dict[str, Any]] = []
    for date, row in aligned.iterrows():
        core_component = core_weight * float(row["core_equity"])
        sleeve_component = float(row["sleeve_equity"]) - float(idle_cash_replaced_by_core)
        equity = core_component + sleeve_component
        sleeve_exposure = float(row.get("sleeve_exposure") or 0.0)
        core_exposure = float(row.get("core_exposure") or 0.0)
        rows.append(
            {
                "date": ccel._date_text(date),
                "equity": equity,
                "core_component": core_component,
                "sleeve_component": sleeve_component,
                "cash": None,
                "position_value": None,
                "exposure": (
                    (core_component * core_exposure + max(0.0, sleeve_component) * sleeve_exposure) / equity
                    if equity > 0
                    else 0.0
                ),
                "costs_paid": 0.0,
                "turnover": 0.0,
            }
        )
    return rows


def render_thematic_sleeve_report(
    *,
    campaign_dir: str | Path = DEFAULT_TCS_CAMPAIGN_DIR,
    output_dir: str | Path = DEFAULT_TCS_REPORT_DIR,
    output_path: str | Path | None = None,
) -> Path:
    root = Path(campaign_dir)
    summary = dict(_read_json(root / "summary.json"))
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    assets_dir = out_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    results = _load_results(summary)
    charts = _build_tcs_charts(summary, results, assets_dir)
    report_path = Path(output_path) if output_path is not None else out_dir / "management_report.html"
    report_path.write_text(_tcs_report_html(summary, charts, results), encoding="utf-8")
    return report_path


def thematic_sleeve_campaign_status(campaign_dir: str | Path = DEFAULT_TCS_CAMPAIGN_DIR) -> dict[str, Any]:
    root = Path(campaign_dir)
    return {
        "campaign_dir": str(root),
        "summary_exists": (root / "summary.json").exists(),
        "cache_manifest_exists": (root / "cache_manifest.json").exists(),
        "result_count": len(list((root / "results").glob("*.json"))) if (root / "results").exists() else 0,
    }


def thematic_sleeve_verdict(rows: list[dict[str, Any]], results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    by_arm = {str(row.get("arm")): row for row in rows}
    tcs = by_arm.get("TCS_overlay_full") or by_arm.get("TCS_full") or {}
    core = by_arm.get("Core_book") or {}
    l1 = by_arm.get("L1") or {}
    qqq = by_arm.get("QQQ_buy_hold") or {}
    spy = by_arm.get("SPY_buy_hold") or {}
    convexity = dict((results.get("TCS_full") or {}).get("convexity") or {})
    right_skew_ok = bool(convexity.get("right_skew_ok"))
    primary_arm = str(tcs.get("arm") or "TCS_full")
    tcs_terminal = _float(tcs.get("after_tax_terminal_wealth"))
    core_terminal = _float(core.get("after_tax_terminal_wealth"))
    tcs_calmar = _float(tcs.get("calmar_ratio"))
    core_calmar = _float(core.get("calmar_ratio"))
    if tcs_terminal is not None and core_terminal is not None:
        has_core_comparison = True
        terminal_delta_vs_core = tcs_terminal - core_terminal
        terminal_ok = tcs_terminal >= core_terminal
    else:
        has_core_comparison = False
        terminal_delta_vs_core = None
        terminal_ok = False
    calmar_ok = bool(tcs_calmar is not None and core_calmar is not None and tcs_calmar >= core_calmar)
    if not has_core_comparison:
        gate_status = "insufficient_data"
        next_step = "Regenerate the campaign with both Core+TCS overlay and Core-alone results before reading the kill-switch."
    elif terminal_ok:
        gate_status = "inconclusive_not_certified"
        next_step = (
            "Do not certify TCS. Core+TCS did not fail the Core kill-switch, but the static-theme proxy remains "
            "survivorship-biased; v2 funding requires a judgment call and point-in-time thematic data."
        )
    else:
        gate_status = "kill_switch_fail"
        next_step = (
            "Do not fund TCS v2. Core+TCS underperformed Core-alone even on the favorable biased basket, "
            "so the sleeve diluted a good core book."
        )

    def _terminal_delta(row: dict[str, Any]) -> float | None:
        row_terminal = _float(row.get("after_tax_terminal_wealth"))
        if tcs_terminal is None or row_terminal is None:
            return None
        return tcs_terminal - row_terminal

    return {
        "gate_status": gate_status,
        "primary_arm": primary_arm,
        "right_skew_convexity_ok": right_skew_ok,
        "terminal_wealth_ok": terminal_ok,
        "overlay_beats_core_terminal": terminal_ok,
        "overlay_after_tax_terminal_wealth": tcs_terminal,
        "core_after_tax_terminal_wealth": core_terminal,
        "overlay_after_tax_terminal_delta_vs_core": terminal_delta_vs_core,
        "risk_adjusted_ok": calmar_ok,
        "overlay_calmar_delta_vs_core": (tcs_calmar - core_calmar) if tcs_calmar is not None and core_calmar is not None else None,
        "benchmark_context": {
            "L1_delta": _terminal_delta(l1),
            "QQQ_buy_hold_delta": _terminal_delta(qqq),
            "SPY_buy_hold_delta": _terminal_delta(spy),
        },
        "production_defaults_changed": False,
        "recommended_next_step": next_step,
        "proxy_caveat": "Core+TCS >= Core is inconclusive because this static-theme proxy is survivorship-biased; Core+TCS < Core is a kill-switch fail.",
        "promotion_requires": "OOS right-skew on a survivorship-free, point-in-time thematic universe after tax/costs, beating the core book and passive indexes.",
    }


def thematic_sleeve_limitations(*, start: str, end: str, load_errors: dict[str, str]) -> list[str]:
    limitations = _limitations(start=start, end=end, load_errors=load_errors)
    limitations.extend(
        [
            TCS_PROXY_LABEL,
            "Theme membership is a static sub-basket over current campaign constituents, so survivorship and hindsight bias can inflate results.",
            "The run can disprove TCS cheaply, but it cannot certify edge without point-in-time thematic membership and delisted-name coverage.",
            "Tax-aware oversize trims use offline loss/LTCG-aware lot ordering; production tax-lot routing is not invoked because this is an offline research backtest.",
            "Single-name harvesting is limited to rule exits: oversize trims and momentum-decay loss relegation.",
        ]
    )
    return limitations


def _tcs_adjusted_price_note(data_source: str) -> str:
    if data_source == "sharadar":
        return "Sharadar SEP adjusted daily OHLC is used with point-in-time SF1 quality gates where available."
    return "Adjusted daily OHLC is used where available; this is a static-theme, survivorship-biased proxy."


def _tcs_row(arm: str, payload: dict[str, Any], result_path: Path) -> dict[str, Any]:
    row = _campaign_row(arm, payload)
    metrics = dict(payload.get("metrics") or {})
    row.update(
        {
            "arm": arm,
            "after_tax_terminal_wealth": metrics.get("after_tax_terminal_wealth"),
            "pre_tax_terminal_wealth": metrics.get("pre_tax_terminal_wealth"),
            "terminal_tax_liability": metrics.get("terminal_tax_liability"),
            "taxes_paid": metrics.get("taxes_paid"),
            "ulcer_index": metrics.get("ulcer_index"),
            "sortino_ratio": metrics.get("sortino_ratio"),
            "trade_count": metrics.get("trade_count"),
            "result_path": str(result_path),
            "proxy_label": payload.get("proxy_label") or TCS_PROXY_LABEL,
            "stress_windows": payload.get("stress_windows") or [],
            "convexity": payload.get("convexity") or {},
        }
    )
    return row


def _build_tcs_charts(summary: dict[str, Any], results: dict[str, dict[str, Any]], assets_dir: Path) -> dict[str, str]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = [row for row in summary.get("rows") or [] if isinstance(row, dict)]
    charts: dict[str, str] = {}

    def save(fig: Any, name: str) -> None:
        path = assets_dir / f"{name}.png"
        fig.tight_layout()
        fig.savefig(path, dpi=160, bbox_inches="tight")
        plt.close(fig)
        charts[name] = str(path)

    ordered = [row for row in rows if str(row.get("arm")) in _report_arm_order()]
    labels = [_arm_label(str(row.get("arm"))) for row in ordered]
    if ordered:
        fig, ax = plt.subplots(figsize=(10.5, 4.4))
        ax.bar(labels, [float(row.get("total_return") or 0.0) * 100 for row in ordered], color="#2563eb")
        ax.set_ylabel("After-tax total return (%)")
        ax.set_title("TCS after-tax total return")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "tcs_total_return")

        fig, ax = plt.subplots(figsize=(10.5, 4.4))
        ax.bar(labels, [float(row.get("max_drawdown") or 0.0) * 100 for row in ordered], color="#dc2626")
        ax.set_ylabel("Max drawdown (%)")
        ax.set_title("After-tax max drawdown")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "tcs_drawdown")

    fig, ax = plt.subplots(figsize=(11.0, 5.2))
    for arm in _report_arm_order():
        payload = results.get(arm)
        if not payload:
            continue
        curve = pd.DataFrame(payload.get("after_tax_equity_curve") or payload.get("equity_curve") or [])
        if curve.empty:
            continue
        curve.index = pd.to_datetime(curve["date"])
        equity = pd.to_numeric(curve["equity"], errors="coerce").dropna()
        if not equity.empty:
            ax.plot(equity.index, equity / float(equity.iloc[0]), label=_arm_label(arm), linewidth=1.4)
    ax.set_title("After-tax growth of $1")
    ax.set_ylabel("Multiple")
    ax.legend(loc="best", fontsize=8)
    save(fig, "tcs_equity_curves")

    per_name = (results.get("TCS_full") or {}).get("per_name_pnl") or []
    if per_name:
        frame = pd.DataFrame(per_name).sort_values("pnl", ascending=False)
        fig, ax = plt.subplots(figsize=(10.5, 4.4))
        ax.bar(frame["ticker"], frame["pnl"], color=["#059669" if value >= 0 else "#dc2626" for value in frame["pnl"]])
        ax.set_ylabel("P&L ($)")
        ax.set_title("TCS full per-name P&L distribution")
        ax.tick_params(axis="x", rotation=45)
        save(fig, "tcs_per_name_pnl")
    return charts


def _tcs_report_html(summary: dict[str, Any], charts: dict[str, str], results: dict[str, dict[str, Any]]) -> str:
    rows = [row for row in summary.get("rows") or [] if isinstance(row, dict)]
    verdict = dict(summary.get("verdict") or {})
    title = f"Thematic Convexity Sleeve Proxy {pd.Timestamp(summary.get('start')).year}-{pd.Timestamp(summary.get('end')).year}"
    chart_tags = "\n".join(
        f'<figure><img src="assets/{html.escape(Path(path).name)}" alt="{html.escape(name)}"><figcaption>{html.escape(name.replace("_", " ").title())}</figcaption></figure>'
        for name, path in charts.items()
    )
    overlay_ok = bool(verdict.get("overlay_beats_core_terminal"))
    overlay_delta = _fmt_money(verdict.get("overlay_after_tax_terminal_delta_vs_core"))
    overlay_text = f"{'>= Core' if overlay_ok else '< Core'} ({overlay_delta})" if overlay_delta else "n/a"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #111827; margin: 28px; line-height: 1.45; }}
    .banner {{ max-width: 1180px; border: 1px solid #f59e0b; background: #fffbeb; border-radius: 8px; padding: 12px 14px; }}
    .summary {{ display: grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 12px; margin: 16px 0 22px; max-width: 1120px; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 8px; padding: 12px 14px; background: #fff; }}
    .label {{ color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: 0; }}
    .value {{ font-size: 22px; font-weight: 700; }}
    .pass {{ color: #047857; }}
    .fail {{ color: #b91c1c; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 1180px; margin: 12px 0 24px; font-size: 13px; }}
    th, td {{ border: 1px solid #e5e7eb; padding: 7px 9px; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child, td.text, th.text {{ text-align: left; }}
    th {{ background: #f3f4f6; }}
    figure {{ margin: 18px 0; max-width: 1080px; }}
    img {{ max-width: 100%; border: 1px solid #e5e7eb; border-radius: 6px; }}
    figcaption {{ color: #4b5563; font-size: 12px; }}
    .note {{ max-width: 1120px; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px 14px; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <p>Generated {html.escape(str(summary.get("generated_at")))} at git SHA <code>{html.escape(str(summary.get("git_sha")))}</code>. Window: <code>{html.escape(str(summary.get("start")))}</code> to <code>{html.escape(str(summary.get("end")))}</code>. OOS starts <code>{html.escape(str(summary.get("oos_start")))}</code>.</p>
  <div class="banner"><strong>{html.escape(TCS_PROXY_LABEL)}.</strong> A pass cannot certify edge; a fail is a kill-switch.</div>
  <section class="summary">
    <div class="card"><div class="label">Gate status</div><div class="value {'pass' if overlay_ok else 'fail'}">{html.escape(str(verdict.get("gate_status")))}</div></div>
    <div class="card"><div class="label">Overlay vs Core</div><div class="value {'pass' if overlay_ok else 'fail'}">{html.escape(overlay_text)}</div></div>
    <div class="card"><div class="label">Right skew</div><div class="value {'pass' if verdict.get("right_skew_convexity_ok") else 'fail'}">{'Pass' if verdict.get("right_skew_convexity_ok") else 'Fail'}</div></div>
    <div class="card"><div class="label">Production defaults changed</div><div class="value fail">No</div></div>
  </section>
  <div class="note"><strong>Executive readout.</strong> {html.escape(str(verdict.get("recommended_next_step") or ""))}</div>
  <h2>Strategy Arms</h2>
  {_html_table(["Arm", "Strategy Used", "Tax / Exit Rule"], _strategy_rows(results), text_columns={0, 1, 2})}
  <h2>After-tax Results</h2>
  {_html_table(["Arm", "Terminal Wealth", "Total Return", "CAGR", "Vol", "Sharpe", "Sortino", "Max DD", "Calmar", "Ulcer", "Turnover", "Costs", "Trades"], _result_rows(rows))}
  <h2>Convexity Checks</h2>
  {_html_table(["Arm", "Skew", "Median Return", "Top 20% Positive P&L Share", "Right-skew OK"], _convexity_rows(rows))}
  <h2>Ablations</h2>
  {_html_table(["Comparison", "Terminal Wealth Delta", "CAGR Delta", "Ulcer Delta", "Interpretation"], _ablation_rows(rows), text_columns={0, 4})}
  <h2>Performance By Year</h2>
  {_html_table(["Year", *[_arm_label(arm) for arm in _report_arm_order()]], _yearly_rows(summary))}
  <h2>Charts</h2>
  {chart_tags}
  <h2>Per-name P&L Distribution</h2>
  {_html_table(["Ticker", "Theme", "Deployed", "Realized", "Open", "P&L", "Total Return"], _per_name_rows(results.get("TCS_full") or {}))}
  <h2>Coverage</h2>
  <p>{html.escape(str(summary.get("adjusted_price_note") or ""))}</p>
  {_html_table(["Ticker", "Sector", "First Date", "Last Date", "Rows", "Starts Late"], _coverage_rows(summary))}
  <h2>Limitations</h2>
  <ul>{''.join(f"<li>{html.escape(str(item))}</li>" for item in summary.get("limitations") or [])}</ul>
  <h2>Reproduction</h2>
  <pre>{html.escape(str(summary.get("single_command") or ""))}</pre>
</body>
</html>
"""


def _load_results(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for row in summary.get("rows") or []:
        if not isinstance(row, dict):
            continue
        arm = str(row.get("arm") or "")
        path = Path(str(row.get("result_path") or ""))
        if arm and path.exists():
            results[arm] = dict(_read_json(path))
    return results


def _per_name_pnl(
    trades: list[dict[str, Any]],
    lots: list[ccel.CCELLot],
    prices: dict[str, float],
    deployed_capital: dict[str, float],
) -> list[dict[str, Any]]:
    by_ticker: dict[str, dict[str, float]] = {}
    for trade in trades:
        ticker = str(trade.get("ticker") or "").upper()
        if not ticker:
            continue
        row = by_ticker.setdefault(ticker, {"deployed_capital": 0.0, "realized_pnl": 0.0, "costs_paid": 0.0})
        row["costs_paid"] += float(trade.get("costs_paid") or 0.0)
        if str(trade.get("side") or "").lower() == "buy":
            row["deployed_capital"] += float(trade.get("notional") or 0.0) + float(trade.get("costs_paid") or 0.0)
        elif str(trade.get("side") or "").lower() == "sell":
            row["realized_pnl"] += float(trade.get("net_pnl") or 0.0)
    for ticker, deployed in deployed_capital.items():
        by_ticker.setdefault(ticker, {"deployed_capital": 0.0, "realized_pnl": 0.0, "costs_paid": 0.0})
        by_ticker[ticker]["deployed_capital"] = max(float(by_ticker[ticker].get("deployed_capital") or 0.0), float(deployed))
    for lot in lots:
        row = by_ticker.setdefault(lot.ticker, {"deployed_capital": 0.0, "realized_pnl": 0.0, "costs_paid": 0.0})
        price = float(prices.get(lot.ticker, 0.0))
        row["open_pnl"] = row.get("open_pnl", 0.0) + (price - lot.basis_per_share) * lot.quantity
    out: list[dict[str, Any]] = []
    theme_map = _ticker_theme_map(ThematicConvexitySleeveConfig())
    for ticker, row in sorted(by_ticker.items()):
        deployed = float(row.get("deployed_capital") or 0.0)
        realized = float(row.get("realized_pnl") or 0.0)
        open_pnl = float(row.get("open_pnl") or 0.0)
        pnl = realized + open_pnl
        out.append(
            {
                "ticker": ticker,
                "theme": theme_map.get(ticker),
                "deployed_capital": deployed,
                "realized_pnl": realized,
                "open_pnl": open_pnl,
                "pnl": pnl,
                "total_return": pnl / deployed if deployed > 0 else None,
            }
        )
    return out


def _convexity_stats(per_name: list[dict[str, Any]]) -> dict[str, Any]:
    returns = pd.Series([_float(row.get("total_return")) for row in per_name if _float(row.get("total_return")) is not None], dtype=float)
    pnls = [float(row.get("pnl") or 0.0) for row in per_name]
    positive = sorted([value for value in pnls if value > 0.0], reverse=True)
    top_n = max(1, math.ceil(len(positive) * 0.20)) if positive else 0
    top_share = sum(positive[:top_n]) / sum(positive) if positive and sum(positive) > 0 else 0.0
    median = float(returns.median()) if not returns.empty else None
    skew = float(returns.skew()) if len(returns) >= 3 else None
    ok = median is not None and median <= 0.05 and top_share >= 0.60 and skew is not None and skew > 0.5
    return {
        "median_per_name_return": median,
        "top_20_positive_pnl_share": top_share,
        "skew": skew,
        "right_skew_ok": ok,
    }


def _ticker_theme_map(cfg: ThematicConvexitySleeveConfig) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for theme, tickers in cfg.active_themes.items():
        for ticker in tickers:
            mapping.setdefault(str(ticker).upper(), theme)
    return mapping


def _listing_days(ticker: str, date: pd.Timestamp, frames: dict[str, pd.DataFrame]) -> int:
    frame = frames.get(ticker)
    return 0 if frame is None else int((frame.index <= pd.Timestamp(date)).sum())


def _dollar_adv(ticker: str, date: pd.Timestamp, frames: dict[str, pd.DataFrame]) -> float:
    frame = frames.get(ticker)
    if frame is None or frame.empty or "volume" not in frame:
        return 0.0
    rows = frame.loc[frame.index <= pd.Timestamp(date)].tail(20)
    if rows.empty:
        return 0.0
    return float((pd.to_numeric(rows["price"], errors="coerce") * pd.to_numeric(rows["volume"], errors="coerce")).mean())


def _percentile_rank(value: float | None, scores: Sequence[float]) -> float:
    if value is None or not scores:
        return 0.0
    below = sum(1 for score in scores if score <= value)
    return 100.0 * below / len(scores)


def _held_quantity(lots: list[ccel.CCELLot], ticker: str) -> float:
    return float(sum(lot.quantity for lot in lots if lot.ticker == ticker))


def _weighted_basis_for_ticker(lots: list[ccel.CCELLot], ticker: str) -> float:
    ticker_lots = [lot for lot in lots if lot.ticker == ticker and lot.quantity > 0]
    if not ticker_lots:
        return 0.0
    total = sum(lot.quantity for lot in ticker_lots)
    return sum(lot.quantity * lot.basis_per_share for lot in ticker_lots) / total if total > 0 else 0.0


def _sleeve_value(lots: list[ccel.CCELLot], prices: dict[str, float], promoted_core: set[str]) -> float:
    return float(sum(lot.quantity * float(prices.get(lot.ticker, 0.0)) for lot in lots if lot.ticker not in promoted_core))


def _core_value(lots: list[ccel.CCELLot], prices: dict[str, float], promoted_core: set[str]) -> float:
    return float(sum(lot.quantity * float(prices.get(lot.ticker, 0.0)) for lot in lots if lot.ticker in promoted_core))


def _theme_value(
    theme: str,
    lots: list[ccel.CCELLot],
    prices: dict[str, float],
    ticker_theme: dict[str, str],
    promoted_core: set[str],
) -> float:
    return float(
        sum(
            lot.quantity * float(prices.get(lot.ticker, 0.0))
            for lot in lots
            if ticker_theme.get(lot.ticker) == theme and lot.ticker not in promoted_core
        )
    )


def _theme_sleeve_count(theme: str, lots: list[ccel.CCELLot], ticker_theme: dict[str, str], promoted_core: set[str]) -> int:
    return sum(1 for ticker in ccel._held_tickers(lots) if ticker_theme.get(ticker) == theme and ticker not in promoted_core)


def _audit_from_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for trade in trades:
        out.append(
            {
                "date": trade.get("date"),
                "ticker": trade.get("ticker"),
                "action": str(trade.get("side") or "").lower(),
                "reason": trade.get("exit_type"),
                "quantity": trade.get("quantity"),
                "price": trade.get("price"),
            }
        )
    return out


def _campaign_metadata(*, arm: str, start: str, end: str, oos_start: str, kind: str) -> dict[str, Any]:
    return {
        "campaign": "thematic_convexity_sleeve",
        "arm": arm,
        "kind": kind,
        "git_sha": _git_sha(),
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "start": start,
        "end": end,
        "oos_start": oos_start,
        "proxy_label": TCS_PROXY_LABEL,
        "research_only": True,
    }


def _row_order(row: dict[str, Any]) -> tuple[int, str]:
    order = {arm: idx for idx, arm in enumerate(_report_arm_order())}
    return (order.get(str(row.get("arm")), 999), str(row.get("arm")))


def _report_arm_order() -> list[str]:
    return [
        "TCS_overlay_full",
        "TCS_full",
        "Core_book",
        "TCS_overlay_winners_capped",
        "TCS_winners_capped",
        "TCS_overlay_momentum_decay_off",
        "TCS_momentum_decay_off",
        "TCS_entry_1pct",
        "TCS_entry_2pct",
        "TCS_cap_15pct",
        "L1",
        "QQQ_buy_hold",
        "SPY_buy_hold",
    ]


def _arm_label(arm: str) -> str:
    labels = {
        "TCS_overlay_full": "TCS overlay",
        "TCS_full": "TCS full",
        "Core_book": "Core book",
        "TCS_overlay_winners_capped": "TCS overlay cap winners",
        "TCS_winners_capped": "TCS cap winners",
        "TCS_overlay_momentum_decay_off": "TCS overlay no momentum exit",
        "TCS_momentum_decay_off": "TCS no momentum exit",
        "TCS_entry_1pct": "TCS 1% entry",
        "TCS_entry_2pct": "TCS 2% entry",
        "TCS_cap_15pct": "TCS 15% cap",
        "L1": "L1",
        "QQQ_buy_hold": "QQQ buy-hold",
        "SPY_buy_hold": "SPY buy-hold",
    }
    return labels.get(arm, arm)


def _strategy_rows(results: dict[str, dict[str, Any]]) -> list[list[str]]:
    descriptions = {
        "TCS_overlay_full": ["80% CCEL core plus TCS sleeve component; idle cash from the sleeve-only run is replaced by core.", "Primary portfolio-shape test."],
        "TCS_full": ["Static-theme convexity sleeve with small tranches and winner graduation.", "Sleeve-only diagnostic account; not the primary terminal-wealth comparison."],
        "Core_book": ["CCEL let-winners-run core book with no sleeve.", "FIFO after-tax benchmark."],
        "TCS_overlay_winners_capped": ["80% core plus capped-winner TCS sensitivity.", "Tests overlay effect of removing winner drift."],
        "TCS_winners_capped": ["TCS sensitivity where winners are capped instead of allowed to run.", "Tests whether convexity depends on no-trim winner drift."],
        "TCS_overlay_momentum_decay_off": ["80% core plus TCS without momentum-decay loss relegation.", "Tests overlay effect of the loss-relegation rule."],
        "TCS_momentum_decay_off": ["TCS sensitivity without momentum-decay loss relegation.", "Tests exit-rule contribution."],
        "TCS_entry_1pct": ["TCS sensitivity with 1% entry tranches.", "Research-only locked-field sensitivity."],
        "TCS_entry_2pct": ["TCS sensitivity with 2% entry tranches.", "Research-only locked-field sensitivity."],
        "TCS_cap_15pct": ["TCS sensitivity with 15% hard cap.", "Research-only locked-field sensitivity."],
        "L1": ["Campaign 2 L1 portfolio strategy.", "After-tax reference."],
        "QQQ_buy_hold": ["QQQ buy-and-hold.", "Passive index after-tax reference."],
        "SPY_buy_hold": ["SPY buy-and-hold.", "Passive index after-tax reference."],
    }
    rows: list[list[str]] = []
    for arm in _report_arm_order():
        if arm not in results:
            continue
        detail = descriptions.get(arm, ["Reference arm.", "After-tax reference."])
        rows.append([_arm_label(arm), detail[0], detail[1]])
    return rows


def _result_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    out: list[list[str]] = []
    for row in rows:
        out.append(
            [
                _arm_label(str(row.get("arm"))),
                _fmt_money(row.get("after_tax_terminal_wealth")),
                _fmt_pct(row.get("total_return")),
                _fmt_pct(row.get("annualized_return")),
                _fmt_pct(row.get("annualized_volatility")),
                _fmt_num(row.get("sharpe_ratio")),
                _fmt_num(row.get("sortino_ratio")),
                _fmt_pct(row.get("max_drawdown")),
                _fmt_num(row.get("calmar_ratio")),
                _fmt_pct(row.get("ulcer_index")),
                _fmt_pct(row.get("annualized_turnover")),
                _fmt_money(row.get("total_costs_paid")),
                str(row.get("trade_count") or ""),
            ]
        )
    return out


def _convexity_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    out: list[list[str]] = []
    for row in rows:
        convexity = dict(row.get("convexity") or {})
        if not convexity:
            continue
        out.append(
            [
                _arm_label(str(row.get("arm"))),
                _fmt_num(convexity.get("skew")),
                _fmt_pct(convexity.get("median_per_name_return")),
                _fmt_pct(convexity.get("top_20_positive_pnl_share")),
                "yes" if convexity.get("right_skew_ok") else "no",
            ]
        )
    return out


def _ablation_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    by_arm = {str(row.get("arm")): row for row in rows}
    base = by_arm.get("TCS_overlay_full") or by_arm.get("TCS_full") or {}
    comparisons = [
        ("Overlay vs core-only", "Core_book", "Measures whether the TCS sleeve adds value over the let-winners-run core."),
        ("Overlay winners run ON vs OFF", "TCS_overlay_winners_capped", "Positive delta means convexity comes from letting winners run."),
        ("Overlay momentum decay ON vs OFF", "TCS_overlay_momentum_decay_off", "Positive delta means loss-relegation helps."),
        ("Entry 1.5% vs 1.0%", "TCS_entry_1pct", "Entry-size sensitivity."),
        ("Entry 1.5% vs 2.0%", "TCS_entry_2pct", "Entry-size sensitivity."),
        ("Cap 20% vs 15%", "TCS_cap_15pct", "Hard-cap sensitivity."),
    ]
    out: list[list[str]] = []
    for label, arm, interpretation in comparisons:
        other = by_arm.get(arm) or {}
        out.append(
            [
                label,
                _fmt_money((_float(base.get("after_tax_terminal_wealth")) or 0.0) - (_float(other.get("after_tax_terminal_wealth")) or 0.0)),
                _fmt_pct((_float(base.get("annualized_return")) or 0.0) - (_float(other.get("annualized_return")) or 0.0)),
                _fmt_pct((_float(base.get("ulcer_index")) or 0.0) - (_float(other.get("ulcer_index")) or 0.0)),
                interpretation,
            ]
        )
    return out


def _yearly_rows(summary: dict[str, Any]) -> list[list[str]]:
    yearly = dict(summary.get("yearly_returns") or {})
    years = sorted({str(row.get("period")) for rows in yearly.values() for row in rows if isinstance(row, dict) and row.get("period")})
    out: list[list[str]] = []
    for year in years:
        row = [year]
        for arm in _report_arm_order():
            values = {str(item.get("period")): item.get("return") for item in yearly.get(arm, []) if isinstance(item, dict)}
            row.append(_fmt_pct(values.get(year)))
        out.append(row)
    return out


def _per_name_rows(payload: dict[str, Any]) -> list[list[str]]:
    rows = sorted([row for row in payload.get("per_name_pnl") or [] if isinstance(row, dict)], key=lambda item: float(item.get("pnl") or 0.0), reverse=True)
    return [
        [
            str(row.get("ticker") or ""),
            str(row.get("theme") or ""),
            _fmt_money(row.get("deployed_capital")),
            _fmt_money(row.get("realized_pnl")),
            _fmt_money(row.get("open_pnl")),
            _fmt_money(row.get("pnl")),
            _fmt_pct(row.get("total_return")),
        ]
        for row in rows
    ]


def _coverage_rows(summary: dict[str, Any]) -> list[list[str]]:
    coverage = dict(summary.get("coverage") or {})
    rows = coverage.get("tickers") or []
    out: list[list[str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            [
                str(row.get("ticker") or ""),
                str(row.get("sector") or ""),
                str(row.get("first_date") or ""),
                str(row.get("last_date") or ""),
                str(row.get("row_count") or ""),
                "yes" if row.get("starts_late") else "no",
            ]
        )
    return out


def _fmt_money(value: Any) -> str:
    parsed = _float(value)
    return "" if parsed is None else f"${parsed:,.0f}"
