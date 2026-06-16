from __future__ import annotations

import datetime as dt
import json
import logging
import subprocess
from dataclasses import asdict, dataclass, is_dataclass, replace
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .data import download_market_frame
from .fundamental_data import fetch_financial_statements
from .hmm_engine import fit_regime_model
from .investor_adapter import get_investor_db_path, get_sector_map
from .meta_labeler import DEFAULT_META_LABELER_MIN_OOF_AUC, MetaLabelerConfig, MetaLabelerEngine
from .pipeline_backtest import PipelineBacktestConfig, PipelineBacktestResult, run_pipeline_backtest
from .stress_windows import stress_windows_payload
from .threshold_sweep import expand_threshold_grid, run_threshold_sweep
from .triple_barrier import DEFAULT_MANAGED_EXIT_CONFIG, build_multi_ticker_managed_frame
from .universe import check_universe_eligibility

logger = logging.getLogger(__name__)

CAMPAIGN_OOS_START = "2024-01-01"
CAMPAIGN_PERIOD = "10y"
CAMPAIGN_MIN_HISTORY_DAYS = 2500
MIN_OOS_TRADES = 100
MIN_TRADED_NAMES = 20
RECOMMENDED_MIN_NAME_WIN_RATE = 0.60
MAX_DRAWDOWN_WORSENING = 1.20

DEFAULT_CAMPAIGN_DIR = Path("data") / "campaign"
DEFAULT_BASKET_PATH = DEFAULT_CAMPAIGN_DIR / "basket.json"
DEFAULT_REPORT_PATH = Path("ALPHA_CAMPAIGN_REPORT.md")

GICS_SECTORS_10: tuple[str, ...] = (
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Utilities",
)

DEFAULT_SECTOR_CANDIDATES: dict[str, tuple[str, ...]] = {
    "Communication Services": ("GOOGL", "GOOG", "META", "NFLX", "TMUS", "DIS", "CMCSA", "T", "VZ", "CHTR"),
    "Consumer Discretionary": ("AMZN", "TSLA", "HD", "MCD", "NKE", "LOW", "SBUX", "BKNG", "TJX", "ORLY"),
    "Consumer Staples": ("WMT", "COST", "PG", "KO", "PEP", "PM", "MDLZ", "CL", "MO", "TGT"),
    "Energy": ("XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "VLO", "OXY", "HAL"),
    "Financials": ("BRK-B", "JPM", "V", "MA", "BAC", "WFC", "GS", "MS", "AXP", "C"),
    "Health Care": ("LLY", "UNH", "JNJ", "ABBV", "MRK", "ABT", "TMO", "DHR", "PFE", "ISRG"),
    "Industrials": ("GE", "CAT", "RTX", "HON", "UNP", "UPS", "ETN", "DE", "BA", "LMT"),
    "Information Technology": ("NVDA", "MSFT", "AAPL", "AVGO", "AMD", "ORCL", "CRM", "ADBE", "QCOM", "AMAT"),
    "Materials": ("LIN", "SHW", "APD", "FCX", "ECL", "NEM", "DD", "DOW", "MLM", "VMC"),
    "Utilities": ("NEE", "SO", "DUK", "CEG", "AEP", "SRE", "D", "EXC", "XEL", "PEG"),
}

SectorLookup = Callable[[list[str]], dict[str, str]]
MarketFrameLoader = Callable[[str], pd.DataFrame]
BacktestRunner = Callable[[str, pd.DataFrame, PipelineBacktestConfig, pd.DataFrame | None], PipelineBacktestResult]


@dataclass(frozen=True)
class BasketCandidate:
    ticker: str
    sector: str
    dollar_adv: float
    price: float | None
    history_days: int | None
    eligible: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _campaign_dir(path: str | Path | None = None) -> Path:
    return Path(path or DEFAULT_CAMPAIGN_DIR)


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(_json_safe(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _json_safe(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_safe(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (pd.Timestamp, dt.datetime, dt.date)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _today_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


_DUAL_CLASS_ISSUER_GROUPS: dict[str, str] = {
    "GOOG": "ALPHABET",
    "GOOGL": "ALPHABET",
    "BRK-A": "BERKSHIRE",
    "BRK-B": "BERKSHIRE",
    "BRK.A": "BERKSHIRE",
    "BRK.B": "BERKSHIRE",
    "FOX": "FOX_CORP",
    "FOXA": "FOX_CORP",
    "NWS": "NEWS_CORP",
    "NWSA": "NEWS_CORP",
    "UA": "UNDER_ARMOUR",
    "UAA": "UNDER_ARMOUR",
    "LEN": "LENNAR",
    "LEN-B": "LENNAR",
    "HEI": "HEICO",
    "HEI-A": "HEICO",
    "LBTYA": "LIBERTY_GLOBAL",
    "LBTYB": "LIBERTY_GLOBAL",
    "LBTYK": "LIBERTY_GLOBAL",
    "PARA": "PARAMOUNT",
    "PARAA": "PARAMOUNT",
    "ZG": "ZILLOW",
    "Z": "ZILLOW",
    "CWEN": "CLEARWAY",
    "CWEN-A": "CLEARWAY",
}


def _issuer_key(ticker: str) -> str:
    """Collapse dual-class share listings to one issuer key.

    A basket holding GOOG and GOOGL holds Alphabet twice — the mechanical
    top-ADV-per-sector rule must not double-count an issuer. Unknown tickers
    map to themselves.
    """
    symbol = str(ticker or "").strip().upper()
    return _DUAL_CLASS_ISSUER_GROUPS.get(symbol, symbol)


def _normalize_sector(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "Unknown"
    lowered = text.lower()
    aliases = {
        "technology": "Information Technology",
        "tech": "Information Technology",
        "information technology": "Information Technology",
        "communication services": "Communication Services",
        "communications": "Communication Services",
        "consumer cyclical": "Consumer Discretionary",
        "consumer discretionary": "Consumer Discretionary",
        "consumer defensive": "Consumer Staples",
        "consumer staples": "Consumer Staples",
        "healthcare": "Health Care",
        "health care": "Health Care",
        "financial services": "Financials",
        "financial": "Financials",
        "financials": "Financials",
        "basic materials": "Materials",
        "materials": "Materials",
        "industrials": "Industrials",
        "industrial": "Industrials",
        "energy": "Energy",
        "utilities": "Utilities",
        "utility": "Utilities",
        "real estate": "Financials",
    }
    return aliases.get(lowered, text)


def default_candidate_universe() -> list[str]:
    seen: set[str] = set()
    tickers: list[str] = []
    for sector in GICS_SECTORS_10:
        for ticker in DEFAULT_SECTOR_CANDIDATES.get(sector, ()):
            normalized = str(ticker).upper()
            if normalized not in seen:
                seen.add(normalized)
                tickers.append(normalized)
    return tickers


def _default_sector_lookup(tickers: list[str]) -> dict[str, str]:
    sector_map: dict[str, str] = {}
    reverse_defaults = {
        ticker.upper(): sector
        for sector, sector_tickers in DEFAULT_SECTOR_CANDIDATES.items()
        for ticker in sector_tickers
    }
    try:
        db_map = get_sector_map(get_investor_db_path(), tickers)
    except Exception:
        db_map = {}
    for ticker in tickers:
        normalized = str(ticker).upper()
        sector = _normalize_sector(db_map.get(normalized) or reverse_defaults.get(normalized))
        if sector == "Unknown":
            try:
                statements = fetch_financial_statements(normalized, use_cache=True)
                sector = _normalize_sector((statements.info or {}).get("sector") or (statements.info or {}).get("industry"))
            except Exception:
                sector = "Unknown"
        sector_map[normalized] = sector
    return sector_map


def _default_market_frame_loader(ticker: str) -> pd.DataFrame:
    return download_market_frame(ticker=ticker, period=CAMPAIGN_PERIOD, interval="1d", cache=True).frame


def _candidate_from_frame(ticker: str, sector: str, frame: pd.DataFrame) -> BasketCandidate:
    eligibility = check_universe_eligibility(ticker, market_frame=frame, asset_class="EQUITY", use_cache=False)
    reasons = list(eligibility.reasons)
    history_days = eligibility.measured_history_days
    if history_days is None or history_days < CAMPAIGN_MIN_HISTORY_DAYS:
        reasons.append("campaign_min_10y_history")
    eligible = bool(eligibility.eligible and (history_days or 0) >= CAMPAIGN_MIN_HISTORY_DAYS)
    return BasketCandidate(
        ticker=ticker.upper(),
        sector=sector,
        dollar_adv=float(eligibility.measured_dollar_adv or 0.0),
        price=eligibility.measured_price,
        history_days=history_days,
        eligible=eligible,
        reasons=sorted(set(reasons)),
    )


def select_basket(
    *,
    output_path: str | Path = DEFAULT_BASKET_PATH,
    candidates: list[str] | None = None,
    sector_lookup: SectorLookup | None = None,
    market_frame_loader: MarketFrameLoader | None = None,
    names_per_sector: int = 3,
) -> dict[str, Any]:
    raw_candidates = candidates or default_candidate_universe()
    tickers = sorted({str(ticker).strip().upper() for ticker in raw_candidates if str(ticker or "").strip()})
    lookup = sector_lookup or _default_sector_lookup
    sectors = lookup(tickers)
    loader = market_frame_loader or _default_market_frame_loader

    screened: list[BasketCandidate] = []
    reason_counts: dict[str, int] = {}
    for ticker in tickers:
        sector = _normalize_sector(sectors.get(ticker))
        try:
            frame = loader(ticker)
            candidate = _candidate_from_frame(ticker, sector, frame)
        except Exception as exc:
            logger.warning("Campaign universe screen failed for %s: %s", ticker, exc)
            candidate = BasketCandidate(ticker=ticker, sector=sector, dollar_adv=0.0, price=None, history_days=None, eligible=False, reasons=["data_error"])
        screened.append(candidate)
        for reason in candidate.reasons:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    selected: list[BasketCandidate] = []
    sector_status: dict[str, Any] = {}
    seen_issuers: set[str] = set()
    for sector in GICS_SECTORS_10:
        pool = [
            item for item in screened
            if item.eligible and _normalize_sector(item.sector) == sector
        ]
        pool.sort(key=lambda item: (-float(item.dollar_adv or 0.0), item.ticker))
        chosen: list[BasketCandidate] = []
        duplicate_issuers: list[str] = []
        for item in pool:
            issuer = _issuer_key(item.ticker)
            if issuer in seen_issuers:
                # Dual-class listings (e.g. GOOG/GOOGL) are the same company;
                # selecting both double-weights one issuer's idiosyncratic risk.
                duplicate_issuers.append(item.ticker)
                continue
            seen_issuers.add(issuer)
            chosen.append(item)
            if len(chosen) >= int(names_per_sector):
                break
        selected.extend(chosen)
        sector_status[sector] = {
            "eligible_count": len(pool),
            "selected_count": len(chosen),
            "selected": [item.ticker for item in chosen],
            "skipped_duplicate_issuers": duplicate_issuers,
        }

    payload = {
        "schema": "regime_alpha_campaign_basket.v1",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "git_sha": _git_sha(),
        "selection_rule": f"top {int(names_per_sector)} dollar-ADV screen-passers per 10 GICS-style sectors, one listing per issuer (dual-class dedupe)",
        "oos_start": CAMPAIGN_OOS_START,
        "period": CAMPAIGN_PERIOD,
        "min_history_days": CAMPAIGN_MIN_HISTORY_DAYS,
        "sectors": list(GICS_SECTORS_10),
        "tickers": [item.ticker for item in selected],
        "basket_size": len(selected),
        "screen_stats": {
            "candidate_count": len(screened),
            "eligible_count": sum(1 for item in screened if item.eligible),
            "selected_count": len(selected),
            "reason_counts": reason_counts,
            "sector_status": sector_status,
        },
        "selected": [item.to_dict() for item in selected],
        "screened": [item.to_dict() for item in screened],
    }
    _write_json(output_path, payload)
    return payload


def load_basket(path: str | Path = DEFAULT_BASKET_PATH) -> dict[str, Any]:
    return dict(_read_json(path))


def subset_tickers_from_basket(basket: dict[str, Any]) -> list[str]:
    by_sector: dict[str, list[dict[str, Any]]] = {}
    for item in basket.get("selected") or []:
        if not isinstance(item, dict):
            continue
        by_sector.setdefault(_normalize_sector(item.get("sector")), []).append(item)
    subset: list[str] = []
    for sector in GICS_SECTORS_10:
        rows = by_sector.get(sector) or []
        rows.sort(key=lambda item: (-float(item.get("dollar_adv") or 0.0), str(item.get("ticker") or "")))
        if rows:
            subset.append(str(rows[0].get("ticker") or "").upper())
    return [ticker for ticker in subset if ticker]


def _phase_path(campaign_dir: str | Path, phase: int | str) -> Path:
    return _campaign_dir(campaign_dir) / f"phase{phase}"


def _phase_dir(campaign_dir: str | Path, phase: int | str) -> Path:
    target = _phase_path(campaign_dir, phase)
    target.mkdir(parents=True, exist_ok=True)
    return target


def _load_frame(ticker: str, *, loader: MarketFrameLoader | None = None) -> pd.DataFrame:
    return (loader or _default_market_frame_loader)(ticker)


def _load_benchmark(loader: MarketFrameLoader | None = None) -> pd.DataFrame:
    return _load_frame("SPY", loader=loader)


def _default_backtest_runner(
    ticker: str,
    market_frame: pd.DataFrame,
    config: PipelineBacktestConfig,
    benchmark_frame: pd.DataFrame | None,
) -> PipelineBacktestResult:
    return run_pipeline_backtest(ticker, market_frame, config=config, benchmark_frame=benchmark_frame)


def _result_payload(result: PipelineBacktestResult, *, config_id: str, phase: str) -> dict[str, Any]:
    payload = result.to_dict()
    payload["campaign"] = {
        "phase": phase,
        "config_id": config_id,
        "git_sha": _git_sha(),
        "data_cache_date": _today_utc(),
    }
    return payload


def _run_backtest_unit(
    *,
    ticker: str,
    config: PipelineBacktestConfig,
    output_path: Path,
    config_id: str,
    phase: str,
    resume: bool,
    frame_loader: MarketFrameLoader | None = None,
    benchmark_frame: pd.DataFrame | None = None,
    backtest_runner: BacktestRunner | None = None,
) -> dict[str, Any]:
    if resume and output_path.exists():
        return dict(_read_json(output_path))
    frame = _load_frame(ticker, loader=frame_loader)
    result = (backtest_runner or _default_backtest_runner)(ticker, frame, config, benchmark_frame)
    payload = _result_payload(result, config_id=config_id, phase=phase)
    _write_json(output_path, payload)
    return payload


def _baseline_config(**updates: Any) -> PipelineBacktestConfig:
    return replace(PipelineBacktestConfig(oos_start=CAMPAIGN_OOS_START), **updates)


def run_phase0(
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_CAMPAIGN_DIR,
    resume: bool = False,
    frame_loader: MarketFrameLoader | None = None,
    backtest_runner: BacktestRunner | None = None,
) -> dict[str, Any]:
    basket = load_basket(basket_path)
    phase_dir = _phase_dir(campaign_dir, 0)
    benchmark = _load_benchmark(frame_loader)
    config = _baseline_config()
    units: list[dict[str, Any]] = []
    for ticker in basket.get("tickers") or []:
        output = phase_dir / f"{str(ticker).upper()}__baseline.json"
        units.append(
            _run_backtest_unit(
                ticker=str(ticker).upper(),
                config=config,
                output_path=output,
                config_id="baseline",
                phase="0",
                resume=resume,
                frame_loader=frame_loader,
                benchmark_frame=benchmark,
                backtest_runner=backtest_runner,
            )
        )
    summary = {
        "phase": 0,
        "config_id": "baseline",
        "git_sha": _git_sha(),
        "basket_size": len(units),
        "aggregate": aggregate_result_payloads(units),
        "units": [f"{str(row.get('ticker')).upper()}__baseline.json" for row in units],
    }
    _write_json(phase_dir / "summary.json", summary)
    return summary


def phase1_grid() -> dict[str, list[Any]]:
    return {
        "use_empirical_durations": [False, True],
        "use_forward_curve_gates": [False, True],
        "strong_buy_min_p_bull_day5": [0.45, 0.55, 0.65],
        "buy_min_p_bull_day5": [0.40, 0.50],
        "neutral_tilt_requires_modal": [False, True],
        "composite_adjustments_enabled": [True, False],
    }


def phase2_configs() -> list[dict[str, Any]]:
    return [
        {"config_id": "hmm_baseline", "hmm_n_seeds": 1, "hmm_covariance_type": "diag", "macro_weight": 1.5},
        {"config_id": "n_seeds=3", "hmm_n_seeds": 3, "hmm_covariance_type": "diag", "macro_weight": 1.5},
        {"config_id": "covariance=full", "hmm_n_seeds": 1, "hmm_covariance_type": "full", "macro_weight": 1.5},
        {"config_id": "macro_weight=1.0", "hmm_n_seeds": 1, "hmm_covariance_type": "diag", "macro_weight": 1.0},
        {"config_id": "macro_weight=1.5", "hmm_n_seeds": 1, "hmm_covariance_type": "diag", "macro_weight": 1.5},
    ]


def _threshold_rows_for_tickers(
    tickers: list[str],
    *,
    frame_loader: MarketFrameLoader | None,
    benchmark_frame: pd.DataFrame | None,
    grid: dict[str, list[Any]],
    base_config: PipelineBacktestConfig,
) -> list[dict[str, Any]]:
    frames = {ticker: _load_frame(ticker, loader=frame_loader) for ticker in tickers}
    return run_threshold_sweep(
        tickers=tickers,
        market_frames=frames,
        benchmark_frame=benchmark_frame,
        grid=grid,
        base_config=base_config,
        include_stress_windows=True,
    )


def _top_configs_from_rows(rows: list[dict[str, Any]], limit: int = 3) -> list[str]:
    aggregate_rows = [row for row in rows if row.get("ticker") == "__AGGREGATE__"]
    rankable = [
        row for row in aggregate_rows
        if _to_float(row.get("oos_trade_count_sum")) is not None
    ]
    rankable.sort(
        key=lambda row: (
            _to_float(row.get("oos_sharpe_ratio_avg")) or -999.0,
            _to_float(row.get("oos_total_return_avg")) or -999.0,
        ),
        reverse=True,
    )
    return [str(row.get("combo_id")) for row in rankable[: int(limit)] if row.get("combo_id")]


def _params_for_combo_ids(rows: list[dict[str, Any]], combo_ids: list[str]) -> list[dict[str, Any]]:
    wanted = set(combo_ids)
    configs: list[dict[str, Any]] = []
    for row in rows:
        if row.get("ticker") == "__AGGREGATE__" and row.get("combo_id") in wanted:
            params = {
                key.replace("param_", "", 1): value
                for key, value in row.items()
                if key.startswith("param_")
            }
            if params:
                configs.append(params)
    return configs


def _threshold_rows_for_exact_combos(
    tickers: list[str],
    *,
    frame_loader: MarketFrameLoader | None,
    benchmark_frame: pd.DataFrame | None,
    combos: list[dict[str, Any]],
    base_config: PipelineBacktestConfig,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for combo in combos:
        rows.extend(
            _threshold_rows_for_tickers(
                tickers,
                frame_loader=frame_loader,
                benchmark_frame=benchmark_frame,
                grid={key: [value] for key, value in combo.items()},
                base_config=base_config,
            )
        )
    return rows


def _combo_artifact_name(combo_id: str) -> str:
    allowed = []
    for char in str(combo_id or "combo"):
        allowed.append(char if char.isalnum() else "_")
    return ("".join(allowed).strip("_") or "combo") + ".json"


def _threshold_rows_for_checkpointed_combos(
    tickers: list[str],
    *,
    frame_loader: MarketFrameLoader | None,
    benchmark_frame: pd.DataFrame | None,
    combos: list[dict[str, Any]],
    base_config: PipelineBacktestConfig,
    output_dir: Path,
    resume: bool,
) -> list[dict[str, Any]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for combo in combos:
        combo_id = str(combo.get("combo_id") or "")
        output_path = output_dir / _combo_artifact_name(combo_id)
        if resume and output_path.exists():
            combo_rows = list(_read_json(output_path))
        else:
            grid = {
                key: [value]
                for key, value in combo.items()
                if key != "combo_id"
            }
            combo_rows = _threshold_rows_for_tickers(
                tickers,
                frame_loader=frame_loader,
                benchmark_frame=benchmark_frame,
                grid=grid,
                base_config=base_config,
            )
            _write_json(output_path, combo_rows)
        rows.extend(combo_rows)
    return rows


def run_phase1(
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_CAMPAIGN_DIR,
    resume: bool = False,
    frame_loader: MarketFrameLoader | None = None,
) -> dict[str, Any]:
    basket = load_basket(basket_path)
    phase_dir = _phase_dir(campaign_dir, 1)
    subset = subset_tickers_from_basket(basket)
    full = [str(ticker).upper() for ticker in basket.get("tickers") or []]
    benchmark = _load_benchmark(frame_loader)
    base_config = _baseline_config()
    subset_combos = expand_threshold_grid(phase1_grid())
    subset_rows = _threshold_rows_for_checkpointed_combos(
        subset,
        frame_loader=frame_loader,
        benchmark_frame=benchmark,
        combos=subset_combos,
        base_config=base_config,
        output_dir=phase_dir / "subset",
        resume=resume,
    )
    promoted = _top_configs_from_rows(subset_rows, limit=3)
    promoted_set = set(promoted)
    full_combos = [combo for combo in subset_combos if combo.get("combo_id") in promoted_set]
    full_rows = (
        _threshold_rows_for_checkpointed_combos(
            full,
            frame_loader=frame_loader,
            benchmark_frame=benchmark,
            combos=full_combos,
            base_config=base_config,
            output_dir=phase_dir / "full",
            resume=resume,
        )
        if full_combos
        else []
    )
    summary = {
        "phase": 1,
        "subset_tickers": subset,
        "promoted_combo_ids": promoted,
        "subset_rows": subset_rows,
        "full_rows": full_rows,
        "config_count": len(subset_combos),
    }
    _write_json(phase_dir / "summary.json", summary)
    return summary


def run_phase2(
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_CAMPAIGN_DIR,
    resume: bool = False,
    frame_loader: MarketFrameLoader | None = None,
    backtest_runner: BacktestRunner | None = None,
) -> dict[str, Any]:
    basket = load_basket(basket_path)
    phase_dir = _phase_dir(campaign_dir, 2)
    subset = subset_tickers_from_basket(basket)
    full = [str(ticker).upper() for ticker in basket.get("tickers") or []]
    benchmark = _load_benchmark(frame_loader)
    configs = phase2_configs()
    subset_units: list[dict[str, Any]] = []
    for config_spec in configs:
        config_id = str(config_spec["config_id"])
        config = _baseline_config(
            hmm_n_seeds=int(config_spec["hmm_n_seeds"]),
            hmm_covariance_type=str(config_spec["hmm_covariance_type"]),
            macro_weight=float(config_spec["macro_weight"]),
        )
        for ticker in subset:
            subset_units.append(
                _run_backtest_unit(
                    ticker=ticker,
                    config=config,
                    output_path=phase_dir / "subset" / config_id.replace("=", "_") / f"{ticker}.json",
                    config_id=config_id,
                    phase="2_subset",
                    resume=resume,
                    frame_loader=frame_loader,
                    benchmark_frame=benchmark,
                    backtest_runner=backtest_runner,
                )
            )
    promoted = _promote_backtest_configs(subset_units, limit=3)
    full_units: list[dict[str, Any]] = []
    for config_spec in configs:
        config_id = str(config_spec["config_id"])
        if config_id not in promoted:
            continue
        config = _baseline_config(
            hmm_n_seeds=int(config_spec["hmm_n_seeds"]),
            hmm_covariance_type=str(config_spec["hmm_covariance_type"]),
            macro_weight=float(config_spec["macro_weight"]),
        )
        for ticker in full:
            full_units.append(
                _run_backtest_unit(
                    ticker=ticker,
                    config=config,
                    output_path=phase_dir / "full" / config_id.replace("=", "_") / f"{ticker}.json",
                    config_id=config_id,
                    phase="2_full",
                    resume=resume,
                    frame_loader=frame_loader,
                    benchmark_frame=benchmark,
                    backtest_runner=backtest_runner,
                )
            )
    summary = {
        "phase": 2,
        "subset_tickers": subset,
        "promoted_config_ids": promoted,
        "subset": aggregate_by_config(subset_units),
        "full": aggregate_by_config(full_units),
        "config_count": len(configs),
    }
    _write_json(phase_dir / "summary.json", summary)
    return summary


def _promote_backtest_configs(units: list[dict[str, Any]], limit: int = 3) -> list[str]:
    rows = aggregate_by_config(units)
    configs = [row for row in rows if row.get("config_id")]
    configs.sort(
        key=lambda row: (
            _to_float(row.get("oos_sharpe_ratio")) or -999.0,
            _to_float(row.get("oos_total_return")) or -999.0,
        ),
        reverse=True,
    )
    return [str(row["config_id"]) for row in configs[: int(limit)]]


def run_phase3(
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_CAMPAIGN_DIR,
    resume: bool = False,
    frame_loader: MarketFrameLoader | None = None,
    min_oof_auc: float = DEFAULT_META_LABELER_MIN_OOF_AUC,
) -> dict[str, Any]:
    del resume
    basket = load_basket(basket_path)
    phase_dir = _phase_dir(campaign_dir, 3)
    tickers = [str(ticker).upper() for ticker in basket.get("tickers") or []]
    regime_pairs: list[tuple[str, Any]] = []
    for ticker in tickers:
        frame = _load_frame(ticker, loader=frame_loader)
        regime_pairs.append((ticker, fit_regime_model(ticker=ticker, market_frame=frame)))
    labeled = build_multi_ticker_managed_frame(regime_pairs, config=DEFAULT_MANAGED_EXIT_CONFIG)
    engine = MetaLabelerEngine(MetaLabelerConfig(min_training_samples=min(100, max(20, len(labeled) // 10 or 20))))
    label_config = asdict(DEFAULT_MANAGED_EXIT_CONFIG)
    metrics = engine.train(labeled, label_mode="managed", label_config=label_config)
    auc = _to_float(metrics.get("roc_auc"))
    status = "disqualified"
    ab_results: list[dict[str, Any]] = []
    if auc is not None and auc >= float(min_oof_auc):
        status = "qualified"
        ab_results = _run_meta_labeler_ab(tickers, engine, frame_loader=frame_loader)
    summary = {
        "phase": 3,
        "status": status,
        "min_oof_auc": float(min_oof_auc),
        "training_metrics": metrics,
        "label_config": label_config,
        "labeled_samples": int(len(labeled)),
        "ab_results": ab_results,
    }
    _write_json(phase_dir / "summary.json", summary)
    return summary


def _run_meta_labeler_ab(
    tickers: list[str],
    engine: MetaLabelerEngine,
    *,
    frame_loader: MarketFrameLoader | None,
) -> list[dict[str, Any]]:
    from .cli import _MetaLabelerVetoProvider

    benchmark = _load_benchmark(frame_loader)
    config = _baseline_config()
    rows: list[dict[str, Any]] = []
    for mode, provider in [
        ("no_veto", None),
        ("gate", _MetaLabelerVetoProvider(engine, veto_mode="gate")),
        ("size_only", _MetaLabelerVetoProvider(engine, veto_mode="size_only")),
    ]:
        units: list[dict[str, Any]] = []
        for ticker in tickers:
            result = run_pipeline_backtest(ticker, _load_frame(ticker, loader=frame_loader), config=config, benchmark_frame=benchmark, signal_provider=provider)
            units.append(_result_payload(result, config_id=mode, phase="3_ab"))
        aggregate = aggregate_result_payloads(units)
        evidence = provider.evidence_summary() if provider is not None else {}
        rows.append({"config_id": mode, **aggregate, "meta_labeler_evidence": evidence})
    return rows


def run_campaign_phase(
    phase: int,
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_CAMPAIGN_DIR,
    resume: bool = False,
) -> dict[str, Any]:
    if int(phase) == 0:
        return run_phase0(basket_path=basket_path, campaign_dir=campaign_dir, resume=resume)
    if int(phase) == 1:
        return run_phase1(basket_path=basket_path, campaign_dir=campaign_dir, resume=resume)
    if int(phase) == 2:
        return run_phase2(basket_path=basket_path, campaign_dir=campaign_dir, resume=resume)
    if int(phase) == 3:
        return run_phase3(basket_path=basket_path, campaign_dir=campaign_dir, resume=resume)
    raise ValueError("phase must be one of 0, 1, 2, or 3")


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else None


def _metric(payload: dict[str, Any], section: str, metric: str) -> float | None:
    value = payload.get(section)
    if isinstance(value, dict):
        return _to_float(value.get(metric))
    return None


def aggregate_result_payloads(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    if not payloads:
        return {
            "ticker_count": 0,
            "traded_ticker_count": 0,
            "oos_trade_count": 0,
        }
    oos_returns = [_metric(row, "out_of_sample", "total_return") for row in payloads]
    oos_sharpes = [_metric(row, "out_of_sample", "sharpe_ratio") for row in payloads]
    oos_drawdowns = [_metric(row, "out_of_sample", "max_drawdown") for row in payloads]
    oos_trades = [_metric(row, "out_of_sample", "trade_count") for row in payloads]
    full_returns = [_metric(row, "metrics", "total_return") for row in payloads]
    return {
        "ticker_count": len(payloads),
        "traded_ticker_count": sum(1 for value in oos_trades if (value or 0.0) > 0),
        "oos_trade_count": int(sum(value or 0.0 for value in oos_trades)),
        "oos_total_return": _mean(oos_returns),
        "oos_sharpe_ratio": _mean(oos_sharpes),
        "oos_max_drawdown": _mean(oos_drawdowns),
        "full_total_return": _mean(full_returns),
        "stress_windows": aggregate_stress_windows(payloads),
    }


def aggregate_by_config(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for payload in payloads:
        campaign_raw = payload.get("campaign")
        campaign = campaign_raw if isinstance(campaign_raw, dict) else {}
        config_id = str(campaign.get("config_id") or payload.get("config_id") or "unknown")
        grouped.setdefault(config_id, []).append(payload)
    return [
        {"config_id": config_id, **aggregate_result_payloads(rows)}
        for config_id, rows in sorted(grouped.items())
    ]


def aggregate_stress_windows(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for payload in payloads:
        for row in payload.get("stress_windows") or []:
            if isinstance(row, dict) and row.get("key"):
                grouped.setdefault(str(row["key"]), []).append(row)
    results: list[dict[str, Any]] = []
    for key, rows in sorted(grouped.items()):
        first = rows[0]
        results.append(
            {
                "key": key,
                "label": first.get("label"),
                "count": len(rows),
                "strategy_total_return_avg": _mean([row.get("strategy_total_return") for row in rows]),
                "benchmark_total_return_avg": _mean([row.get("benchmark_total_return") for row in rows]),
                "strategy_max_drawdown_avg": _mean([row.get("strategy_max_drawdown") for row in rows]),
                "benchmark_max_drawdown_avg": _mean([row.get("benchmark_max_drawdown") for row in rows]),
                "days_to_bear_flag_avg": _mean([row.get("days_to_bear_flag") for row in rows]),
                "trade_count_sum": int(sum(_to_float(row.get("trade_count")) or 0.0 for row in rows)),
            }
        )
    return results


def _mean(values: list[Any]) -> float | None:
    finite: list[float] = []
    for value in values:
        parsed = _to_float(value)
        if parsed is not None:
            finite.append(parsed)
    return sum(finite) / len(finite) if finite else None


def evidence_floor(row: dict[str, Any], *, min_trades: int = MIN_OOS_TRADES, min_names: int = MIN_TRADED_NAMES) -> dict[str, Any]:
    trades = int(_to_float(row.get("oos_trade_count")) or 0)
    names = int(_to_float(row.get("traded_ticker_count")) or 0)
    passed = trades >= int(min_trades) and names >= int(min_names)
    return {
        "passed": passed,
        "oos_trade_count": trades,
        "traded_ticker_count": names,
        "min_oos_trades": int(min_trades),
        "min_traded_names": int(min_names),
        "status": "rankable" if passed else "insufficient_sample",
    }


def robustness_verdict(
    baseline_by_ticker: dict[str, dict[str, Any]],
    candidate_by_ticker: dict[str, dict[str, Any]],
    *,
    min_trades: int = MIN_OOS_TRADES,
    min_names: int = MIN_TRADED_NAMES,
) -> dict[str, Any]:
    candidate_aggregate = aggregate_result_payloads(list(candidate_by_ticker.values()))
    floor = evidence_floor(candidate_aggregate, min_trades=min_trades, min_names=min_names)
    if not floor["passed"]:
        return {"verdict": "insufficient_sample", **floor}

    baseline_aggregate = aggregate_result_payloads(list(baseline_by_ticker.values()))
    return_delta = (_to_float(candidate_aggregate.get("oos_total_return")) or 0.0) - (_to_float(baseline_aggregate.get("oos_total_return")) or 0.0)
    sharpe_delta = (_to_float(candidate_aggregate.get("oos_sharpe_ratio")) or 0.0) - (_to_float(baseline_aggregate.get("oos_sharpe_ratio")) or 0.0)
    baseline_dd = abs(_to_float(baseline_aggregate.get("oos_max_drawdown")) or 0.0)
    candidate_dd = abs(_to_float(candidate_aggregate.get("oos_max_drawdown")) or 0.0)
    drawdown_ok = baseline_dd == 0.0 or candidate_dd <= baseline_dd * MAX_DRAWDOWN_WORSENING

    shared = sorted(set(baseline_by_ticker) & set(candidate_by_ticker))
    improved = 0
    for ticker in shared:
        base_return = _metric(baseline_by_ticker[ticker], "out_of_sample", "total_return") or 0.0
        candidate_return = _metric(candidate_by_ticker[ticker], "out_of_sample", "total_return") or 0.0
        if candidate_return > base_return:
            improved += 1
    improvement_rate = improved / len(shared) if shared else 0.0
    aggregate_wins = return_delta > 0.0 and sharpe_delta > 0.0
    if aggregate_wins and improvement_rate >= RECOMMENDED_MIN_NAME_WIN_RATE and drawdown_ok:
        verdict = "recommended"
    elif aggregate_wins or improvement_rate >= RECOMMENDED_MIN_NAME_WIN_RATE:
        verdict = "mixed"
    else:
        verdict = "not_supported"
    return {
        "verdict": verdict,
        **floor,
        "return_delta": return_delta,
        "sharpe_delta": sharpe_delta,
        "drawdown_ok": drawdown_ok,
        "individual_improvement_rate": improvement_rate,
        "individual_improved_count": improved,
        "individual_compared_count": len(shared),
    }


def _load_phase_summary(campaign_dir: str | Path, phase: int) -> dict[str, Any] | None:
    path = _phase_path(campaign_dir, phase) / "summary.json"
    if path.exists():
        return dict(_read_json(path))
    return None


def render_report(
    *,
    campaign_dir: str | Path = DEFAULT_CAMPAIGN_DIR,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    output_path: str | Path = DEFAULT_REPORT_PATH,
) -> str:
    basket = load_basket(basket_path) if Path(basket_path).exists() else {}
    phase0 = _load_phase_summary(campaign_dir, 0)
    phase1 = _load_phase_summary(campaign_dir, 1)
    phase2 = _load_phase_summary(campaign_dir, 2)
    phase3 = _load_phase_summary(campaign_dir, 3)
    lines = [
        "# Alpha Campaign Report",
        "",
        f"Generated: {dt.datetime.now(dt.timezone.utc).isoformat()}",
        f"Git SHA: `{_git_sha()}`",
        f"OOS boundary: `{CAMPAIGN_OOS_START}`",
        "",
        "## Basket",
        "",
        f"Selection rule: {basket.get('selection_rule', 'not available')}",
        f"Basket size: {basket.get('basket_size', 0)}",
        "",
        _markdown_table(["Sector", "Selected"], [
            [sector, ", ".join((basket.get("screen_stats", {}).get("sector_status", {}).get(sector, {}) or {}).get("selected", []))]
            for sector in GICS_SECTORS_10
        ]),
        "",
        "## Q1 Baseline Versus Buy-And-Hold",
        "",
        _summary_table(phase0.get("aggregate") if phase0 else None),
        "",
        "## Q2 Capability Sweep And HMM Robustness",
        "",
        _phase_summary_line("Phase 1", phase1),
        _phase_summary_line("Phase 2", phase2),
        "",
        "## Q3 Meta-Labeler Verdict",
        "",
        _meta_labeler_summary(phase3),
        "",
        "## Q4 Stress Windows",
        "",
        _stress_table((phase0 or {}).get("aggregate", {}).get("stress_windows") if phase0 else []),
        "",
        "## Configurations Evaluated",
        "",
        _config_count_table(phase1, phase2, phase3),
        "",
        "## Recommended Default Changes",
        "",
        "No defaults are changed by this campaign runner. Any recommendation below requires human approval after reviewing the evidence tables.",
        "",
        "- Pending: complete Phase 0-4 artifacts and review OOS evidence.",
        "",
    ]
    report = "\n".join(lines)
    Path(output_path).write_text(report, encoding="utf-8")
    return report


def _summary_table(aggregate: dict[str, Any] | None) -> str:
    if not aggregate:
        return "Phase 0 baseline has not been run yet."
    return _markdown_table(
        ["Metric", "Value"],
        [
            ["Ticker count", aggregate.get("ticker_count")],
            ["Traded tickers", aggregate.get("traded_ticker_count")],
            ["OOS trades", aggregate.get("oos_trade_count")],
            ["OOS return", _fmt_pct(aggregate.get("oos_total_return"))],
            ["OOS Sharpe", _fmt_num(aggregate.get("oos_sharpe_ratio"))],
            ["OOS max drawdown", _fmt_pct(aggregate.get("oos_max_drawdown"))],
        ],
    )


def _phase_summary_line(label: str, summary: dict[str, Any] | None) -> str:
    if not summary:
        return f"{label}: not run."
    return f"{label}: completed; configurations evaluated `{summary.get('config_count', 'n/a')}`."


def _meta_labeler_summary(summary: dict[str, Any] | None) -> str:
    if not summary:
        return "Phase 3 has not been run yet."
    metrics_raw = summary.get("training_metrics")
    metrics = metrics_raw if isinstance(metrics_raw, dict) else {}
    return _markdown_table(
        ["Metric", "Value"],
        [
            ["Status", summary.get("status")],
            ["OOF ROC-AUC", _fmt_num(metrics.get("roc_auc"))],
            ["Brier", _fmt_num(metrics.get("brier_score_calibrated") or metrics.get("brier_score"))],
            ["Positive rate", _fmt_pct(metrics.get("positive_rate_train"))],
            ["Labeled samples", summary.get("labeled_samples")],
            ["Skill bar", summary.get("min_oof_auc")],
        ],
    )


def _stress_table(rows: list[dict[str, Any]] | None) -> str:
    if not rows:
        return "No stress-window results are available yet."
    return _markdown_table(
        ["Window", "Strategy Return", "Benchmark Return", "Strategy DD", "Benchmark DD", "Days To Bear", "Trades"],
        [
            [
                row.get("key"),
                _fmt_pct(row.get("strategy_total_return_avg")),
                _fmt_pct(row.get("benchmark_total_return_avg")),
                _fmt_pct(row.get("strategy_max_drawdown_avg")),
                _fmt_pct(row.get("benchmark_max_drawdown_avg")),
                _fmt_num(row.get("days_to_bear_flag_avg")),
                row.get("trade_count_sum"),
            ]
            for row in rows
        ],
    )


def _config_count_table(phase1: dict[str, Any] | None, phase2: dict[str, Any] | None, phase3: dict[str, Any] | None) -> str:
    return _markdown_table(
        ["Phase", "Configurations"],
        [
            ["Phase 1", (phase1 or {}).get("config_count", "not run")],
            ["Phase 2", (phase2 or {}).get("config_count", "not run")],
            ["Phase 3", len((phase3 or {}).get("ab_results") or []) if phase3 else "not run"],
        ],
    )


def _markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    rendered = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        rendered.append("| " + " | ".join(str(value if value is not None else "") for value in row) + " |")
    return "\n".join(rendered)


def _fmt_num(value: Any) -> str:
    parsed = _to_float(value)
    return "" if parsed is None else f"{parsed:.3f}"


def _fmt_pct(value: Any) -> str:
    parsed = _to_float(value)
    return "" if parsed is None else f"{parsed:.2%}"


def campaign_status(campaign_dir: str | Path = DEFAULT_CAMPAIGN_DIR) -> dict[str, Any]:
    root = _campaign_dir(campaign_dir)
    return {
        "campaign_dir": str(root),
        "basket_exists": (root / "basket.json").exists(),
        "phases": {
            str(phase): (_phase_path(root, phase) / "summary.json").exists()
            for phase in range(4)
        },
        "stress_windows": stress_windows_payload(),
    }
