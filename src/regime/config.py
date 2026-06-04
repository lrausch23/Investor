from __future__ import annotations

from dataclasses import dataclass, field
import os

DEFAULT_TICKERS = ["NVDA", "AVGO", "PLTR", "MSFT", "MTRN", "PLAB"]

EXCLUDED_TICKER_PATTERNS: set[str] = {
    "EUR",
    "GBP",
    "JPY",
    "CHF",
    "CAD",
    "AUD",
    "GLD",
    "SGOL",
    "SLV",
    "GDX",
    "GDXJ",
    "SGOV",
    "BIL",
    "SHV",
    "TLT",
    "AGG",
    "BND",
    "LQD",
    "HYG",
    "VOO",
    "SPY",
    "QQQ",
    "IVV",
    "VTI",
}

HMM_ELIGIBLE_ASSET_CLASSES: set[str] = {
    "EQUITY",
    "ETF",
    "STOCK",
    "UNKNOWN",
}


@dataclass(frozen=True)
class SignalThresholds:
    """Tunable thresholds for regime signal generation."""

    strong_buy_max_transition_risk: float = 0.05
    strong_buy_min_duration: float = 15.0
    strong_buy_min_probability: float = 0.70
    buy_max_transition_risk: float = 0.15
    neutral_bull_tilt_probability: float = 0.40
    strong_sell_max_transition_risk: float = 0.05
    strong_sell_min_duration: float = 10.0
    strong_sell_min_probability: float = 0.70
    sell_max_transition_risk: float = 0.15
    bear_emerging_probability: float = 0.40
    hold_bull_max_transition_risk: float = 0.30
    earnings_strength_penalty: float = 0.15


DEFAULT_SIGNAL_THRESHOLDS = SignalThresholds()


@dataclass(frozen=True)
class DiscoveryThresholds:
    """Tunable thresholds for discovery entry signals."""

    entry_signal_min_probability: float = 0.55
    entry_signal_max_crowd_score: int = 40
    entry_signal_min_conviction: int = 3
    stale_candidate_max_age_days: int = 90
    crowd_cache_ttl_seconds: int = 14400
    crowd_cache_max_size: int = 500


DEFAULT_DISCOVERY_THRESHOLDS = DiscoveryThresholds()


@dataclass(frozen=True)
class PaperTradingConfig:
    """Configuration for paper trading simulation."""

    default_budget: float = 100000.0
    conviction_allocation: tuple[float, ...] = (0.0, 0.05, 0.10, 0.15, 0.25, 0.30)
    core_max_pct: float = 0.50
    critical_path_max_pct: float = 0.35
    speculative_max_pct: float = 0.15
    speculative_absolute_cap_pct: float = 0.05
    min_cash_reserve_pct: float = 0.10


DEFAULT_PAPER_TRADING_CONFIG = PaperTradingConfig()


@dataclass(frozen=True)
class RiskGuardrails:
    """Execution guardrails for broker-submitted paper trades."""

    max_position_pct: float = 0.10
    max_single_order_value: float = 10000.0
    daily_loss_limit: float = 5000.0
    daily_loss_limit_pct: float = 0.02
    max_trades_per_day: int = 10
    max_total_exposure_pct: float = 0.80
    max_limit_price_deviation_pct: float = 0.10
    max_marketable_limit_deviation_pct: float = 0.03


DEFAULT_RISK_GUARDRAILS = RiskGuardrails()


@dataclass(frozen=True)
class IBKRConfig:
    host: str = field(default_factory=lambda: os.environ.get("IBKR_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: int(os.environ.get("IBKR_PORT", "7497")))
    client_id: int = field(default_factory=lambda: int(os.environ.get("IBKR_CLIENT_ID", "1")))
    account_id: str = field(default_factory=lambda: os.environ.get("IBKR_ACCOUNT_ID", "DUP579027"))
    live_account_id: str = field(default_factory=lambda: os.environ.get("IBKR_LIVE_ACCOUNT_ID", ""))
    paper_backend: bool = field(default_factory=lambda: os.environ.get("IBKR_PAPER_BACKEND", "false").lower() in ("true", "1", "yes"))
    live_backend: bool = field(default_factory=lambda: os.environ.get("IBKR_LIVE_BACKEND", "false").lower() in ("true", "1", "yes"))
    execution_client_id_offset: int = field(default_factory=lambda: int(os.environ.get("IBKR_EXECUTION_CLIENT_ID_OFFSET", "20")))
    timeout: int = field(default_factory=lambda: int(os.environ.get("IBKR_TIMEOUT", "10")))


DEFAULT_IBKR_CONFIG = IBKRConfig()


PAPER_IBKR_PORTS = {7497, 4002}
LIVE_IBKR_PORTS = {7496, 4001}
VALID_IBKR_PORTS = PAPER_IBKR_PORTS | LIVE_IBKR_PORTS
LOCAL_IBKR_HOSTS = {"127.0.0.1", "localhost"}


def _is_du_paper_account(account_id: str) -> bool:
    return str(account_id or "").strip().upper().startswith("DU")


def is_ibkr_paper_config(config: IBKRConfig | None = None) -> bool:
    cfg = config or IBKRConfig()
    return (
        str(cfg.host).strip().lower() in LOCAL_IBKR_HOSTS
        and int(cfg.port) in PAPER_IBKR_PORTS
        and _is_du_paper_account(cfg.account_id)
    )


def should_use_ibkr_paper_backend(config: IBKRConfig | None = None) -> bool:
    cfg = config or IBKRConfig()
    return bool(cfg.paper_backend and is_ibkr_paper_config(cfg))


def should_use_real_ibkr_backend(config: IBKRConfig | None = None) -> bool:
    cfg = config or IBKRConfig()
    return bool(cfg.live_backend or should_use_ibkr_paper_backend(cfg))


def ibkr_backend_account_id(config: IBKRConfig | None = None) -> str:
    cfg = config or IBKRConfig()
    if cfg.live_backend and str(cfg.live_account_id or "").strip():
        return str(cfg.live_account_id).strip()
    return str(cfg.account_id or "").strip()


def ibkr_execution_mode(config: IBKRConfig | None = None) -> str:
    cfg = config or IBKRConfig()
    if should_use_ibkr_paper_backend(cfg):
        return "ibkr_paper"
    if cfg.live_backend:
        return "ibkr_live"
    if cfg.paper_backend:
        return "ibkr_paper_misconfigured"
    return "simulated"


def validate_ibkr_readiness() -> dict[str, bool]:
    """Check IBKR configuration before enabling broker-backed execution."""
    config = IBKRConfig()
    checks = {
        "live_backend_enabled": bool(config.live_backend),
        "paper_backend_enabled": bool(config.paper_backend),
        "execution_backend_enabled": should_use_real_ibkr_backend(config),
        "account_configured": bool(str(config.account_id or "").strip()),
        "account_is_paper": _is_du_paper_account(config.account_id),
        "port_is_paper": int(config.port) == 7497,
        "port_is_paper_api": int(config.port) in PAPER_IBKR_PORTS,
        "port_is_valid": int(config.port) in {7496, 7497, 4001, 4002},
        "host_is_local": str(config.host).strip().lower() in {"127.0.0.1", "localhost"},
        "paper_backend_ready": should_use_ibkr_paper_backend(config),
        "execution_client_id_valid": 0 <= int(config.execution_client_id_offset) <= 900,
    }
    checks["all_clear"] = all(
        checks[key] for key in ("account_configured", "port_is_valid", "host_is_local", "execution_client_id_valid")
    ) and (
        bool(checks["live_backend_enabled"])
        or bool(checks["paper_backend_ready"])
    )
    return checks


def ticker_candidates(ticker: str) -> list[str]:
    """Generate yfinance-compatible ticker symbol candidates."""
    base = (ticker or "").strip().upper()
    if not base:
        return []

    seen: set[str] = set()
    candidates: list[str] = []

    def add(symbol: str) -> None:
        normalized = symbol.strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            candidates.append(normalized)

    if " " in base:
        add(base.replace(" ", "-"))
        add(base.replace(" ", "."))
        add(base.replace(" ", ""))
    add(base)
    if "-" in base:
        add(base.replace("-", "."))
        add(base.replace("-", ""))
    if "." in base:
        add(base.replace(".", "-"))
        add(base.replace(".", ""))
    return candidates
