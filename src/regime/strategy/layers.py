from __future__ import annotations

import math
from typing import Any

import pandas as pd

from .interfaces import ExposureOverride, SignalMap, SignalValue
from .registry import register_layer


def _as_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        parsed = float(value)
    except Exception:
        return default
    return parsed if math.isfinite(parsed) else default


def _clip(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


@register_layer("signal", "price_history")
class PriceHistorySignalProvider:
    """Price-derived signal provider with no future-looking calculations."""

    def __init__(self) -> None:
        self._signals: dict[str, pd.DataFrame] = {}

    def prepare(self, ticker: str, frame: pd.DataFrame) -> None:
        normalized = _normalize_frame(frame)
        price = normalized["price"].astype(float)
        daily_return = price.pct_change().fillna(0.0)
        ma_200 = price.rolling(200, min_periods=1).mean()
        above_ma = price > ma_200
        confirmed = above_ma.rolling(5, min_periods=5).sum().fillna(0.0) >= 5.0
        momentum = price.shift(21) / price.shift(252) - 1.0
        realized_vol = daily_return.rolling(20, min_periods=2).std(ddof=0).fillna(0.0) * math.sqrt(252.0)
        signals = pd.DataFrame(
            {
                "price": price,
                "open": normalized["open"].astype(float),
                "daily_return": daily_return,
                "ma_200": ma_200,
                "above_ma_200": above_ma,
                "ma_200_confirmed": confirmed,
                "momentum_12_1": momentum,
                "realized_vol_20d": realized_vol,
            },
            index=normalized.index,
        )
        for column in (
            "regime_label",
            "regime",
            "p_bull_day5",
            "p_bear_day5",
            "p_neutral_day5",
            "market_timing_confirmed",
        ):
            if column in normalized.columns:
                signals[column] = normalized[column]
        self._signals[str(ticker).upper()] = signals

    def signals(self, ticker: str, date: pd.Timestamp) -> dict[str, SignalValue]:
        frame = self._signals.get(str(ticker).upper())
        if frame is None or frame.empty:
            return {}
        date = pd.Timestamp(date)
        rows = frame.loc[frame.index <= date]
        if rows.empty:
            return {}
        row = rows.iloc[-1]
        regime = str(row.get("regime_label") or row.get("regime") or "Bull")
        output = {
            "price": _as_float(row.get("price")),
            "open": _as_float(row.get("open")),
            "daily_return": _as_float(row.get("daily_return"), 0.0),
            "ma_200": _as_float(row.get("ma_200")),
            "ma_200_confirmed": bool(row.get("ma_200_confirmed", False)),
            "momentum_12_1": _as_float(row.get("momentum_12_1")),
            "realized_vol_20d": _as_float(row.get("realized_vol_20d"), 0.0),
            "regime": regime,
            "regime_label": regime,
            "p_bull_day5": _as_float(row.get("p_bull_day5")),
            "p_bear_day5": _as_float(row.get("p_bear_day5")),
            "p_neutral_day5": _as_float(row.get("p_neutral_day5")),
        }
        if "market_timing_confirmed" in row:
            output["market_timing_confirmed"] = bool(row.get("market_timing_confirmed", False))
        return output

    def prepared_frame(self, ticker: str) -> pd.DataFrame:
        frame = self._signals.get(str(ticker).upper())
        return pd.DataFrame() if frame is None else frame.copy()


@register_layer("signal", "precomputed_regime")
class PrecomputedRegimeSignalProvider(PriceHistorySignalProvider):
    """Reads regime columns already cached on the market frame."""


@register_layer("signal", "regime_hmm")
class RegimeHMMSignalProvider(PriceHistorySignalProvider):
    """Walk-forward HMM signal series, computed once per ticker and then reused."""

    def __init__(self, training_window: int = 504, refit_step: int = 21, lookback_window: int = 20) -> None:
        super().__init__()
        self.training_window = int(training_window)
        self.refit_step = int(refit_step)
        self.lookback_window = int(lookback_window)

    def prepare(self, ticker: str, frame: pd.DataFrame) -> None:
        super().prepare(ticker, frame)
        normalized = _normalize_frame(frame)
        if len(normalized) < max(90, self.lookback_window + 10):
            return
        try:
            from ..pipeline_backtest import PipelineBacktestConfig, _ProductionSignalProvider
        except Exception:
            return
        provider = _ProductionSignalProvider()
        config = PipelineBacktestConfig(
            training_window=self.training_window,
            refit_step=self.refit_step,
            lookback_window=self.lookback_window,
        )
        previous_regime: str | None = None
        rows: dict[pd.Timestamp, dict[str, SignalValue]] = {}
        last_refit_idx: int | None = None
        min_history = max(90, self.lookback_window + 10)
        for idx, date in enumerate(normalized.index):
            if idx + 1 < min_history:
                continue
            if last_refit_idx is not None and idx - last_refit_idx < max(1, self.refit_step):
                continue
            history = normalized.loc[:date]
            signal = provider(str(ticker).upper(), pd.Timestamp(date), history, config, previous_regime)
            if signal is None:
                continue
            rows[pd.Timestamp(date)] = {
                "regime": signal.regime,
                "regime_label": signal.regime,
                "probability": signal.probability,
                "p_bull_day5": signal.p_bull_day5,
                "p_bear_day5": signal.p_bear_day5,
                "p_neutral_day5": signal.p_neutral_day5,
                "transition_risk": signal.transition_risk,
                "regime_days": signal.regime_days,
                "composite_action": signal.composite_action,
                "composite_strength": signal.composite_strength,
            }
            previous_regime = signal.regime
            last_refit_idx = idx
        if not rows:
            return
        base = self._signals[str(ticker).upper()].copy()
        regime_frame = pd.DataFrame.from_dict(rows, orient="index")
        for column in regime_frame.columns:
            base[column] = regime_frame[column].reindex(base.index).ffill()
        self._signals[str(ticker).upper()] = base


@register_layer("exposure", "always_full")
class AlwaysFullExposure:
    def target_exposure(self, date: pd.Timestamp, portfolio_state: dict[str, Any], signal_map: SignalMap) -> float:
        del date, portfolio_state, signal_map
        return 1.0


@register_layer("exposure", "vol_target")
class VolTargetExposure:
    def __init__(self, target_vol: float = 0.15, min_exposure: float = 0.25, ewma_lambda: float = 0.94, min_history: int = 20) -> None:
        self.target_vol = float(target_vol)
        self.min_exposure = float(min_exposure)
        self.ewma_lambda = float(ewma_lambda)
        self.min_history = int(min_history)

    def target_exposure(self, date: pd.Timestamp, portfolio_state: dict[str, Any], signal_map: SignalMap) -> float:
        del date, signal_map
        returns = [float(value) for value in portfolio_state.get("portfolio_returns", []) if _as_float(value) is not None]
        if len(returns) < self.min_history:
            return 1.0
        variance = 0.0
        for value in returns[-252:]:
            variance = self.ewma_lambda * variance + (1.0 - self.ewma_lambda) * (value ** 2)
        forecast_vol = math.sqrt(max(0.0, variance) * 252.0)
        if forecast_vol <= 0:
            return 1.0
        return _clip(self.target_vol / forecast_vol, self.min_exposure, 1.0)


@register_layer("exposure", "moving_average_timing")
class MovingAverageTimingExposure:
    def __init__(self, ticker: str = "SPY") -> None:
        self.ticker = str(ticker).upper()

    def target_exposure(self, date: pd.Timestamp, portfolio_state: dict[str, Any], signal_map: SignalMap) -> float:
        del date, portfolio_state
        row = signal_map.get(self.ticker) or next(iter(signal_map.values()), {})
        return 1.0 if bool(row.get("ma_200_confirmed")) else 0.0


@register_layer("override", "regime_brake")
class RegimeBrakeOverride:
    def __init__(
        self,
        breadth_trigger: float = 0.5,
        breadth_cap: float = 0.5,
        aux_dd_trigger: float = 0.08,
        aux_cap: float = 0.5,
        reentry_days: int = 3,
    ) -> None:
        self.breadth_trigger = float(breadth_trigger)
        self.breadth_cap = float(breadth_cap)
        self.aux_dd_trigger = float(aux_dd_trigger)
        self.aux_cap = float(aux_cap)
        self.reentry_days = int(reentry_days)
        self._excluded: set[str] = set()
        self._reentry_counts: dict[str, int] = {}

    def override(self, date: pd.Timestamp, portfolio_state: dict[str, Any], signal_map: SignalMap) -> ExposureOverride | None:
        del date
        reasons: dict[str, str] = {}
        tickers = sorted(signal_map)
        bear_count = 0
        non_bull_count = 0
        for ticker in tickers:
            row = signal_map.get(ticker, {})
            regime = str(row.get("regime_label") or row.get("regime") or "Neutral")
            if regime == "Bear":
                bear_count += 1
                self._excluded.add(ticker)
                self._reentry_counts[ticker] = 0
                reasons[ticker] = "bear_regime"
                continue
            if regime != "Bull":
                non_bull_count += 1
            if ticker in self._excluded:
                p_bull = _as_float(row.get("p_bull_day5"), 0.0) or 0.0
                p_bear = _as_float(row.get("p_bear_day5"), 0.0) or 0.0
                reentry_signal = regime == "Bull" or (regime == "Neutral" and p_bull > p_bear)
                self._reentry_counts[ticker] = (self._reentry_counts.get(ticker, 0) + 1) if reentry_signal else 0
                if self._reentry_counts[ticker] >= self.reentry_days:
                    self._excluded.discard(ticker)
                    self._reentry_counts[ticker] = 0
                else:
                    reasons[ticker] = f"awaiting_reentry_{self._reentry_counts[ticker]}_of_{self.reentry_days}"
        exposure_cap: float | None = None
        portfolio_reasons: list[str] = []
        bear_share = (bear_count / len(tickers)) if tickers else 0.0
        if bear_share >= self.breadth_trigger:
            exposure_cap = self.breadth_cap
            portfolio_reasons.append(f"breadth_bear_share={bear_share:.2f}")
        drawdown = abs(float(portfolio_state.get("drawdown") or 0.0))
        median_non_bull = non_bull_count >= max(1, math.ceil(len(tickers) / 2)) if tickers else False
        if drawdown >= self.aux_dd_trigger and median_non_bull:
            exposure_cap = min(exposure_cap if exposure_cap is not None else 1.0, self.aux_cap)
            portfolio_reasons.append(f"aux_grinding_bear_drawdown={drawdown:.2%}")
        excluded = tuple(sorted(self._excluded))
        if not excluded and exposure_cap is None:
            return None
        return ExposureOverride(
            exposure_cap=exposure_cap,
            exclude_tickers=excluded,
            reasons=reasons,
            reason=", ".join(portfolio_reasons) if portfolio_reasons else "per_name_regime_exclusion",
        )


@register_layer("override", "market_timing_brake")
class MarketTimingBrakeOverride:
    """Portfolio-level brake driven by a precomputed market timing signal.

    Campaign runners can copy a benchmark timing state onto each investable
    row. The override then caps exposure without making the benchmark an
    eligible portfolio holding.
    """

    def __init__(self, cap: float = 0.0, signal_column: str = "market_timing_confirmed", missing_is_safe: bool = True) -> None:
        self.cap = float(cap)
        self.signal_column = str(signal_column)
        self.missing_is_safe = bool(missing_is_safe)

    def override(self, date: pd.Timestamp, portfolio_state: dict[str, Any], signal_map: SignalMap) -> ExposureOverride | None:
        del date, portfolio_state
        values = [bool(row.get(self.signal_column)) for row in signal_map.values() if self.signal_column in row]
        if not values:
            if self.missing_is_safe:
                return None
            return ExposureOverride(exposure_cap=_clip(self.cap, 0.0, 1.0), reason=f"{self.signal_column}_missing")
        if any(values):
            return None
        return ExposureOverride(exposure_cap=_clip(self.cap, 0.0, 1.0), reason=f"{self.signal_column}_false")


@register_layer("allocation", "equal_weight")
class EqualWeightAllocation:
    def weights(self, date: pd.Timestamp, eligible_names: list[str], signal_map: SignalMap) -> dict[str, float]:
        del date, signal_map
        names = [str(name).upper() for name in eligible_names]
        if not names:
            return {}
        weight = 1.0 / len(names)
        return {name: weight for name in names}


@register_layer("allocation", "momentum_tilt")
class MomentumTiltAllocation:
    def __init__(self, top_fraction: float = 0.5) -> None:
        self.top_fraction = float(top_fraction)
        self.call_count = 0

    def weights(self, date: pd.Timestamp, eligible_names: list[str], signal_map: SignalMap) -> dict[str, float]:
        del date
        self.call_count += 1
        scored = []
        for name in eligible_names:
            score = _as_float((signal_map.get(str(name).upper()) or {}).get("momentum_12_1"))
            if score is not None:
                scored.append((str(name).upper(), score))
        if not scored:
            return EqualWeightAllocation().weights(pd.Timestamp.now(tz="UTC"), eligible_names, signal_map)
        scored.sort(key=lambda item: (-item[1], item[0]))
        count = max(1, math.ceil(len(scored) * _clip(self.top_fraction, 0.0, 1.0)))
        selected = [name for name, _score in scored[:count]]
        weight = 1.0 / len(selected)
        return {name: weight for name in selected}


@register_layer("rebalance", "monthly_bands")
class MonthlyBandsRebalance:
    def __init__(self, band: float = 0.25) -> None:
        self.band = float(band)

    def should_rebalance(self, date: pd.Timestamp, drift_state: dict[str, Any]) -> bool:
        del date
        if bool(drift_state.get("is_first_trading_day_month")):
            return True
        for value in drift_state.get("relative_drifts", {}).values():
            parsed = _as_float(value, 0.0) or 0.0
            if abs(parsed) > self.band:
                return True
        return False


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    rename = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "price",
        "Adj Close": "price",
        "Volume": "volume",
    }
    normalized = normalized.rename(columns={column: rename.get(str(column), str(column)) for column in normalized.columns})
    if "price" not in normalized.columns and "close" in normalized.columns:
        normalized["price"] = normalized["close"]
    if "open" not in normalized.columns:
        normalized["open"] = normalized["price"]
    if "volume" not in normalized.columns:
        normalized["volume"] = 1_000_000.0
    normalized = normalized.sort_index()
    normalized["price"] = pd.to_numeric(normalized["price"], errors="coerce")
    normalized["open"] = pd.to_numeric(normalized["open"], errors="coerce")
    return normalized.dropna(subset=["price", "open"])
