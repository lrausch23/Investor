from __future__ import annotations

from dataclasses import dataclass
import logging

import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM
from sklearn.preprocessing import StandardScaler

from .exceptions import InsufficientDataError, ModelFittingError
from .logging_config import setup_regime_logging

setup_regime_logging()
logger = logging.getLogger(__name__)


STATE_META = {
    "Bull": {
        "state_id": 0,
        "badge": "Bullish Expansion",
        "color": "#d9f2df",
        "plot_color": "#4caf50",
        "signal": "Positive Mean + Low Volatility",
    },
    "Neutral": {
        "state_id": 1,
        "badge": "Volatile Neutral",
        "color": "#e3e5e8",
        "plot_color": "#9aa0a6",
        "signal": "Near-zero Mean + Moderate Volatility",
    },
    "Bear": {
        "state_id": 2,
        "badge": "Bearish Contraction",
        "color": "#f8d7da",
        "plot_color": "#d9534f",
        "signal": "Negative Mean + High Volatility",
    },
}


@dataclass
class RegimeResult:
    ticker: str
    price_frame: pd.DataFrame
    latest_label: str
    latest_state_id: int
    latest_probability: float
    latest_price: float
    latest_state_vector: np.ndarray
    state_map: dict[int, str]
    canonical_state_map: dict[int, int]
    state_statistics: pd.DataFrame
    recent_state_mean_return: float | None
    regime_inconsistency_warning: str | None
    transition_matrix: np.ndarray
    expected_regime_duration: float
    transition_risk: float
    model: GaussianHMM
    scaler: StandardScaler
    regime_days: int = 0

    @property
    def regime_signal(self) -> str:
        badge = STATE_META[self.latest_label]["badge"]
        return f"{badge} (State {self.latest_state_id}) detected - Probability {self.latest_probability:.0%}"


def build_features(market_frame: pd.DataFrame, lookback_window: int = 20) -> pd.DataFrame:
    prices = market_frame["price"].astype(float)
    high = market_frame["high"].astype(float) if "high" in market_frame.columns else prices
    low = market_frame["low"].astype(float) if "low" in market_frame.columns else prices
    volume = market_frame["volume"].astype(float).replace(0.0, np.nan)
    vix = market_frame["vix"].astype(float)
    yield_10y = market_frame["yield_10y"].astype(float)

    returns = prices.pct_change()
    rolling_vol = returns.rolling(lookback_window).std() * np.sqrt(252)
    trend = prices.pct_change(max(5, lookback_window // 2))
    volume_z = (volume - volume.rolling(lookback_window).mean()) / volume.rolling(lookback_window).std()
    vix_change = vix.diff()
    yield_change = yield_10y.diff()

    frame = pd.DataFrame(
        {
            "price": prices,
            "high": high,
            "low": low,
            "volume": volume,
            "vix": vix,
            "yield_10y": yield_10y,
            "return": returns,
            "volatility": rolling_vol,
            "trend": trend,
            "volume_zscore": volume_z,
            "vix_change": vix_change,
            "yield_10y_change": yield_change,
        }
    ).dropna()
    if len(frame) < max(120, lookback_window * 4):
        raise InsufficientDataError("Insufficient history to fit a stable 3-state HMM. Increase the period or reduce the lookback window.")
    return frame

def _rank_state_labels(
    model_states: pd.Series,
    features: pd.DataFrame,
) -> tuple[dict[int, str], dict[int, int], pd.DataFrame]:
    """Assign Bull/Neutral/Bear labels using only training-window statistics."""
    labeled = features.assign(state=model_states)
    stats = (
        labeled
        .groupby("state")[["return", "volatility", "trend", "volume_zscore", "vix_change", "yield_10y_change"]]
        .mean()
        .rename(
            columns={
                "return": "mean_return",
                "volatility": "expected_volatility",
                "trend": "mean_trend",
                "volume_zscore": "volume_zscore",
                "vix_change": "mean_vix_change",
                "yield_10y_change": "mean_yield_10y_change",
            }
        )
    )
    stats["regime_score"] = stats["mean_return"] - 0.5 * stats["expected_volatility"]
    bull_state = stats["regime_score"].idxmax()
    bear_state = stats["regime_score"].idxmin()
    remaining = [state for state in stats.index if state not in {bull_state, bear_state}]
    neutral_state = remaining[0] if remaining else stats["regime_score"].abs().idxmin()

    state_map = {
        int(bull_state): "Bull",
        int(neutral_state): "Neutral",
        int(bear_state): "Bear",
    }
    canonical_state_map = {hidden_state: STATE_META[label]["state_id"] for hidden_state, label in state_map.items()}

    ordered_stats = stats.copy()
    ordered_stats["label"] = pd.Series(state_map)
    ordered_stats["state_id"] = pd.Series(canonical_state_map)
    ordered_stats = ordered_stats.sort_values("state_id")[
        [
            "state_id",
            "label",
            "regime_score",
            "mean_return",
            "expected_volatility",
            "mean_trend",
            "volume_zscore",
            "mean_vix_change",
            "mean_yield_10y_change",
        ]
    ]
    return state_map, canonical_state_map, ordered_stats


def fit_regime_model(
    ticker: str,
    market_frame: pd.DataFrame,
    lookback_window: int = 20,
    training_window: int = 504,
    refit_step: int = 21,
    macro_weighting: bool = False,
    n_states: int = 3,
    random_state: int = 7,
    iterations: int = 500,
) -> RegimeResult:
    logger.info(
        "Fitting HMM for %s rows=%d lookback=%d training_window=%d refit_step=%d",
        ticker,
        len(market_frame),
        lookback_window,
        training_window,
        refit_step,
    )
    features = build_features(market_frame, lookback_window=lookback_window)
    if len(features) < training_window:
        raise InsufficientDataError(f"Insufficient history for walk-forward analysis. Need at least {training_window} feature rows.")

    feature_cols = [
        "return",
        "volatility",
        "trend",
        "volume_zscore",
        "vix_change",
        "yield_10y_change",
    ]
    hidden_states = pd.Series(index=features.index, dtype=float, name="hidden_state")
    canonical_states = pd.Series(index=features.index, dtype=float, name="canonical_state")
    regime_labels = pd.Series(index=features.index, dtype=object, name="regime")
    state_probabilities = pd.Series(index=features.index, dtype=float, name="state_probability")

    latest_model: GaussianHMM | None = None
    latest_scaler: StandardScaler | None = None
    latest_state_map: dict[int, str] | None = None
    latest_canonical_state_map: dict[int, int] | None = None
    latest_state_statistics: pd.DataFrame | None = None
    latest_posteriors: np.ndarray | None = None
    last_refit_end_pos: int | None = None

    for end_pos in range(training_window, len(features) + 1):
        window = features.iloc[end_pos - training_window : end_pos].copy()
        should_refit = (
            latest_model is None
            or last_refit_end_pos is None
            or (end_pos - last_refit_end_pos) >= refit_step
            or end_pos == len(features)
        )
        X_window = window[feature_cols].to_numpy()

        if should_refit:
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X_window)
            if macro_weighting:
                X_scaled[:, 4:6] *= 1.5

            model = GaussianHMM(
                n_components=n_states,
                covariance_type="diag",
                n_iter=iterations,
                random_state=random_state,
            )
            model.fit(X_scaled)
            logger.debug("Refit HMM for %s at end_pos=%d converged=%s", ticker, end_pos, getattr(model.monitor_, "converged", None))

            decoded_window = pd.Series(model.predict(X_scaled), index=window.index, name="hidden_state")
            state_map, canonical_state_map, state_statistics = _rank_state_labels(decoded_window, window)
            posteriors = model.predict_proba(X_scaled)

            latest_model = model
            latest_scaler = scaler
            latest_state_map = state_map
            latest_canonical_state_map = canonical_state_map
            latest_state_statistics = state_statistics
            latest_posteriors = posteriors
            last_refit_end_pos = end_pos
        else:
            assert latest_model is not None
            assert latest_scaler is not None
            assert latest_state_map is not None
            assert latest_canonical_state_map is not None
            X_scaled = latest_scaler.transform(X_window)
            if macro_weighting:
                X_scaled[:, 4:6] *= 1.5
            decoded_window = pd.Series(latest_model.predict(X_scaled), index=window.index, name="hidden_state")
            posteriors = latest_model.predict_proba(X_scaled)
            state_map = latest_state_map
            canonical_state_map = latest_canonical_state_map
            state_statistics = latest_state_statistics

        current_index = window.index[-1]
        current_hidden_state = int(decoded_window.iloc[-1])
        hidden_states.loc[current_index] = current_hidden_state
        canonical_states.loc[current_index] = canonical_state_map[current_hidden_state]
        regime_labels.loc[current_index] = state_map[current_hidden_state]
        state_probabilities.loc[current_index] = float(posteriors[-1, current_hidden_state])

    if (
        latest_model is None
        or latest_scaler is None
        or latest_state_map is None
        or latest_canonical_state_map is None
        or latest_state_statistics is None
        or latest_posteriors is None
    ):
        raise ModelFittingError("Walk-forward HMM fitting failed to produce a valid model.")

    result_frame = features.loc[hidden_states.dropna().index].copy()
    result_frame["hidden_state"] = hidden_states.dropna().astype(int)
    result_frame["canonical_state"] = canonical_states.loc[result_frame.index].astype(int)
    result_frame["regime"] = regime_labels.loc[result_frame.index]
    result_frame["state_probability"] = state_probabilities.loc[result_frame.index].astype(float)

    latest_hidden_state = int(result_frame["hidden_state"].iloc[-1])
    latest_label = latest_state_map[latest_hidden_state]
    latest_state_id = latest_canonical_state_map[latest_hidden_state]
    latest_probability = float(result_frame["state_probability"].iloc[-1])
    latest_price = float(features["price"].iloc[-1])
    latest_state_vector = np.zeros(3, dtype=float)
    last_hidden_posterior = latest_posteriors[-1]
    for hidden_state, canonical_state in latest_canonical_state_map.items():
        latest_state_vector[canonical_state] = float(last_hidden_posterior[hidden_state])
    transition_matrix = np.zeros((3, 3), dtype=float)
    for from_hidden_state, from_canonical in latest_canonical_state_map.items():
        for to_hidden_state, to_canonical in latest_canonical_state_map.items():
            transition_matrix[from_canonical, to_canonical] = float(latest_model.transmat_[from_hidden_state, to_hidden_state])
    stay_probability = float(transition_matrix[latest_state_id, latest_state_id])
    # Capped at 999 trading days (~4 years); effectively permanent regime.
    expected_regime_duration = 999.0 if stay_probability >= 0.999999 else min(999.0, 1.0 / max(1e-9, 1.0 - stay_probability))
    transition_risk = max(0.0, min(1.0, 1.0 - stay_probability))
    regime_days = 0
    for label in result_frame["regime"].iloc[::-1]:
        if label != latest_label:
            break
        regime_days += 1
    regime_days = max(1, regime_days)
    recent_window = result_frame.tail(20)
    current_state_slice = recent_window.loc[recent_window["canonical_state"] == latest_state_id, "return"]
    recent_state_mean_return = float(current_state_slice.mean()) if not current_state_slice.empty else None
    regime_inconsistency_warning = None
    if latest_label == "Bull" and recent_state_mean_return is not None and recent_state_mean_return < 0:
        regime_inconsistency_warning = (
            "Regime Inconsistency: Bull state is active, but its mean return over the last 20 observations is negative."
        )

    result = RegimeResult(
        ticker=ticker,
        price_frame=result_frame,
        latest_label=latest_label,
        latest_state_id=latest_state_id,
        latest_probability=latest_probability,
        latest_price=latest_price,
        latest_state_vector=latest_state_vector,
        state_map=latest_state_map,
        canonical_state_map=latest_canonical_state_map,
        state_statistics=latest_state_statistics,
        regime_days=regime_days,
        recent_state_mean_return=recent_state_mean_return,
        regime_inconsistency_warning=regime_inconsistency_warning,
        transition_matrix=transition_matrix,
        expected_regime_duration=expected_regime_duration,
        transition_risk=transition_risk,
        model=latest_model,
        scaler=latest_scaler,
    )
    logger.info(
        "Completed HMM fit for %s label=%s probability=%.3f regime_days=%d",
        ticker,
        result.latest_label,
        result.latest_probability,
        result.regime_days,
    )
    return result


def fit_regime_model_weekly(
    ticker: str,
    market_frame: pd.DataFrame,
    lookback_window: int = 8,
    training_window: int = 104,
    refit_step: int = 4,
    macro_weighting: bool = False,
    n_states: int = 3,
    random_state: int = 7,
    iterations: int = 500,
) -> RegimeResult:
    weekly = market_frame.copy()
    weekly.index = pd.DatetimeIndex(weekly.index)
    weekly_frame = pd.DataFrame(
        {
            "price": weekly["price"].resample("W-FRI").last(),
            "high": weekly["high"].resample("W-FRI").max() if "high" in weekly.columns else weekly["price"].resample("W-FRI").last(),
            "low": weekly["low"].resample("W-FRI").min() if "low" in weekly.columns else weekly["price"].resample("W-FRI").last(),
            "volume": weekly["volume"].resample("W-FRI").sum(),
            "vix": weekly["vix"].resample("W-FRI").last(),
            "yield_10y": weekly["yield_10y"].resample("W-FRI").last(),
        }
    ).dropna()
    try:
        return fit_regime_model(
            ticker=ticker,
            market_frame=weekly_frame,
            lookback_window=lookback_window,
            training_window=training_window,
            refit_step=refit_step,
            macro_weighting=macro_weighting,
            n_states=n_states,
            random_state=random_state,
            iterations=iterations,
        )
    except InsufficientDataError:
        adaptive_window = max(26, min(training_window, max(26, len(weekly_frame) - refit_step)))
        if adaptive_window >= training_window:
            raise
        return fit_regime_model(
            ticker=ticker,
            market_frame=weekly_frame,
            lookback_window=lookback_window,
            training_window=adaptive_window,
            refit_step=refit_step,
            macro_weighting=macro_weighting,
            n_states=n_states,
            random_state=random_state,
            iterations=iterations,
        )
