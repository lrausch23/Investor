from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Protocol, Sequence, cast

import numpy as np
import pandas as pd

from ..basket_study import _normalize_frame
from ..sharadar import DEFAULT_SHARADAR_DIR, SharadarStore

UNVALIDATED_LABEL = "UNVALIDATED exploration — in-sample only, not certified."
DEFAULT_SNAPSHOT_HASH = "d2ccfd9ea42e4db663003dcfacfa6a3ce69e4e91ea5c059de82b356f3a17f527"
DEFAULT_HOLDOUT_START = "2024-01-01"
DEFAULT_HOLDOUT_END = "2025-12-31"


class MarketDataProvider(Protocol):
    @property
    def data_snapshot_hash(self) -> str | None: ...

    def universe_asof(self, date: str | pd.Timestamp, *, top_n: int = 500) -> list[int]: ...

    def get_prices(self, permatickers: Sequence[int | str], start: str, end: str) -> dict[int, pd.DataFrame]: ...

    def terminal_value_events(
        self,
        permatickers: Sequence[int | str],
        *,
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> dict[int, Any]: ...

    def synth_sp500_total_return(self, start: str | pd.Timestamp, end: str | pd.Timestamp) -> pd.DataFrame: ...


@dataclass(frozen=True)
class RLMarketEnvConfig:
    train_start: str = "1998-01-01"
    train_end: str = "2020-12-31"
    validation_start: str = "2021-01-01"
    validation_end: str = "2023-12-31"
    holdout_start: str = DEFAULT_HOLDOUT_START
    holdout_end: str = DEFAULT_HOLDOUT_END
    starting_cash: float = 100_000.0
    top_k: int = 12
    universe_top_n: int = 120
    episode_days: int = 252
    rebalance_every_days: int = 21
    lookback_days: int = 63
    entry_cost_bps: float = 5.0
    exit_cost_bps: float = 5.0
    slippage_bps: float = 2.0
    snapshot_hash: str = DEFAULT_SNAPSHOT_HASH

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EpisodeResult:
    terminal_wealth: float
    terminal_log_wealth: float
    steps: int
    costs_paid: float
    turnover: float
    terminal_events_used: list[dict[str, Any]]
    selected_permatickers: list[int]
    start: str
    end: str
    holdout_accessed: bool = False
    max_state_date: str | None = None
    label: str = UNVALIDATED_LABEL

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RLMarketEnv:
    """Faithful market sandbox for raw-terminal-wealth exploration.

    The action space is a tractable cash + top-K discretization: at each
    rebalance date the provider supplies a PIT eligible universe and the env
    selects the first top_k names. The agent emits target weights over those
    K names plus cash. Between rebalances the target weights drift with market
    returns. Costs and slippage are applied to turnover on every rebalance.
    """

    def __init__(self, provider: MarketDataProvider | None = None, config: RLMarketEnvConfig | None = None) -> None:
        self.provider = provider or SharadarStore(DEFAULT_SHARADAR_DIR)
        self.config = config or RLMarketEnvConfig()
        self._accessed_ranges: list[tuple[str, str]] = []

    @property
    def accessed_ranges(self) -> list[tuple[str, str]]:
        return list(self._accessed_ranges)

    def sample_episode_window(self, rng: np.random.Generator) -> tuple[str, str]:
        start_ts = pd.Timestamp(self.config.train_start).normalize()
        end_ts = pd.Timestamp(self.config.train_end).normalize()
        max_start = end_ts - pd.Timedelta(days=max(1, self.config.episode_days))
        if max_start <= start_ts:
            return start_ts.date().isoformat(), end_ts.date().isoformat()
        span_days = int((max_start - start_ts).days)
        sampled = start_ts + pd.Timedelta(days=int(rng.integers(0, span_days + 1)))
        episode_end = min(sampled + pd.Timedelta(days=self.config.episode_days), end_ts)
        return sampled.date().isoformat(), episode_end.date().isoformat()

    def run_episode(
        self,
        agent: Any,
        *,
        start: str,
        end: str,
        rng: np.random.Generator | None = None,
        train: bool = True,
    ) -> EpisodeResult:
        start_ts = pd.Timestamp(start).normalize()
        end_ts = pd.Timestamp(end).normalize()
        self._assert_not_holdout(start_ts, end_ts)
        research_floor = pd.Timestamp(self.config.train_start).normalize()
        lookback_ts = max(research_floor, start_ts - pd.Timedelta(days=max(1, self.config.lookback_days) + 10))
        lookback_start = lookback_ts.date().isoformat()
        universe = self.provider.universe_asof(start_ts, top_n=max(self.config.top_k, self.config.universe_top_n))
        selected = [int(item) for item in universe[: self.config.top_k]]
        if not selected:
            return EpisodeResult(
                terminal_wealth=self.config.starting_cash,
                terminal_log_wealth=0.0,
                steps=0,
                costs_paid=0.0,
                turnover=0.0,
                terminal_events_used=[],
                selected_permatickers=[],
                start=start_ts.date().isoformat(),
                end=end_ts.date().isoformat(),
            )
        self._record_access(lookback_start, end_ts.date().isoformat())
        price_frames = self.provider.get_prices(selected, lookback_start, end_ts.date().isoformat())
        price_panel = self._price_panel(selected, price_frames, start_ts, end_ts)
        if price_panel.empty or len(price_panel.index) < 2:
            return EpisodeResult(
                terminal_wealth=self.config.starting_cash,
                terminal_log_wealth=0.0,
                steps=0,
                costs_paid=0.0,
                turnover=0.0,
                terminal_events_used=[],
                selected_permatickers=selected,
                start=start_ts.date().isoformat(),
                end=end_ts.date().isoformat(),
            )
        selected = [int(column) for column in price_panel.columns]
        self._record_access(start_ts.date().isoformat(), end_ts.date().isoformat())
        terminal_events = self.provider.terminal_value_events(selected, start=start_ts, end=end_ts)
        return self._simulate(price_panel, selected, terminal_events, agent, rng=rng, train=train)

    def benchmark_terminal_wealth(self, *, start: str, end: str) -> float:
        start_ts = pd.Timestamp(start).normalize()
        end_ts = pd.Timestamp(end).normalize()
        self._assert_not_holdout(start_ts, end_ts)
        self._record_access(start_ts.date().isoformat(), end_ts.date().isoformat())
        frame = self.provider.synth_sp500_total_return(start_ts, end_ts)
        if frame.empty:
            return self.config.starting_cash
        normalized = _normalize_frame(frame)
        if normalized.empty:
            return self.config.starting_cash
        start_price = float(normalized["price"].iloc[0])
        end_price = float(normalized["price"].iloc[-1])
        if start_price <= 0:
            return self.config.starting_cash
        return float(self.config.starting_cash * (end_price / start_price))

    def _simulate(
        self,
        prices: pd.DataFrame,
        selected: list[int],
        terminal_events: dict[int, Any],
        agent: Any,
        *,
        rng: np.random.Generator | None,
        train: bool,
    ) -> EpisodeResult:
        dates = pd.DatetimeIndex(prices.index)
        equity = float(self.config.starting_cash)
        weights = np.zeros(len(selected) + 1, dtype=float)
        weights[0] = 1.0
        total_costs = 0.0
        total_turnover = 0.0
        rewards: list[float] = []
        processed_terminal: set[int] = set()
        terminal_rows: list[dict[str, Any]] = []
        active = np.ones(len(selected), dtype=bool)
        max_state_date: pd.Timestamp | None = None
        cost_rate = (float(self.config.entry_cost_bps) + float(self.config.exit_cost_bps) + float(self.config.slippage_bps)) / 10_000.0
        for idx, date in enumerate(dates):
            if idx % max(1, self.config.rebalance_every_days) == 0:
                features = self._state_features(prices, idx, active)
                max_state_date = date if max_state_date is None else max(max_state_date, date)
                target = np.asarray(agent.act(features, rng=rng, train=train), dtype=float)
                if len(target) != len(weights):
                    raise ValueError(f"Agent returned {len(target)} weights for {len(weights)} assets.")
                target = _normalize_weights(target)
                target[1:] = np.where(active, target[1:], 0.0)
                target = _normalize_weights(target)
                turnover = float(np.abs(target[1:] - weights[1:]).sum())
                cost = turnover * equity * cost_rate
                equity = max(0.0, equity - cost)
                total_costs += cost
                total_turnover += turnover
                weights = target
            if idx == 0:
                continue
            prev_equity = equity
            prev_prices = prices.iloc[idx - 1].astype(float).to_numpy()
            curr_prices = prices.iloc[idx].astype(float).to_numpy()
            asset_returns = np.zeros(len(selected), dtype=float)
            for asset_idx, perma in enumerate(selected):
                event = terminal_events.get(int(perma))
                if event is not None and int(perma) not in processed_terminal and pd.Timestamp(event.date).normalize() <= date:
                    terminal_price = float(event.value)
                    base = float(prev_prices[asset_idx])
                    asset_returns[asset_idx] = terminal_price / base - 1.0 if base > 0 else -1.0
                    active[asset_idx] = False
                    processed_terminal.add(int(perma))
                    terminal_rows.append(
                        {
                            "permaticker": int(perma),
                            "date": pd.Timestamp(event.date).date().isoformat(),
                            "value": terminal_price,
                            "source": str(event.source),
                            "reason": str(event.reason),
                        }
                    )
                    continue
                if not active[asset_idx]:
                    asset_returns[asset_idx] = 0.0
                    continue
                base = float(prev_prices[asset_idx])
                asset_returns[asset_idx] = float(curr_prices[asset_idx]) / base - 1.0 if base > 0 else 0.0
            portfolio_return = float(np.dot(weights[1:], asset_returns))
            equity = max(0.0, equity * (1.0 + portfolio_return))
            if equity > 0 and prev_equity > 0:
                rewards.append(float(math.log(equity / prev_equity)))
            else:
                rewards.append(float("-inf"))
            if not active.all():
                weights[1:] = np.where(active, weights[1:], 0.0)
                weights = _normalize_weights(weights)
        terminal_log = float(sum(reward for reward in rewards if math.isfinite(reward)))
        return EpisodeResult(
            terminal_wealth=float(equity),
            terminal_log_wealth=terminal_log,
            steps=max(0, len(dates) - 1),
            costs_paid=float(total_costs),
            turnover=float(total_turnover),
            terminal_events_used=terminal_rows,
            selected_permatickers=selected,
            start=dates[0].date().isoformat(),
            end=dates[-1].date().isoformat(),
            holdout_accessed=self._holdout_touched(),
            max_state_date=max_state_date.date().isoformat() if max_state_date is not None else None,
        )

    def _state_features(self, prices: pd.DataFrame, idx: int, active: np.ndarray) -> np.ndarray:
        start = max(0, idx - max(1, self.config.lookback_days))
        window = prices.iloc[start : idx + 1].astype(float)
        current = window.iloc[-1].to_numpy(dtype=float)
        first = window.iloc[0].to_numpy(dtype=float)
        momentum = np.divide(current, first, out=np.ones_like(current), where=first > 0) - 1.0
        returns = window.pct_change().replace([np.inf, -np.inf], 0.0).fillna(0.0)
        volatility = returns.std(ddof=0).to_numpy(dtype=float)
        rolling_max = window.cummax().iloc[-1].to_numpy(dtype=float)
        drawdown = np.divide(current, rolling_max, out=np.ones_like(current), where=rolling_max > 0) - 1.0
        features = np.column_stack([momentum, -volatility, drawdown])
        features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)
        features[~active, :] = 0.0
        return cast(np.ndarray, features)

    def _price_panel(
        self,
        selected: Sequence[int],
        frames: dict[int, pd.DataFrame],
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        series: dict[int, pd.Series] = {}
        for perma in selected:
            frame = frames.get(int(perma), pd.DataFrame())
            if frame.empty:
                continue
            normalized = _normalize_frame(frame)
            if normalized.empty or "price" not in normalized.columns:
                continue
            price = normalized["price"].astype(float)
            price.index = pd.to_datetime(price.index).normalize()
            series[int(perma)] = price
        if not series:
            return pd.DataFrame()
        panel = pd.concat(series, axis=1).sort_index().ffill()
        panel = panel.loc[(panel.index >= start_ts) & (panel.index <= end_ts)]
        panel = panel.dropna(how="all").ffill().dropna(axis=1)
        return panel

    def _record_access(self, start: str, end: str) -> None:
        start_ts = pd.Timestamp(start).normalize()
        end_ts = pd.Timestamp(end).normalize()
        self._assert_not_holdout(start_ts, end_ts)
        self._accessed_ranges.append((start_ts.date().isoformat(), end_ts.date().isoformat()))

    def _assert_not_holdout(self, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> None:
        holdout_start = pd.Timestamp(self.config.holdout_start).normalize()
        holdout_end = pd.Timestamp(self.config.holdout_end).normalize()
        if start_ts <= holdout_end and end_ts >= holdout_start:
            raise ValueError("RL exploration attempted to access quarantined 2024-2025 holdout.")

    def _holdout_touched(self) -> bool:
        holdout_start = pd.Timestamp(self.config.holdout_start).normalize()
        holdout_end = pd.Timestamp(self.config.holdout_end).normalize()
        for start, end in self._accessed_ranges:
            if pd.Timestamp(start) <= holdout_end and pd.Timestamp(end) >= holdout_start:
                return True
        return False


def _normalize_weights(weights: np.ndarray) -> np.ndarray:
    clean = np.nan_to_num(np.asarray(weights, dtype=float), nan=0.0, posinf=0.0, neginf=0.0)
    clean = np.clip(clean, 0.0, None)
    total = float(clean.sum())
    if total <= 0:
        out = np.zeros_like(clean)
        out[0] = 1.0
        return cast(np.ndarray, out)
    return cast(np.ndarray, clean / total)
