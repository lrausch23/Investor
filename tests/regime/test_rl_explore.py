from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Sequence

import numpy as np
import pandas as pd
import pytest

from src.regime.rl_explore.agent import RLAgentConfig
from src.regime.rl_explore.env import DEFAULT_SNAPSHOT_HASH, RLMarketEnv, RLMarketEnvConfig
from src.regime.rl_explore.train import (
    CHECKPOINT_DIR_NAME,
    RLExploreConfig,
    load_latest_good_checkpoint,
    rl_explore_status,
    run_rl_explore,
    write_checkpoint,
)


class FakeMarketDataProvider:
    def __init__(self, *, terminal_event: SimpleNamespace | None = None) -> None:
        self._terminal_event = terminal_event
        self.accessed_ranges: list[tuple[str, str, str]] = []
        self.universe_dates: list[str] = []
        dates = pd.bdate_range("2019-01-01", "2025-12-31")
        trend = np.arange(len(dates), dtype=float)
        self.frames = {
            1: self._price_frame(dates, 100.0 * np.power(1.0007, trend)),
            2: self._price_frame(dates, 80.0 * np.power(1.0011, trend)),
            3: self._price_frame(dates, 60.0 * np.power(0.9998, trend)),
        }
        self.benchmark = self._price_frame(dates, 100.0 * np.power(1.0005, trend))

    @property
    def data_snapshot_hash(self) -> str | None:
        return DEFAULT_SNAPSHOT_HASH

    def universe_asof(self, date: str | pd.Timestamp, *, top_n: int = 500) -> list[int]:
        date_text = pd.Timestamp(date).date().isoformat()
        self.universe_dates.append(date_text)
        return [1, 2, 3][:top_n]

    def get_prices(self, permatickers: Sequence[int | str], start: str, end: str) -> dict[int, pd.DataFrame]:
        self._record("prices", start, end)
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        out: dict[int, pd.DataFrame] = {}
        for perma in permatickers:
            key = int(perma)
            frame = self.frames[key]
            out[key] = frame.loc[(frame.index >= start_ts) & (frame.index <= end_ts)].copy()
        return out

    def terminal_value_events(
        self,
        permatickers: Sequence[int | str],
        *,
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> dict[int, SimpleNamespace]:
        if start is not None and end is not None:
            self._record("terminal", str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date()))
        if self._terminal_event is None:
            return {}
        event_date = pd.Timestamp(self._terminal_event.date).normalize()
        start_ts = pd.Timestamp(start).normalize() if start is not None else pd.Timestamp.min
        end_ts = pd.Timestamp(end).normalize() if end is not None else pd.Timestamp.max
        wanted = {int(item) for item in permatickers}
        if int(self._terminal_event.permaticker) in wanted and start_ts <= event_date <= end_ts:
            return {int(self._terminal_event.permaticker): self._terminal_event}
        return {}

    def synth_sp500_total_return(self, start: str | pd.Timestamp, end: str | pd.Timestamp) -> pd.DataFrame:
        self._record("benchmark", str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date()))
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        return self.benchmark.loc[(self.benchmark.index >= start_ts) & (self.benchmark.index <= end_ts)].copy()

    @staticmethod
    def _price_frame(dates: pd.DatetimeIndex, close: np.ndarray) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "open": close * 0.999,
                "close": close,
                "closeadj": close,
                "volume": np.full(len(dates), 1_000_000),
            },
            index=dates,
        )

    def _record(self, kind: str, start: str, end: str) -> None:
        self.accessed_ranges.append((kind, pd.Timestamp(start).date().isoformat(), pd.Timestamp(end).date().isoformat()))


class PermaTwoAgent:
    def act(self, features: np.ndarray, *, rng: np.random.Generator | None = None, train: bool = True) -> np.ndarray:
        weights = np.zeros(features.shape[0] + 1, dtype=float)
        weights[2] = 1.0
        return weights


def _small_cfg(
    output_dir: Path,
    *,
    max_episodes: int | None = None,
    max_steps: int | None = None,
    max_wall_clock: str | float | int | None = None,
    force_new: bool = False,
) -> RLExploreConfig:
    return RLExploreConfig(
        output_dir=output_dir,
        seed=123,
        success_margin=999.0,
        checkpoint_every_episodes=1,
        checkpoint_every_steps=10_000,
        checkpoint_every_minutes=10_000.0,
        keep_checkpoints=20,
        validation_every_episodes=1,
        max_steps=max_steps,
        max_episodes=max_episodes,
        max_wall_clock=max_wall_clock,
        force_new=force_new,
        env=RLMarketEnvConfig(
            train_start="2020-01-01",
            train_end="2020-04-30",
            validation_start="2020-05-01",
            validation_end="2020-08-31",
            top_k=2,
            universe_top_n=3,
            episode_days=25,
            rebalance_every_days=5,
            lookback_days=10,
        ),
        agent=RLAgentConfig(learning_rate=0.02, exploration_sigma=0.05),
    )


def _latest_state(root: Path) -> dict:
    state, checkpoint = load_latest_good_checkpoint(root / CHECKPOINT_DIR_NAME)
    assert checkpoint is not None
    assert state is not None
    return state


def _assert_no_holdout_access(provider: FakeMarketDataProvider) -> None:
    holdout_start = pd.Timestamp("2024-01-01")
    holdout_end = pd.Timestamp("2025-12-31")
    for _, start, end in provider.accessed_ranges:
        assert not (pd.Timestamp(start) <= holdout_end and pd.Timestamp(end) >= holdout_start)
    for date in provider.universe_dates:
        assert not (holdout_start <= pd.Timestamp(date) <= holdout_end)


def test_resume_from_checkpoint_is_continuous(tmp_path: Path) -> None:
    resume_root = tmp_path / "resume"
    full_root = tmp_path / "full"

    first = run_rl_explore(_small_cfg(resume_root, max_episodes=3), provider=FakeMarketDataProvider(), mode="run")
    assert first["stop_reason"] == "max_episodes"

    resumed = run_rl_explore(_small_cfg(resume_root, max_episodes=5), provider=FakeMarketDataProvider(), mode="resume")
    full = run_rl_explore(_small_cfg(full_root, max_episodes=5, force_new=True), provider=FakeMarketDataProvider(), mode="run")
    assert resumed["stop_reason"] == "max_episodes"
    assert full["stop_reason"] == "max_episodes"

    resumed_state = _latest_state(resume_root)
    full_state = _latest_state(full_root)
    assert resumed_state["episode"] == full_state["episode"] == 5
    assert resumed_state["step"] == full_state["step"]
    assert resumed_state["rng_state"] == full_state["rng_state"]
    assert resumed_state["agent_state"]["episodes_seen"] == full_state["agent_state"]["episodes_seen"]
    assert np.allclose(resumed_state["agent_state"]["weights"], full_state["agent_state"]["weights"])
    assert resumed_state["agent_state"]["cash_bias"] == pytest.approx(full_state["agent_state"]["cash_bias"])
    assert resumed_state["best_policy"]["validation_terminal_wealth"] == pytest.approx(
        full_state["best_policy"]["validation_terminal_wealth"]
    )


def test_atomic_checkpoint_no_partial(tmp_path: Path) -> None:
    checkpoint_dir = tmp_path / "checkpoints"
    good_state = {"schema": "test", "episode": 1, "step": 7, "value": "good"}
    write_checkpoint(checkpoint_dir, good_state, keep=5)

    with pytest.raises(RuntimeError):
        write_checkpoint(checkpoint_dir, {"schema": "test", "episode": 2, "step": 8, "value": "partial"}, keep=5, fail_after_temp_write=True)

    corrupt = checkpoint_dir / "checkpoint_00000003.json"
    corrupt.write_text('{"schema":"rl_explore_checkpoint_wrapper.v1","checksum":"bad","payload":{"episode":3}}', encoding="utf-8")
    state, checkpoint = load_latest_good_checkpoint(checkpoint_dir)
    assert checkpoint == checkpoint_dir / "checkpoint_00000001.json"
    assert state == {**good_state, "checkpoint_written_at": state["checkpoint_written_at"]}


def test_holdout_never_touched(tmp_path: Path) -> None:
    provider = FakeMarketDataProvider()
    summary = run_rl_explore(_small_cfg(tmp_path / "holdout", max_episodes=2), provider=provider, mode="run")
    assert summary["holdout_untouched"] is True
    _assert_no_holdout_access(provider)

    status = rl_explore_status(tmp_path / "holdout")
    assert status["holdout_untouched"] is True
    assert status["snapshot_hash"] == DEFAULT_SNAPSHOT_HASH

    env = RLMarketEnv(provider=FakeMarketDataProvider(), config=RLMarketEnvConfig(train_start="2020-01-01", top_k=2))
    with pytest.raises(ValueError, match="quarantined"):
        env.run_episode(PermaTwoAgent(), start="2024-01-02", end="2024-02-01", train=False)


def test_env_is_faithful() -> None:
    event = SimpleNamespace(
        permaticker=2,
        date=pd.Timestamp("2020-02-03"),
        value=0.0,
        source="actions_failure_default_zero",
        reason="bankruptcy",
    )
    provider = FakeMarketDataProvider(terminal_event=event)
    env = RLMarketEnv(
        provider=provider,
        config=RLMarketEnvConfig(
            train_start="2020-01-01",
            train_end="2020-12-31",
            validation_start="2021-01-01",
            validation_end="2021-12-31",
            top_k=2,
            universe_top_n=3,
            rebalance_every_days=5,
            lookback_days=10,
        ),
    )

    result = env.run_episode(PermaTwoAgent(), start="2020-01-02", end="2020-03-31", train=True)

    assert result.costs_paid > 0.0
    assert result.terminal_events_used == [
        {
            "permaticker": 2,
            "date": "2020-02-03",
            "value": 0.0,
            "source": "actions_failure_default_zero",
            "reason": "bankruptcy",
        }
    ]
    assert pd.Timestamp(result.max_state_date) <= pd.Timestamp(result.end)
    assert result.holdout_accessed is False
    for _, start, end in provider.accessed_ranges:
        assert pd.Timestamp(start) >= pd.Timestamp("2020-01-01")
        assert pd.Timestamp(end) <= pd.Timestamp("2020-03-31")


def test_budgets_checkpoint_then_stop(tmp_path: Path) -> None:
    episode_root = tmp_path / "episode"
    episode_summary = run_rl_explore(_small_cfg(episode_root, max_episodes=1), provider=FakeMarketDataProvider(), mode="run")
    assert episode_summary["stop_reason"] == "max_episodes"
    assert _latest_state(episode_root)["episode"] == 1
    run_rl_explore(_small_cfg(episode_root, max_episodes=2), provider=FakeMarketDataProvider(), mode="resume")
    assert _latest_state(episode_root)["episode"] == 2

    step_root = tmp_path / "step"
    step_summary = run_rl_explore(_small_cfg(step_root, max_steps=1), provider=FakeMarketDataProvider(), mode="run")
    assert step_summary["stop_reason"] == "max_steps"
    assert _latest_state(step_root)["episode"] == 1
    run_rl_explore(_small_cfg(step_root, max_episodes=2), provider=FakeMarketDataProvider(), mode="resume")
    assert _latest_state(step_root)["episode"] == 2

    wall_root = tmp_path / "wall"
    wall_summary = run_rl_explore(_small_cfg(wall_root, max_wall_clock=0), provider=FakeMarketDataProvider(), mode="run")
    assert wall_summary["stop_reason"] == "max_wall_clock"
    assert _latest_state(wall_root)["episode"] == 0
    run_rl_explore(_small_cfg(wall_root, max_episodes=1), provider=FakeMarketDataProvider(), mode="resume")
    assert _latest_state(wall_root)["episode"] == 1
