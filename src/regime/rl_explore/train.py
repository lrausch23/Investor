from __future__ import annotations

import hashlib
import json
import math
import os
import time
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

import numpy as np

from .agent import RLAgentConfig, SoftmaxLinearAgent
from .env import (
    DEFAULT_SNAPSHOT_HASH,
    UNVALIDATED_LABEL,
    MarketDataProvider,
    RLMarketEnv,
    RLMarketEnvConfig,
)

DEFAULT_RL_EXPLORE_DIR = Path("data") / "rl_explore"
CHECKPOINT_DIR_NAME = "checkpoints"
SCRATCH_DIR_NAME = "scratch"
PAUSE_SENTINEL_NAME = "pause.requested"
STATUS_FILE_NAME = "status.json"
SUMMARY_FILE_NAME = "run_summary.json"


@dataclass(frozen=True)
class RLExploreConfig:
    output_dir: str | Path = DEFAULT_RL_EXPLORE_DIR
    snapshot_hash: str = DEFAULT_SNAPSHOT_HASH
    seed: int = 17
    success_margin: float = 0.05
    checkpoint_every_episodes: int = 5
    checkpoint_every_steps: int = 1_000
    checkpoint_every_minutes: float = 5.0
    keep_checkpoints: int = 5
    validation_every_episodes: int = 5
    replay_buffer_size: int = 200
    max_steps: int | None = None
    max_episodes: int | None = None
    max_wall_clock: str | float | int | None = None
    force_new: bool = False
    env: RLMarketEnvConfig = field(default_factory=RLMarketEnvConfig)
    agent: RLAgentConfig = field(default_factory=RLAgentConfig)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["output_dir"] = str(self.output_dir)
        return payload


def run_rl_explore(
    config: RLExploreConfig | None = None,
    *,
    provider: MarketDataProvider | None = None,
    mode: str = "run",
) -> dict[str, Any]:
    cfg = config or RLExploreConfig()
    root = Path(cfg.output_dir)
    checkpoint_dir = root / CHECKPOINT_DIR_NAME
    scratch_dir = root / SCRATCH_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    pause_sentinel = root / PAUSE_SENTINEL_NAME
    if mode not in {"run", "resume"}:
        raise ValueError("mode must be run or resume.")
    env_cfg = replace(cfg.env, snapshot_hash=cfg.snapshot_hash)
    env = RLMarketEnv(provider=provider, config=env_cfg)
    actual_snapshot = getattr(env.provider, "data_snapshot_hash", None)
    if actual_snapshot and str(actual_snapshot) != str(cfg.snapshot_hash):
        raise ValueError("RL exploration snapshot mismatch; refusing to train on changed data.")
    if mode == "resume":
        state, checkpoint_path = load_latest_good_checkpoint(checkpoint_dir)
        if state is None:
            raise ValueError("No valid RL exploration checkpoint found to resume.")
        rng = np.random.default_rng()
        rng.bit_generator.state = state["rng_state"]
        agent = SoftmaxLinearAgent.from_state(dict(state["agent_state"]), rng=rng)
    else:
        if cfg.force_new:
            _remove_checkpoints(checkpoint_dir)
        elif any(checkpoint_dir.glob("checkpoint_*.json")):
            state, checkpoint_path = load_latest_good_checkpoint(checkpoint_dir)
            if state is not None:
                return {
                    "schema": "rl_explore_run_result.v1",
                    "label": UNVALIDATED_LABEL,
                    "state": "already_initialized",
                    "message": "Existing checkpoint found. Use resume or --force-new.",
                    "latest_checkpoint_path": str(checkpoint_path),
                    "production_defaults_changed": False,
                }
        rng = np.random.default_rng(int(cfg.seed))
        agent = SoftmaxLinearAgent(cfg.agent, rng=rng)
        state = _initial_state(cfg, agent, rng)
        checkpoint_path = write_checkpoint(checkpoint_dir, state, keep=cfg.keep_checkpoints)
    wall_budget = _parse_duration_seconds(cfg.max_wall_clock)
    started = time.monotonic()
    last_checkpoint_time = time.monotonic()
    stop_reason: str | None = None
    latest_checkpoint_path = checkpoint_path
    _write_status(root, state, latest_checkpoint_path=latest_checkpoint_path, state_name="running")
    while True:
        if pause_sentinel.exists():
            stop_reason = "paused"
            break
        if cfg.max_episodes is not None and int(state["episode"]) >= int(cfg.max_episodes):
            stop_reason = "max_episodes"
            break
        if cfg.max_steps is not None and int(state["step"]) >= int(cfg.max_steps):
            stop_reason = "max_steps"
            break
        if wall_budget is not None and time.monotonic() - started >= wall_budget:
            stop_reason = "max_wall_clock"
            break
        agent.begin_episode(rng, train=True)
        episode_start, episode_end = env.sample_episode_window(rng)
        result = env.run_episode(agent, start=episode_start, end=episode_end, rng=rng, train=True)
        learn = agent.learn_from_episode(result.terminal_log_wealth)
        state["episode"] = int(state["episode"]) + 1
        state["step"] = int(state["step"]) + int(result.steps)
        state["agent_state"] = agent.to_state()
        state["rng_state"] = rng.bit_generator.state
        state["holdout_accessed"] = bool(state.get("holdout_accessed")) or bool(result.holdout_accessed)
        _append_replay(
            state,
            {
                "episode": state["episode"],
                "start": result.start,
                "end": result.end,
                "terminal_wealth": result.terminal_wealth,
                "terminal_log_wealth": result.terminal_log_wealth,
                "steps": result.steps,
                "costs_paid": result.costs_paid,
                "turnover": result.turnover,
                "learn": learn,
            },
            max_size=cfg.replay_buffer_size,
        )
        if int(state["episode"]) % max(1, int(cfg.validation_every_episodes)) == 0:
            validation = validate_policy(env, agent, cfg)
            state["last_validation"] = validation
            _update_best_policy(state, agent, validation)
            if bool(validation.get("success")):
                state["success_found"] = True
                stop_reason = "success"
        should_checkpoint = _checkpoint_due(
            state,
            cfg=cfg,
            last_checkpoint_time=last_checkpoint_time,
        )
        if should_checkpoint or stop_reason is not None:
            latest_checkpoint_path = write_checkpoint(checkpoint_dir, state, keep=cfg.keep_checkpoints)
            state["last_checkpoint_episode"] = int(state["episode"])
            state["last_checkpoint_step"] = int(state["step"])
            last_checkpoint_time = time.monotonic()
            _write_status(root, state, latest_checkpoint_path=latest_checkpoint_path, state_name="running")
        if stop_reason is not None:
            break
    latest_checkpoint_path = write_checkpoint(checkpoint_dir, state, keep=cfg.keep_checkpoints)
    if pause_sentinel.exists():
        pause_sentinel.unlink(missing_ok=True)
    summary = {
        "schema": "rl_explore_run_result.v1",
        "label": UNVALIDATED_LABEL,
        "generated_at": _now_iso(),
        "state": "paused" if stop_reason == "paused" else "success" if stop_reason == "success" else "stopped",
        "stop_reason": stop_reason or "unknown",
        "episode": state.get("episode"),
        "step": state.get("step"),
        "best_policy": state.get("best_policy"),
        "latest_checkpoint_path": str(latest_checkpoint_path),
        "holdout_untouched": not bool(state.get("holdout_accessed")),
        "holdout_window": {"start": cfg.env.holdout_start, "end": cfg.env.holdout_end, "accessed": bool(state.get("holdout_accessed"))},
        "snapshot_hash": cfg.snapshot_hash,
        "run_summary_note": (
            "In-sample/internal-validation success is expected to overfit on this data volume; "
            "this is a generated hypothesis, not a validated edge. The quarantined 2024-2025 holdout "
            "is the eventual judge, run by a human."
        ),
        "production_defaults_changed": False,
    }
    _write_json_atomic(root / SUMMARY_FILE_NAME, summary)
    _write_status(root, state, latest_checkpoint_path=latest_checkpoint_path, state_name=str(summary["state"]))
    return summary


def validate_policy(env: RLMarketEnv, agent: SoftmaxLinearAgent, cfg: RLExploreConfig) -> dict[str, Any]:
    result = env.run_episode(agent, start=cfg.env.validation_start, end=cfg.env.validation_end, train=False)
    benchmark = env.benchmark_terminal_wealth(start=cfg.env.validation_start, end=cfg.env.validation_end)
    success = result.terminal_wealth >= benchmark * (1.0 + float(cfg.success_margin))
    return {
        "label": UNVALIDATED_LABEL,
        "validation_start": cfg.env.validation_start,
        "validation_end": cfg.env.validation_end,
        "terminal_wealth": result.terminal_wealth,
        "terminal_log_wealth": result.terminal_log_wealth,
        "benchmark_terminal_wealth": benchmark,
        "margin": result.terminal_wealth / benchmark - 1.0 if benchmark > 0 else None,
        "success_margin": cfg.success_margin,
        "success": bool(success),
        "holdout_accessed": result.holdout_accessed,
    }


def pause_rl_explore(output_dir: str | Path = DEFAULT_RL_EXPLORE_DIR) -> dict[str, Any]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    sentinel = root / PAUSE_SENTINEL_NAME
    sentinel.write_text(_now_iso() + "\n", encoding="utf-8")
    return {
        "schema": "rl_explore_pause_request.v1",
        "label": UNVALIDATED_LABEL,
        "sentinel": str(sentinel),
        "state": "pause_requested",
        "production_defaults_changed": False,
    }


def rl_explore_status(output_dir: str | Path = DEFAULT_RL_EXPLORE_DIR) -> dict[str, Any]:
    root = Path(output_dir)
    checkpoint_dir = root / CHECKPOINT_DIR_NAME
    state, checkpoint_path = load_latest_good_checkpoint(checkpoint_dir)
    status = _safe_json(root / STATUS_FILE_NAME)
    pause_requested = (root / PAUSE_SENTINEL_NAME).exists()
    if state is None:
        return {
            "schema": "rl_explore_status.v1",
            "label": UNVALIDATED_LABEL,
            "state": "not_started",
            "pause_requested": pause_requested,
            "latest_checkpoint_path": None,
            "holdout_untouched": True,
            "production_defaults_changed": False,
        }
    latest_validation = state.get("last_validation") or {}
    best = state.get("best_policy") or {}
    return {
        "schema": "rl_explore_status.v1",
        "label": UNVALIDATED_LABEL,
        "state": status.get("state") or "idle",
        "pause_requested": pause_requested,
        "step": state.get("step"),
        "episode": state.get("episode"),
        "best_validation_terminal_wealth": best.get("validation_terminal_wealth"),
        "best_benchmark_terminal_wealth": best.get("benchmark_terminal_wealth"),
        "best_validation_margin": best.get("margin"),
        "success_found": bool(state.get("success_found")),
        "latest_validation": latest_validation,
        "latest_checkpoint_path": str(checkpoint_path) if checkpoint_path else None,
        "holdout_untouched": not bool(state.get("holdout_accessed")),
        "snapshot_hash": state.get("snapshot_hash"),
        "production_defaults_changed": False,
    }


def write_checkpoint(
    checkpoint_dir: str | Path,
    state: dict[str, Any],
    *,
    keep: int = 5,
    fail_after_temp_write: bool = False,
) -> Path:
    path = Path(checkpoint_dir)
    path.mkdir(parents=True, exist_ok=True)
    sequence = int(state.get("episode") or 0)
    target = path / f"checkpoint_{sequence:08d}.json"
    payload = dict(state)
    payload["checkpoint_written_at"] = _now_iso()
    encoded_payload = json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    wrapper = {
        "schema": "rl_explore_checkpoint_wrapper.v1",
        "checksum": hashlib.sha256(encoded_payload).hexdigest(),
        "payload": _json_safe(payload),
    }
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(wrapper, sort_keys=True, indent=2), encoding="utf-8")
    if fail_after_temp_write:
        raise RuntimeError("simulated checkpoint failure after temp write")
    os.replace(tmp, target)
    _prune_checkpoints(path, keep=keep)
    return target


def load_latest_good_checkpoint(checkpoint_dir: str | Path) -> tuple[dict[str, Any] | None, Path | None]:
    path = Path(checkpoint_dir)
    if not path.exists():
        return None, None
    for checkpoint in sorted(path.glob("checkpoint_*.json"), reverse=True):
        try:
            wrapper = json.loads(checkpoint.read_text(encoding="utf-8"))
            payload = dict(wrapper.get("payload") or {})
            encoded_payload = json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
            expected = hashlib.sha256(encoded_payload).hexdigest()
            if str(wrapper.get("checksum") or "") != expected:
                continue
            return payload, checkpoint
        except Exception:
            continue
    return None, None


def _initial_state(cfg: RLExploreConfig, agent: SoftmaxLinearAgent, rng: np.random.Generator) -> dict[str, Any]:
    return {
        "schema": "rl_explore_training_state.v1",
        "label": UNVALIDATED_LABEL,
        "generated_at": _now_iso(),
        "snapshot_hash": cfg.snapshot_hash,
        "config": cfg.to_dict(),
        "agent_state": agent.to_state(),
        "rng_state": rng.bit_generator.state,
        "episode": 0,
        "step": 0,
        "last_checkpoint_episode": 0,
        "last_checkpoint_step": 0,
        "replay_buffer": [],
        "best_policy": None,
        "last_validation": None,
        "success_found": False,
        "holdout_accessed": False,
        "production_defaults_changed": False,
    }


def _update_best_policy(state: dict[str, Any], agent: SoftmaxLinearAgent, validation: dict[str, Any]) -> None:
    current = state.get("best_policy") or {}
    wealth = _float(validation.get("terminal_wealth"), default=-math.inf)
    best = _float(current.get("validation_terminal_wealth"), default=-math.inf)
    if wealth > best:
        state["best_policy"] = {
            "label": UNVALIDATED_LABEL,
            "episode": state.get("episode"),
            "step": state.get("step"),
            "agent_state": agent.to_state(),
            "validation_terminal_wealth": validation.get("terminal_wealth"),
            "benchmark_terminal_wealth": validation.get("benchmark_terminal_wealth"),
            "margin": validation.get("margin"),
            "success": validation.get("success"),
        }


def _checkpoint_due(state: dict[str, Any], *, cfg: RLExploreConfig, last_checkpoint_time: float) -> bool:
    episode_delta = int(state.get("episode") or 0) - int(state.get("last_checkpoint_episode") or 0)
    step_delta = int(state.get("step") or 0) - int(state.get("last_checkpoint_step") or 0)
    if episode_delta >= max(1, int(cfg.checkpoint_every_episodes)):
        state["last_checkpoint_episode"] = int(state.get("episode") or 0)
        state["last_checkpoint_step"] = int(state.get("step") or 0)
        return True
    if step_delta >= max(1, int(cfg.checkpoint_every_steps)):
        state["last_checkpoint_episode"] = int(state.get("episode") or 0)
        state["last_checkpoint_step"] = int(state.get("step") or 0)
        return True
    if time.monotonic() - last_checkpoint_time >= max(0.0, float(cfg.checkpoint_every_minutes)) * 60.0:
        state["last_checkpoint_episode"] = int(state.get("episode") or 0)
        state["last_checkpoint_step"] = int(state.get("step") or 0)
        return True
    return False


def _append_replay(state: dict[str, Any], row: dict[str, Any], *, max_size: int) -> None:
    replay = list(state.get("replay_buffer") or [])
    replay.append(row)
    state["replay_buffer"] = replay[-max(1, int(max_size)) :]


def _write_status(root: Path, state: dict[str, Any], *, latest_checkpoint_path: Path | None, state_name: str) -> None:
    best = state.get("best_policy") or {}
    payload = {
        "schema": "rl_explore_status_snapshot.v1",
        "label": UNVALIDATED_LABEL,
        "generated_at": _now_iso(),
        "state": state_name,
        "episode": state.get("episode"),
        "step": state.get("step"),
        "best_validation_terminal_wealth": best.get("validation_terminal_wealth"),
        "success_found": bool(state.get("success_found")),
        "latest_checkpoint_path": str(latest_checkpoint_path) if latest_checkpoint_path else None,
        "holdout_untouched": not bool(state.get("holdout_accessed")),
        "snapshot_hash": state.get("snapshot_hash"),
        "production_defaults_changed": False,
    }
    _write_json_atomic(root / STATUS_FILE_NAME, payload)


def _remove_checkpoints(checkpoint_dir: Path) -> None:
    for file in checkpoint_dir.glob("checkpoint_*.json*"):
        if file.is_file():
            file.unlink()


def _prune_checkpoints(checkpoint_dir: Path, *, keep: int) -> None:
    checkpoints = sorted(checkpoint_dir.glob("checkpoint_*.json"))
    excess = len(checkpoints) - max(1, int(keep))
    for file in checkpoints[: max(0, excess)]:
        file.unlink(missing_ok=True)


def _parse_duration_seconds(value: str | float | int | None) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().lower()
    if text.endswith("ms"):
        return float(text[:-2]) / 1000.0
    if text.endswith("s"):
        return float(text[:-1])
    if text.endswith("m"):
        return float(text[:-1]) * 60.0
    if text.endswith("h"):
        return float(text[:-1]) * 3600.0
    return float(text)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(_json_safe(payload), sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _safe_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    return value


def _float(value: Any, *, default: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        return default
    return parsed if math.isfinite(parsed) else default


def _now_iso() -> str:
    return pd_timestamp_now()


def pd_timestamp_now() -> str:
    # Kept local to avoid importing pandas into checkpoint-only test paths.
    import datetime as dt

    return dt.datetime.now(dt.timezone.utc).isoformat()
