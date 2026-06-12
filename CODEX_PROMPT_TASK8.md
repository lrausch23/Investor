# Codex Prompt — Task 8: Complete the Real Module Split

You are working in the `Investor/` Python project. Tasks 1–7 are merged. This task finishes the one structural item still open from `REGIME_REVIEW.md` §4.x.

## Problem

Task 7 delivered the package *facade* but not the split. Current state:

- `src/regime/persistence/core.py` — **3,937 lines** (the entire old monolith, slightly grown)
- `src/regime/paper_trading/core.py` — **2,719 lines** (same)
- Every domain module (`persistence/plans.py`, `positions.py`, `portfolios.py`, `snapshots.py`, `settings.py`, `audit.py`, `signals_cache.py`; `paper_trading/planning.py`, `execution.py`, `performance.py`, `sizing.py`) is a 3-line `from .core import *` shim — the **inverse** of the intended structure.
- `src/regime/persistence.py` and `src/regime/paper_trading.py` are 4-line compatibility markers; imports resolve to the packages.

The goal: code actually lives in the domain modules; `core.py` shrinks to connection/migration/shared helpers (persistence) and shared utilities/config plumbing (paper_trading). Pure mechanical moves — **zero behavior change**.

## Critical trap — investigate BEFORE moving anything

The test fixtures reload and monkeypatch these modules. Example (`tests/regime/test_beta_target_deployment.py::temp_modules`):

```python
store = importlib.reload(persistence_module)
monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
paper = importlib.reload(paper_trading_module)
```

Today this works because everything lives in one module namespace. After the split, a function moved to `plans.py` that resolves `DB_PATH` or `_connect` from `core`'s globals will NOT see an attribute patched onto the *package*. Before writing any code:

1. Read how `DB_PATH` / `_connect` are defined and consumed in `persistence/core.py` and how `__init__.py` re-exports them.
2. Read every test fixture that does `importlib.reload` + `monkeypatch.setattr` on these modules (`grep -rn "reload(persistence\|reload(paper" tests/`).
3. Design the connection seam so both keep working. The recommended pattern: `DB_PATH` and `_connect()` live ONLY in `core.py`; every domain module calls `core._connect()` (module-attribute access at call time, never `from .core import _connect` which freezes a reference); the package `__init__` exposes `DB_PATH` via a module-level `__getattr__`/`__setattr__` passthrough to `core`, OR the fixtures are updated to patch `persistence_module.core.DB_PATH` — pick one, apply it consistently, and add a regression test that proves a patched `DB_PATH` is honored by a function in every domain module.

Same concern in `paper_trading`: functions monkeypatched by tests (`paper.get_watchlist`, `paper._batch_current_prices`, `paper._lookup_atr`, `paper._lookup_beta`, etc.) are patched **on the package**. Calls between domain modules must resolve through the same namespace the tests patch, or those tests will silently stop intercepting. Audit every `monkeypatch.setattr(paper, ...)` in the test suite and ensure the internal call sites resolve through the patched attribute (e.g. domain modules call `from src.regime import paper_trading as pt; pt._lookup_atr(...)` — or keep such seams in `core` and have callers go through `core`). Add a test that proves each commonly-patched seam still intercepts after the split.

## Target layout

### `src/regime/persistence/`
- `core.py`: DB path/connection, schema creation, column-migration dicts (`_PAPER_TRADE_PLAN_COLUMNS` etc.), generic row/serialization helpers. Target **< 600 lines**.
- `plans.py`: trade-plan CRUD (`create_trade_plan`, `get_trade_plans`, plan status updates, expiry).
- `positions.py`: paper positions (open/close/update, risk updates incl. `update_paper_position_risk`).
- `portfolios.py`: paper portfolios (create/get/update/summary/list).
- `snapshots.py`: daily snapshots + signal snapshots (`save_signal_snapshot`, `get_latest_signal_snapshot`, `get_daily_snapshots`).
- `settings.py`: `get_setting`/`set_setting` and settings helpers.
- `audit.py`: audit trail, alerts (`save_alert`, `get_audit_trail`), LLM attribution storage.
- `signals_cache.py`: regime cache payload load/save, sentiment history, watchlist (`get_watchlist`, `upsert_watchlist_candidate`) — if watchlist is large, a separate `watchlist.py` is fine.
- Anything that genuinely doesn't fit: `misc.py` is acceptable; do not force it into `core.py`.

### `src/regime/paper_trading/`
- `core.py`: shared config access (`get_sizing_settings`, `get_hurdle_settings` accessors), price/ATR/beta lookup seams, small shared helpers (`_positive_float`, `_now`, timestamp parsing). Target **< 500 lines**.
- `sizing.py`: `_risk_adjusted_quantity`, `allocate_budget`, `compute_theme_budget`, `compute_position_budget`, ML size scaling helper.
- `planning.py`: `generate_buy_plans`, `generate_holdings_plans`, `generate_exit_plans`, `generate_daily_plans`, trade geometry (`_actual_fill_trade_geometry`, `_timeframe_days_from_sources`), exit helpers (`trailing_stop_level`, `_ratchet_trailing_stop`, `_neutral_reduce_reason`, `_reduced_exit_quantity`, time-stop helpers).
- `execution.py`: approval/execution (`auto_approve_plans`, `execute_approved_plans*`, `auto_execute_approved`, `cancel_submitted_orders_by_policy`, `expire_stale_plans`, `kill_switch`, `_apply_filled_execution`).
- `performance.py`: `compute_paper_performance`, `compute_daily_snapshot`, `compute_benchmark_*`, `compute_beta_target_progress`, `record_trade_outcome`, `estimate_after_tax_performance`, `get_paper_dashboard`.

`__init__.py` for both packages: replace `from .core import *` with **explicit imports from each domain module**, preserving every currently-public name (including the underscore-prefixed ones tests import: `_risk_adjusted_quantity`, `_neutral_reduce_reason`, `_actual_fill_trade_geometry`, `_batch_current_prices`, `_lookup_atr`, `_lookup_beta`, ...). Build the canonical name list FIRST: `python -c "import src.regime.paper_trading as m; print(sorted(n for n in dir(m) if not n.startswith('__')))"` before any change, and pin it in a test (see below).

## Method — strict

1. **Snapshot the public API**: write `tests/regime/test_module_split_compat.py` FIRST. It must contain the pinned pre-split name lists for both packages and assert every name is importable from the package root, plus identity checks for cross-module objects (e.g. `pipeline_backtest.trailing_stop_level is paper_trading.trailing_stop_level`) and the monkeypatch-seam tests from the trap section. Run it green against the CURRENT code before moving anything.
2. Move **one domain module at a time**, in this order (lowest-risk first): persistence `settings` → `audit` → `snapshots` → `portfolios` → `positions` → `plans` → `signals_cache`; then paper_trading `sizing` → `performance` → `planning` → `execution`. After EACH move: full suite (`python -m pytest -q` — it runs clean as of the IBKR fix) + `scripts/typecheck.sh`. Commit after each green run with message `split(<package>): move <domain> out of core`.
3. Moves must be **byte-identical function bodies** — no renames, no signature changes, no formatting churn, no "while I'm here" fixes. If you find a bug mid-move, leave a `# TODO` comment and report it; do not fix it in this task.
4. Inter-domain references: prefer importing the *function's new home module* (`from . import plans` then `plans.create_trade_plan(...)`) over `from .plans import create_trade_plan`, except where a frozen reference is harmless (pure functions never monkeypatched). When in doubt, late-bind.
5. Circular imports will appear (e.g. planning ↔ execution sharing helpers). Resolve by moving the shared helper to `core.py`, never by function-local imports added in new places (existing function-local imports may stay as-is).
6. Do not touch: `pipeline_backtest.py` internals (only its imports if needed), exit-ladder semantics, any `src/app/routes` logic, test files other than the new compat test and mechanical fixture updates explicitly required by the DB_PATH decision (if you update fixtures, update ALL of them the same way and say so in the PR).

## Definition of done

1. `persistence/core.py` < 600 lines, `paper_trading/core.py` < 500 lines; every domain module contains real code; no `import *` anywhere in either package.
2. `tests/regime/test_module_split_compat.py` passes: full pinned-name imports, object identity, and monkeypatch-seam interception for `DB_PATH`, `get_watchlist`, `_batch_current_prices`, `_lookup_atr`, `_lookup_beta` (each proven from at least one function in a *different* domain module).
3. Full suite `python -m pytest -q` green in one command; `scripts/typecheck.sh` clean.
4. One commit per domain move, each individually green (state this in the PR; the reviewer will spot-check by checking out intermediate commits).
5. PR description: final line counts per module, the DB_PATH seam decision and why, any TODOs found during moves, and confirmation that no function body changed (e.g. via `git diff --color-moved=dimmed-zebra` review).
