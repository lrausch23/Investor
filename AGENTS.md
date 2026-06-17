# AGENTS.md

**Before any backtesting / strategy-research work, load `HARNESS_READINESS.md` (the definition-of-done gate) plus the charter for the stage you are in: `CODEX_AGENT_HARNESS_READINESS.md` (Stage 1 — certify the harness) or `CODEX_AGENT_RESEARCH_LOOP.md` (Stage 2 — run the research loop). Load these as standing context.**

Always-on guardrails (these never lapse, even if the linked docs aren't opened):

- Research-only: never change production trading defaults; assert `production_defaults_changed = False`.
- The **research/strategy loop MUST NOT start** until `HARNESS_READINESS.md` is all-green (`HARNESS READY`).
- **INV-1:** genuinely different strategy configs must produce different results — identical/degenerate arms are always a bug, never a finding.
- Fail-closed: no run reads `certifiable` without `survivorship_free` readiness AND an EDGAR PASS bound to the current snapshot.
- Reality guards are fixed (survivorship-free + point-in-time data, tradeable-liquidity participation, costs + slippage, mechanical rules only) — never relaxed to make something pass. Tax treatment is mandate-specific (the current Stage-2 mandate is pre-tax); costs/slippage are never dropped.
- **The trial ledger is immutable (INV-10): never reset, branch, or edit it — not even on a new mandate or a "clean slate."** Pause/resume continues the cumulative trial count; deflated-significance uses that cumulative count.
- **No multi-trial batch run until the canonical flow-control runner is built and its tests are green** (`run | resume | pause | status`, the three budgets, and crash-safe **atomic, committed-only** ledger appends — a kill mid-trial must lose only scratch, never corrupt the ledger). **Never improvise a throwaway bounded runner** — there is exactly one canonical runner path; single-iteration commands are thin wrappers over it.
- Verify the mechanism, not just the conclusion; a result that flatters expectations gets the same scrutiny as one that doesn't.

## Stage 2 mandate (current)
Find a strategy with risk-adjusted alpha vs the synthesized S&P 500, on the **survivorship-free SEP equity universe only** (no bonds/ETFs — not in the data). Actions: buy/sell/hold or 100% cash. Pre-tax; costs kept. Scored DEV-only under **walk-forward, multi-fold, multi-crash** evaluation with the single-fold-concentration flag; the 2024–2025 holdout is locked. Promotion is human-gated (one-shot holdout + Deflated Sharpe on the cumulative trial count). The loop is allowed to conclude "no edge found."

Operational control: one canonical runner `agent-research-loop run | resume | pause | status` with `--max-trials`, `--max-wall-clock`, `--stop-after-no-promising` budgets. Resume re-confirms `HARNESS READY` on the current snapshot, continues the cumulative ledger (never resets), and is crash-safe (atomic, committed-only, hash-chained appends). **This runner must be built and tested green before any batch run** — do not improvise a one-off bounded runner. A `promising` verdict halts the batch and flags for human review.

Stage 1 = `CODEX_AGENT_HARNESS_READINESS.md` (certify the environment). Stage 2 = `CODEX_AGENT_RESEARCH_LOOP.md` (the complete research-loop charter incl. mandate + pause/resume; design spec in `AGENT_RESEARCH_LOOP.md`) — run only after Stage 1 reports READY for the current snapshot. A snapshot change re-opens the gate and sends you back to Stage 1.
