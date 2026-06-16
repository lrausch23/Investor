# AGENTS.md

**Before any backtesting / strategy-research work, load `HARNESS_READINESS.md` (the definition-of-done gate) plus the charter for the stage you are in: `CODEX_AGENT_HARNESS_READINESS.md` (Stage 1 — certify the harness) or `CODEX_AGENT_RESEARCH_LOOP.md` (Stage 2 — run the research loop). Load these as standing context.**

Always-on guardrails (these never lapse, even if the linked docs aren't opened):

- Research-only: never change production trading defaults; assert `production_defaults_changed = False`.
- The **research/strategy loop MUST NOT start** until `HARNESS_READINESS.md` is all-green (`HARNESS READY`).
- **INV-1:** genuinely different strategy configs must produce different results — identical/degenerate arms are always a bug, never a finding.
- Fail-closed: no run reads `certifiable` without `survivorship_free` readiness AND an EDGAR PASS bound to the current snapshot.
- Reality guards are fixed (survivorship-free + point-in-time data, tradeable-liquidity participation, after-tax, costs, mechanical rules only) — never relaxed to make something pass.
- Verify the mechanism, not just the conclusion; a result that flatters expectations gets the same scrutiny as one that doesn't.

Stage 1 = `CODEX_AGENT_HARNESS_READINESS.md` (certify the environment). Stage 2 = `CODEX_AGENT_RESEARCH_LOOP.md` (the research-loop charter; design spec in `AGENT_RESEARCH_LOOP.md`) — run only after Stage 1 reports READY for the current snapshot. A snapshot change re-opens the gate and sends you back to Stage 1.
