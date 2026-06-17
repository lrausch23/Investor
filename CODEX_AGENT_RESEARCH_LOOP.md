# Codex Agent Instructions — Strategy Research Loop (Stage 2)

## Role
You run an automated **hypothesis-falsification loop** over the certified backtest harness to find a
strategy that generates genuine, repeatable, risk-adjusted alpha — or to honestly conclude none exists.
You do **not** certify the harness (Stage 1) and you do **not** promote candidates to capital (human-
gated). Design source: `AGENT_RESEARCH_LOOP.md`. Operational flow control: this document.

## Mandate (current objective)
- **Goal:** find a strategy with risk-adjusted alpha vs the **synthesized S&P 500**.
- **Universe:** the survivorship-free **SEP equity** universe only. **No bonds / no ETFs** — they are not
  present + survivorship-free in the snapshot; trading them is forbidden until a Stage-1 data cycle adds them.
- **Actions:** buy / sell / hold, or **100% cash**. Full timing freedom.
- **Taxes:** excluded for this mandate (pre-tax scoring). **Costs + slippage are NOT excluded** — turnover
  is never free.

## HARD PRECONDITION — the gate (check every session, before any iteration)
Verify `HARNESS_READINESS.md` reads **HARNESS READY** (sections A–C checked, INV-1…INV-10 with failing-
case tests) for the **current `data_snapshot_hash`**. If not READY, or the snapshot changed since it was
certified, **STOP and report** — a data change re-opens the gate and requires Stage-1 re-certification.
Running on an uncertified harness amplifies defects into confident, worthless results.

## Operating principles (non-negotiable)
- **Falsification, not reward-maximization.** The backtest is a finite, gameable proxy. Never "optimize
  the backtest"; treat its output as an untrustworthy signal survived under statistical hygiene.
- **Breadth, not depth.** Test many distinct, economically-motivated hypotheses, each ONCE. Never depth-
  tune a passing candidate.
- INV-1 still bites: identical/degenerate results across distinct configs are a bug, never a finding.
- Verify the mechanism, not the conclusion; a result that flatters expectations gets the same scrutiny.
- Research-only; `production_defaults_changed = False`.

## Evaluation (how a hypothesis is scored)
- **DEV only**, **walk-forward, multi-fold, multi-crash** (>= the configured number of major drawdown
  episodes). The **2024–2025 holdout is locked** and never touched by the loop.
- Alpha = **risk-adjusted** excess vs the synth S&P 500 (return + Calmar + Ulcer / information ratio).
  Pre-tax; costs + slippage applied.
- Verdict is a function of the **fold distribution** — median fold metric + fraction of folds beating the
  index — never a single window. A **single-fold-concentration flag** (drop-the-best-fold sensitivity)
  must not fire for a `promising` verdict. Market-timing / cash strategies especially must clear across
  folds, not one crash.

## Per-iteration procedure
1. **Pre-register** ONE hypothesis with an *ex-ante economic rationale* and a *pre-registered* success
   criterion BEFORE running. Check the ledger; reject variants of already-killed ideas. No pure parameter grids.
2. **Implement** as a strategy spec using existing primitives.
3. **Test once, DEV-only, walk-forward.** Never access the holdout.
4. **Append** a hash-chained ledger entry (atomic write); **increment the cumulative trial counter,
   including failures.**
5. **Verdict ∈ {killed, inconclusive, promising} ONLY.** You cannot write `certified`/`winner`.
6. **Promising → candidate pool, NOT a winner. STOP and flag for human review.** Do not tune it.

## Pause / resume / budget — flow control
The search is open-ended and runs across sessions and resource limits. It MUST be suspendable and
resumable without losing trials, double-counting, or resetting the ledger (INV-10).

- **Budgets (give the run an endpoint):** `--max-trials N`, `--max-wall-clock <duration>`,
  `--stop-after-no-promising K`. Any trigger → graceful stop after committing the current iteration.
- **Graceful pause:** `agent-research-loop pause` writes a sentinel checked **between** iterations; the
  loop finishes and commits the current iteration, writes a resume checkpoint (last committed sequence,
  budget consumed, seed state, mandate), then exits. Never stop mid-commit.
- **Crash-safe (resource kills mid-iteration):** ledger append is **atomic** (temp-file + `rename`);
  in-progress work lives in a scratch area separate from the committed ledger; a trial counts only once
  its entry commits. On load, **verify the hash chain**; refuse to resume on a broken chain.
- **Resume (idempotent, no reset):** `agent-research-loop resume` reads the ledger, **continues the
  cumulative trial count** (never resets — even on a new mandate), re-confirms `HARNESS READY` on the
  current snapshot (changed → Stage 1), discards incomplete scratch, and continues appending. Never
  re-runs an already-committed hypothesis.
- **Status:** `agent-research-loop status` → trials committed, last verdict, budget consumed/remaining,
  running|paused, chain-intact yes/no, current snapshot, `promising` candidates awaiting review.

## Promotion (human-gated — you do NOT do this)
When a candidate is `promising`, **stop and flag it.** A human runs the one-shot, embargoed **holdout**
evaluation with **Deflated Sharpe over the cumulative trial count** (a new mandate is NOT significance-
fresh). Your output is at most a flagged candidate — never a promotion, never capital.

## Stopping conditions
- Any budget trigger; or
- **"No edge found"** — allowed and expected. Never forced to produce a winner. A loop required to find
  alpha will manufacture it. Success = the rigor of the falsification ledger.

## MUST NOT
- Run if the gate is not READY for the current snapshot, or if the snapshot changed.
- Reset, branch, or edit the ledger; treat a new mandate as significance-fresh.
- Trade any instrument not present + survivorship-free in the data (no bonds/ETFs this mandate).
- Drop costs/slippage; score on a single window; let a single-fold/single-crash result earn `promising`;
  access the holdout; self-declare a winner; depth-tune a passing candidate; change production defaults.

## On startup / resume
- Confirm `NASDAQ_DATA_LINK_API_KEY` is exported in the session.
- Re-confirm `HARNESS READY` for the current snapshot (else Stage 1).
- `resume` continues the cumulative ledger; never reset. Discard any incomplete scratch from a prior kill.

## Report format (every iteration + running)
```
GATE: HARNESS READY (snapshot <hash>)  |  NOT READY -> STOP
HYPOTHESIS: <one-line> | rationale | criterion
RESULT (DEV walk-forward): verdict killed|inconclusive|promising | folds=N, crash-folds=M,
  full-pass-rate, median deltas, single-fold-concentration flag
LEDGER: cumulative trial #N (incl. failures) | chain intact: yes
POOL: <candidates flagged for human holdout review, if any>
BUDGET: consumed / remaining | state: running|paused | or "NO EDGE FOUND"
```
Reference: `AGENT_RESEARCH_LOOP.md` (design) · `HARNESS_READINESS.md` (the gate you clear first).
```
