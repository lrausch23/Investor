# Agent Research Loop (ARL) — Specification

Status: Draft v1 — research-only orchestration over the existing certification pipeline.
Not investment advice. No production defaults. Built on the Sharadar PIT layer + basket study + tax engine.

## Objective

An automated loop in which Codex proposes and **honestly falsifies** trading-strategy hypotheses, at
breadth, under strict anti-overfitting hygiene — to find genuine, repeatable, after-tax alpha if it
exists, and to conclude "no edge found" if it does not.

## Framing (read first)

This is **hypothesis-falsification, not reward maximization.** The backtest is a finite, gameable proxy
for live markets; "maximize the backtest" = reward hacking = overfitting. The loop therefore treats the
backtest as an *untrustworthy signal survived under statistical hygiene*, never a number to optimize. The
agent's tireless breadth is the asset (many distinct economic hypotheses, rigorous logging); depth-tuning
one idea toward a metric is forbidden.

## Search space — what is FREE vs FIXED

**Free (the agent explores widely):** selection signals/factors, factor definitions, basket size,
weighting, concentration, rebalance cadence, holding/exit rules, sector posture.

**Relaxed (de-bias):** minimum listing history -> reduced to the **signal-formation window** (e.g. ~252
trading days for 12-1 momentum). Young and later-delisted names are eligible. (The old ~10-year filter was
itself the survivorship bias; PIT survivorship-free data lets us drop it.)

**FIXED — reality/bias guards, non-negotiable, the agent may NOT relax these:**
- Survivorship-free, point-in-time data (Sharadar layer); strictly as-of selection (no look-ahead).
- **Tradeable-liquidity participation rule:** any position must be <= `max_participation_pct` of trailing
  ADV (default ~2-3%), sized to deployable capital. This adapts the floor to small capital (fish in
  smaller names than institutions) WITHOUT permitting untradeable microcaps. Not removable.
- After-tax (32% ST / 20% LT, wash-sale aware) and realistic costs + slippage.
- Mechanical rules only (no discretionary/timing exits).

> Principle: survivorship-free + PIT removes *bias*; liquidity + costs + after-tax preserve *realism*.
> Never trade realism away in the name of removing bias.

## Data splits (anti-snooping)

- **DEV** (train + walk-forward CV): all iteration happens here.
- **LOCKED HOLDOUT** (embargoed, agent-blind): a final block the agent NEVER sees during the loop (e.g.
  the most recent N years, plus an embargo gap to prevent leakage). Used at most once per candidate, by a
  human-gated step.
- Stamp `data_snapshot_hash` on every trial.

## The loop (per iteration)

1. **Pre-register:** the agent states ONE hypothesis with an *ex-ante economic rationale* and a
   *pre-registered* success criterion (metric, expected sign, threshold) BEFORE running. No post-hoc
   metric shopping; no pure parameter grids.
2. **Registry dedup:** check the hypothesis registry; reject variants of already-killed ideas.
3. **Implement** the hypothesis as a strategy spec (reuse basket-study / CCEL / TCS primitives).
4. **Test once, DEV only:** run the certification pipeline (PIT, survivorship-free, after-tax, walk-forward
   OOS on DEV). Never touch the holdout.
5. **Record honestly:** verdict in {killed, inconclusive, promising}; append to the registry; **increment a
   global trial counter that INCLUDES failures** (hiding failures corrupts the significance correction).
6. **Promising -> candidate pool**, NOT a winner. The loop does not keep tuning a passing candidate.

## Promotion gate (human-in-the-loop, one-shot)

A candidate becomes "real" only when:
1. A human selects it from the pool and runs it **once** against the LOCKED HOLDOUT.
2. Significance is **deflated for multiple testing** using the *total honest trial count* (Deflated Sharpe
   Ratio / equivalent). A raw OOS win with N=hundreds of trials behind it is presumed luck.
3. It survives -> earns **paper trading**, not capital. Capital only after sustained paper performance per
   pre-set promotion criteria.

## Stopping conditions

- Trial budget and/or cost/time budget.
- **Allowed to conclude "no edge found."** The loop is NEVER forced to output a winner. A loop required to
  find alpha will manufacture it. Success = rigor of falsification, not production of a winner.

## Anti-overfitting invariants (hard constraints)

- Log every trial including failures; the registry is the multiple-testing ledger.
- The agent cannot access the holdout; promotion is human-gated and one-shot.
- Success criterion is pre-registered per hypothesis (no metric shopping).
- Every hypothesis carries an economic rationale (no blind grids).
- Reality guards (liquidity participation, costs, after-tax, PIT, survivorship-free) are fixed.
- A surfaced "winner" is presumed overfit until holdout + deflated significance + live paper prove
  otherwise.

## Reuse

Built on the existing certification pipeline: Sharadar PIT store + adapter, basket-study runner, FIFO tax
engine, readiness/fail-closed hooks, EDGAR gate, snapshot hashing. ARL adds only the thin orchestration:
hypothesis registry, DEV/holdout/embargo split, trial counter + deflated-significance promotion gate, and
the human checkpoint.

## Per-iteration Codex prompt (skeleton)

```
You are one iteration of a falsification loop. Here is the hypothesis registry (tried/killed + verdicts)
and the current global trial count = {N}.
1. Propose ONE new strategy hypothesis with an economic rationale and a pre-registered success criterion.
   It must not be a variant of a killed entry in the registry.
2. Implement it as a strategy spec using existing primitives.
3. Run the certification pipeline on DEV data ONLY (PIT, survivorship-free, after-tax, walk-forward OOS).
   You may NOT access the holdout.
4. Report: hypothesis, rationale, criterion, verdict (killed/inconclusive/promising), metrics, and append
   to the registry. Increment the trial counter. Do not tune a passing candidate; stop and log.
```

## Honest expectation

A correctly-designed loop will most likely conclude "no robust edge found," because durable alpha is rare
and most apparent edge is noise. That is a successful, valuable outcome. If a candidate survives the
holdout + deflated significance, treat it as a hypothesis for paper trading — not a proven edge.
