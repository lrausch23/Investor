# Backtest Harness Readiness — Definition of Done

Standing gate for the strategy-research phase. **The agent research loop (ARL) MUST NOT run until every
item below is green.** Rationale: the loop optimizes against the harness, so any harness defect is
amplified into confident, worthless results. An accurate, self-policing backtest is the deliverable of
this phase — not a winning strategy.

Current snapshot under test: `d2ccfd9ea42e4db663003dcfacfa6a3ce69e4e91ea5c059de82b356f3a17f527`

---

## Standing harness invariants (must hold on ANY run)

These are permanent tripwires. A violation is always a bug, never a finding.

- **INV-1 — Distinctness.** Genuinely different strategy configs must produce different results. Two
  byte-identical arms = bug. *(This is what the zero-marking failure violated.)*
- **INV-2 — No false zero-marks.** A held position with a valid price never marks at zero. Zero/te­rminal
  marks occur only via explicit delisting handling (ACTIONS), never silently.
- **INV-3 — No look-ahead.** No signal or selection uses data dated after the decision date; fundamentals
  keyed on `datekey <= as_of`, prices on `date <= as_of`.
- **INV-4 — Reproducibility.** Every result stamps `data_snapshot_hash` and is reproducible for that
  snapshot.
- **INV-5 — Survivorship-free.** The universe at any date includes then-eligible names that later
  delisted; selection is never drawn from a hindsight-frozen list.
- **INV-6 — Benchmark fidelity.** The synthesized S&P 500 tracks the real index within tolerance on
  spot-check windows.
- **INV-7 — Permaticker keying.** A reused ticker never leaks one issuer's history into another; all joins
  on `permaticker`.
- **INV-8 — Tax correctness.** FIFO lots, ST/LT by holding period, wash-sale deferral; tax only on
  realized gains.
- **INV-9 — Fail-closed certification.** A run cannot read `certifiable` unless readiness ==
  `survivorship_free` AND a passing EDGAR artifact is bound to the current snapshot.
- **INV-10 — Tamper-evident ledger.** (ARL) the trial registry is append-only, hash-chained, and cannot be
  silently reset; the trial count includes failures.

---

## Definition-of-done checklist

### A. Data ingredients
- [x] Survivorship-free PIT data ingested (SF1, SEP, DAILY, SP500, ACTIONS, TICKERS); snapshot hashed.
- [x] EDGAR as-reported validation PASS, sample weighted to small-cap/delisted, bound to snapshot.
- [x] Synthesized S&P 500 benchmark sanity-checked vs real index (INV-6).
- [x] Permaticker reused-ticker leakage test exercises an actual recycled symbol (INV-7).
- [x] Resolve why readiness was capped at `partial_pit` despite EDGAR PASS (coverage gap vs classifier
      rule) — fixed by explicit delisted-name terminal handling. Failures/receiverships/liquidations mark
      to `$0`; acquisition-like delistings mark to last traded adjusted SEP price; unknown residuals branch
      by pre-delisting health and are disclosed. SBNY (`permaticker` 119243) is terminal-handled at `$0`
      and remains documented only as a fundamental-scoreability exception.

### B. Engine correctness
- [x] **Position valuation/marking fixed** — `P`-prefixed lot ids resolve to SEP prices on the PIT
      reconstitution path; no false zero-marks (INV-2). *(active bug)*
- [x] Reconstituted arms are mutually distinct and non-degenerate (INV-1): A1–A4 and C0b differ in
      terminal wealth and trade count; not all byte-identical.
- [x] No-look-ahead test on selection signals (momentum/quality/valuation) (INV-3).
- [x] Tax engine: FIFO + ST/LT + wash-sale, after-tax terminal wealth (INV-8).
- [x] Costs + slippage + after-tax applied on every run.

### C. Process integrity
- [x] Fail-closed readiness gating refuses `certifiable` without survivorship_free + EDGAR (INV-9).
- [x] Reproducible snapshot hashing stamped on results (INV-4).
- [x] Tests actually exercise the failing paths (a green suite is not proof — the zero-marking bug passed
      36 tests). Every invariant above has a test that FAILS on the broken code and PASSES on the fix.
- [x] (ARL prerequisite) Tamper-evident, append-only trial ledger implemented and tested (INV-10).

### Current blocker

- [x] Basket-study readiness is `survivorship_free`. Current corrected status: both rerun windows are
      `survivorship_free` and `certifiable` with EDGAR PASS bound to snapshot
      `d2ccfd9ea42e4db663003dcfacfa6a3ce69e4e91ea5c059de82b356f3a17f527`. The 2006-2025 run has price
      coverage `100.0%`, terminal coverage `100.0%`, and PIT fundamental coverage `99.5%`; SBNY is seeded
      to a `$0` terminal value on 2023-03-12 with `use_count=0`, while its missing SF1 rows are reported as
      a documented fundamental exception. The 1998-2015 run has price coverage `100.0%`, terminal coverage
      `100.0%`, and PIT fundamental coverage `100.0%`. The terminal artifact has `19,164` rows:
      `4,043` acquisition-last-price, `7,562` unknown-healthy-last-price, `1,878` explicit/seeded failure
      zeros, `2,046` unknown-distressed zeros, `3,540` unknown-missing-price zeros, and `95`
      acquisition-missing-price zeros. Do not infer terminal payouts from Sharadar's generic ACTIONS
      `value` column.

- [x] Reason-dependent terminal values rerun completed. Versus the prior blanket-zero run, held names that
      flipped from `$0`/default to last-price handling totaled `20` in 2006-2025 and `28` in 1998-2015.
      A1-A4 rose materially, confirming the old blanket-zero policy was distorting selection arms, but the
      corrected verdict still remains `kill_switch_fail` in both windows because no PIT selection arm beats
      the synthesized S&P 500 after tax on risk-adjusted terms.

---

## The gate

> **The agent research loop may not be started until every box in sections A–C is checked and every
> invariant INV-1…INV-10 has a passing test that demonstrably fails on the corresponding broken code.**

Until then, all strategy verdicts (survivorship delta, whether selection beats the index) are
**provisional** and must not be acted on. A result that agrees with prior expectations gets the same
scrutiny as one that does not — we verify the mechanism, not just the conclusion.

Sign-off requires: full suite green, the distinctness + zero-mark + look-ahead tests demonstrated to fail
on pre-fix code, a manual spot-check that per-name marks and the benchmark look sane, and the readiness
question resolved.
