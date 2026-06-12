# Codex Prompt — Task 11: Re-pin Basket, Execute the Alpha Campaign, Fix the Suite Hang

You are working in the `Investor/` Python project. All implementation work is done (Tasks 1–10 merged; campaign machinery verified). This task is mostly **execution and evidence**, not feature code. The governing document for campaign rules is `CODEX_PROMPT_TASK10_ALPHA_CAMPAIGN.md` — its statistical discipline (pre-registration, evidence floor, robustness verdicts, no default flips) applies verbatim and is not restated here.

Three workstreams, in order.

## A — Commit the dedupe fix and re-pin the basket

The verifier found and fixed a flaw in the pinned basket: `GOOGL` and `GOOG` (both Alphabet) were selected into the same sector — one issuer double-weighted. `select_basket` now dedupes by issuer (`_DUAL_CLASS_ISSUER_GROUPS` in `src/regime/alpha_campaign.py`), records `skipped_duplicate_issuers` per sector, and the selection-rule string says "one listing per issuer". The fix plus its test (`test_select_basket_dedupes_dual_class_listings_by_issuer`) are **uncommitted in the worktree**.

1. Run the touched tests (`tests/regime/test_alpha_campaign.py` and the new-suite slice) and `scripts/typecheck.sh`; commit the fix as `fix(campaign): dedupe dual-class issuers in basket selection`.
2. Re-run `python -m src.regime.cli alpha-campaign select-basket` (network OK locally). Expected: `GOOG` drops, the next eligible communication-services name by dollar ADV is promoted (likely NFLX or DIS — accept whatever the data says). Sanity-check the new `basket.json`: 30 names, 10 sectors × 3, `skipped_duplicate_issuers` populated for Communication Services, no other dual-class pairs present (scan the list against `_DUAL_CLASS_ISSUER_GROUPS` and eyeball for any pair you recognize as same-issuer that the map missed — if you find one, extend the map, re-select, and note it).
3. Commit the re-pinned basket as `chore(campaign): re-pin basket with issuer dedupe`. **The basket is frozen from this commit onward** — phases must run against exactly this list. If any later step makes you want to change the basket, stop and report instead.

## B — Execute the campaign (Phases 0–4)

### Pre-flight (do once, before Phase 0)
- Confirm the IBKR gateway is NOT required (the campaign reads market data only); confirm network access to the data source works (`download_market_frame` on one ticker).
- Warm the price cache for all 30 basket names + SPY with the 10y window. One frozen cache snapshot for the whole campaign — record the cache date in a `data/campaign/cache_manifest.json` (ticker, rows, first/last date). **Do not refresh data between phases.**
- Time a single 10y `pipeline-backtest` run on one ticker. If wall-clock exceeds ~5 minutes, stop and investigate before launching 30× runs (the capped-window optimization should keep it well under).
- Record git SHA; every phase artifact must carry it (the runner already stamps this — verify on the first artifact, not after 30).

### Running phases
- `alpha-campaign run --phase 0 --resume` → then 1 → 2 → 3, each only after the previous phase's artifact set is complete (`alpha-campaign status` between phases).
- Run unattended-safe: phases are resumable; if a run dies, re-invoke with `--resume` — never delete partial artifacts to "start clean" (that breaks the frozen-snapshot guarantee).
- **Per-ticker failures**: if a ticker errors (data gap, fetch failure), retry once from cache; if it still fails, mark it failed in the artifact, continue the phase, and report it. Do NOT substitute a replacement ticker mid-campaign. If ≥3 names fail, stop and report before proceeding — the basket may need a data-quality re-pin, which is a human decision.
- **Phase 3 gate**: per Task 10 — if v5's aggregate OOF AUC < 0.55, skip the A/B entirely and record the verdict. Do not "try just one A/B anyway."
- Keep a `data/campaign/run_log.md` as you go: start/end times per phase, wall-clock, failures, retries, anomalies noticed (e.g., a ticker with zero trades across all configs, a stress window where `days_to_bear_flag` is null because the model never flagged Bear — these belong in the report's findings, not buried in logs).

### Phase 4 — the report
- `alpha-campaign report`, then complete the prose conclusions per Task 10's rules. The four questions (edge vs buy-and-hold; which capabilities are supported; meta-labeler skill; crisis behavior) each get a direct answer sentence followed by its evidence table.
- Honesty requirements, restated because they bind hardest here: state the total configuration count evaluated; rank by the robustness verdicts, never by best single number; if Q1's answer is "no demonstrated edge," write exactly that in the first paragraph.
- Commit: `ALPHA_CAMPAIGN_REPORT.md`, `basket.json` (already), `cache_manifest.json`, `run_log.md`. Raw per-run artifacts stay gitignored.

## C — Fix the order-dependent suite hang (parallel-safe; do not let it block A/B)

The full `tests/regime` run stalls order-dependently around 73% at 0% CPU (an earlier symptom implicated `test_sprint57.py` LSTM/Torch after sprint51–56; everything passes in slices and in isolation).

1. Add `pytest-timeout` as a **dev-only** dependency (this is the one allowed new dependency; pin it in the dev requirements, not runtime).
2. Reproduce with `--timeout=120 --timeout-method=thread -q` to get the exact hanging test and its predecessors; bisect the predecessor set to the minimal pair.
3. Diagnose: typical culprits are a Torch/OpenMP thread deadlock after fork, an unclosed event loop, or a module-level singleton (event bus, IB thread) left running by an earlier test. Fix the root cause if it is a test-hygiene bug (missing teardown, leaked global); if it is a genuine Torch runtime incompatibility, isolate the LSTM tests behind a `pytest.mark.forked`-style subprocess boundary or an explicit fixture that re-initializes the runtime, and document why.
4. Acceptance: `python -m pytest tests/regime -q` completes in one command, no skips added, and a brief root-cause note in the PR. If after two hours of bisection the cause is still unclear, stop, write up what you found, and add `--timeout=300` to the suite config so the hang at least fails loudly instead of stalling forever.

## Definition of done

1. Three commit groups pushed: dedupe fix, re-pinned basket, campaign artifacts + report (plus the suite-hang fix, separately).
2. `ALPHA_CAMPAIGN_REPORT.md` committed, answering Q1–Q4 under Task 10's discipline, with run log and cache manifest.
3. Full `tests/regime` green in one command (workstream C), full suite + typecheck green.
4. No production defaults changed anywhere. The report's "recommended default changes" section is the only place recommendations live, each citing its evidence table, all awaiting human sign-off.
5. PR/summary message: the re-pinned basket diff (which name replaced GOOG), total campaign wall-clock, failure count, the one-sentence answers to Q1–Q4, and the suite-hang root cause.
