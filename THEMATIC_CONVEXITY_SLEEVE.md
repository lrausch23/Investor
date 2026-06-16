# Thematic Convexity Sleeve (TCS) — Strategy Specification

Status: Draft v1 — research-only, for backtest validation. Not investment advice. No production defaults.
Owner discipline: extends the CCEL "let-winners-run" core. Reuses existing platform primitives.

## Principle

> **Do not time the theme. Make the theme's timing irrelevant** through small early breadth,
> let-winners-run holding, and rule-based exits.

Timing irrelevance is *enforced*, not aspirational, by four mechanics:
1. **Bounded downside** — each name enters small and in fixed tranches, so being early/wrong is cheap.
2. **No discretionary top-calling** — exits fire only on rules; "I think the theme topped" is forbidden.
3. **A ratcheting trailing stop is the only "top" mechanism** — it exits on the way down mechanically.
4. **Unbounded upside** — winners are never trimmed to rebalance; they run (and may graduate to the core).

The intended return signature is **convex: wrong small and often, right big and rarely.** Most theme
names should exit near their (small) entry size; a minority should compound into large multiples, and the
sleeve's P&L should be driven by those few. If realized name-level returns are not right-skewed, the
sleeve is not working and must be cut.

## Canonical config (machine-readable)

```yaml
strategy: thematic_convexity_sleeve
version: 1

capital:
  sleeve_max_pct_of_portfolio: 20.0     # aggregate cap across all active themes
  per_theme_max_pct: 8.0                # cap per theme -> forces breadth across themes
  per_name_entry_pct: 1.5               # small initial bet (wrong-small)
  per_name_hard_cap_pct: 20.0           # winner ceiling; only the EXCESS is ever trimmed
  min_names_per_theme_at_entry: 3       # no single-name theme bets
  max_names_per_theme: 8

entry:                                  # rule-based; never "buy the bottom"
  require_theme_membership: true        # name must be on an active theme's candidate list
  min_dollar_adv: 10_000_000
  min_listing_days: 180                 # lets IPO froth settle; provides some history
  emerging_filter:
    momentum_12_1_min_percentile: 50    # identifiable strength, not yet consensus-extreme
    quality_gate: pass                  # reuse repo Piotroski / ROIC / Altman gate
  scale_in:
    enabled: true
    tranche_pct: 1.5
    max_tranches: 3
    add_condition: confirmation         # add on higher-high confirmation, NOT averaging down

hold:
  never_trim_to_rebalance: true
  significant_gain_min_hold_days: 365   # any significant gain held to long-term treatment
  promote_to_core_at_pct: 6.0           # a winner that grows past this graduates to the core book

exit:
  discretionary_timing_exit: forbidden  # hard rule: no top-calling, no theme-timing sells
  triggers:
    thesis_break:
      quality_gate_fails: true          # fundamental deterioration
      theme_invalidation_flag: true     # theme-level structural kill switch
    trailing_stop:
      enabled: true
      type: ratchet_high_water
      initial_giveback_pct: 30          # wide early (let it breathe)
      tightened_giveback_pct: 18        # tighter once matured
      tighten_after_gain_pct: 100
    momentum_decay_relegation:
      enabled: true
      bottom_momentum_quantile: 0.25
      confirm_days: 21                  # hysteresis -> no whipsaw
    oversize_trim:
      enabled: true
      trim_only_above_pct: 20           # trim only the excess above per_name_hard_cap_pct
      tax_aware: true                   # long-term lots / harvest losses first
  no_rebuy_days: 31                     # wash-sale-safe re-entry

discipline_lock:                        # the non-modifiable core (kept fixed while themes rotate)
  locked:
    - capital.per_name_entry_pct
    - capital.per_name_hard_cap_pct
    - hold.never_trim_to_rebalance
    - exit.*                            # ALL exit rules are load-bearing and fixed
  modifiable:
    - active theme list and per-theme candidate names
    - entry.emerging_filter thresholds (within preset bounds only)
```

## Decision rules (deterministic; one rule-coded reason per action)

**Entry.** On each review, for every name on an active theme list that passes
`require_theme_membership`, `min_dollar_adv`, `min_listing_days`, `emerging_filter`:
buy one tranche of `per_name_entry_pct`, subject to `min_names_per_theme_at_entry` breadth and the
`per_theme_max_pct` / `sleeve_max_pct_of_portfolio` caps. Add later tranches only on confirmation
(new higher high), never by averaging into a falling name. Entry is purely rule-gated — there is no
input for "is this the bottom."

**Hold.** Never trim a winner to rebalance. A name that grows past `promote_to_core_at_pct`
graduates into the let-winners-run core book and leaves the sleeve's name budget. Any position sitting
on a significant gain is held at least `significant_gain_min_hold_days` (long-term treatment).

**Exit — rules only, evaluated in this order:**
1. `thesis_break` — quality gate fails, or the theme's `theme_invalidation_flag` is set -> full exit.
2. `oversize_trim` — if weight > `per_name_hard_cap_pct`, sell only the excess, tax-aware.
3. `trailing_stop` — exit if price falls more than the active giveback below its high-water mark
   (`initial_giveback_pct`, tightening to `tightened_giveback_pct` after a `tighten_after_gain_pct` gain).
4. `momentum_decay_relegation` — a name persistently in the bottom momentum quantile for
   `confirm_days` is a loser; cut it (harvest the loss).
There is no rule that exits because the operator believes the theme has peaked. Every executed exit
must carry one of the reason codes above in the audit trail.

## Convexity & validation criteria (must pass before any capital)

- **Right-skew check:** name-level realized return distribution is right-skewed; aggregate sleeve P&L is
  concentrated in the top few winners. Symmetric or left-skewed -> fail.
- **Bounded loss check:** no single name loses materially more than `per_name_entry_pct x (1 + adds)
  x giveback`. A name that loses multiples of its entry size means an exit rule failed.
- **No-timing audit:** 100% of exits carry a rule reason code; zero discretionary timing exits.
- **Standard discipline:** survivorship-free / point-in-time universe, out-of-sample across multiple
  regimes, net of cost / tax / slippage, benchmarked against the core book, L1, and a passive index.
- **Lock integrity:** a regression test asserts the `discipline_lock.locked` fields are unchanged across
  any theme rotation.

## Mapping to existing platform primitives (so it is directly implementable)

| TCS element | Reuse |
| --- | --- |
| Sleeve allocation, small fixed weights | `EqualWeightAllocation` (capped) within the sleeve |
| Let-winners-run, two-tier hold/sell, no_rebuy | CCEL core (`ccel_campaign.py`) |
| `emerging_filter` quality gate | repo Piotroski / ROIC / Altman gates |
| `trailing_stop` ratchet | existing `trailing_stop_level` / `_ratchet_trailing_stop` |
| `oversize_trim` tax-aware, `significant_gain_min_hold_days` | LTCG override + `tax_lot_router` |
| Audit reason codes | existing audit trail |
| Theme membership / candidate lists | new `theme` tag on watchlist names (the only "modifiable" input) |

## Notes

- The sleeve is a *capped satellite*, not the whole portfolio. It sits beside the let-winners-run core;
  the caps guarantee a theme that fails cannot sink the book, and graduation feeds proven winners into
  the core.
- Theme membership is the one intentionally human/modifiable input — define a theme by a thesis and a
  candidate list, then let the rules handle entry, sizing, holding, and exit. Rotate themes freely; never
  rotate the exit and sizing rules.
- Harvesting at single-name scale carries the tracking-error caveat documented in the CCEL close-out; use
  the platform's tax-lot routing for the `oversize_trim` and loss-relegation paths only, not as a
  standalone alpha source.
