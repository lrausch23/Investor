# Codex Prompt — Task 12: Redesigned Agent Monitor + Context-Sensitive Help

You are working in the `Investor/` Python project (FastAPI + Jinja2 + vanilla JS in `src/app/static/regime.js` + `src/app/static/app.css` — **no frontend frameworks, keep it that way**). This task redesigns the `#agents` tab of `/regime` into a monitoring cockpit and adds quick-reference help to every section. It is UI + read-only API work: **no changes to any trading, gating, sizing, or persistence-write logic.**

**Layout reference:** `design/AGENT_MONITOR_MOCKUP.html` (committed). Match its structure and hierarchy; adapt visual details to the app's existing styles. The mockup's sample numbers are illustrative.

## Problems being solved

1. **Width bug (fix first, separately):** the entire regime dashboard renders in a ~270px column on wide screens — content is microscopic. Find the cause in `app.css` / `regime.html` (likely a max-width/flex-basis on the main container) and fix it for ALL tabs, not just `#agents`. Commit independently: `fix(ui): regime dashboard width on wide screens`.
2. **Flat information architecture:** `#agents` is ten stacked database tables. Monitoring needs answers, in order: is the system alive and safe → who's winning → what did agents decide today and why → what's at risk → are the models trustworthy.
3. **Missing aggregate views:** no decision funnel (where candidates die), no narrative activity feed, no visual stop/target distance — even though all underlying data exists.
4. **No onboarding:** a new user has no way to learn what "hurdle", "ML 0.58", or "skill gate" mean without reading source.

## Deliverables

### 1. New `#agents` layout (template + JS rendering)

Top-to-bottom, per the mockup:

- **Health ribbon** — pill row: IBKR connection, market window + next scheduled cycle, operating mode, VIX level/freeze state, kill-switch state, unacknowledged-alert count with the most urgent alert inline. Source: existing monitoring payload (`/regime/paper-portfolio/{id}/monitoring`), settings, alerts. Reuse the polling cadence already in `regime.js` (`monitoringTimer`).
- **Agent leaderboard cards** — one card per beta agent: name + mandate one-liner, rank badge, equity, today's P&L, after-tax return vs the 2%/month target as a progress bar, open-position count, exposure %, pending-plan count, and a status badge (`active` / `paused — drawdown` / `cooldown` / `blocked`) derived from `agent_policy.buy_pause_status` reasons. Source: `agent_dashboard.py` payload (leaderboard already computed) — extend it with the pause status rather than recomputing client-side.
- **Decision funnel (today)** — horizontal bars: candidates → universe screen → agent mandates → entry gates → plans created → executed, with a "top blockers" line listing the most frequent rejection reasons. Requires a small **new read-only endpoint** (see §3).
- **Live activity feed** — reverse-chronological sentences with timestamp, agent, icon by type (buy / sell / reduce / blocked / system), and the *reason inline* (e.g. "trailing stop, +$74 net", "price 8.6% above entry premise"). Client-side filter chips: all / trades / blocks / system. Requires the feed endpoint (§3).
- **Open positions risk board** — table with a stop◂price▸target bar per position (geometry fields exist on positions since the exit-parity work), P&L colored, held-days vs time-stop, regime badge with probability. One computed callout line for the position nearest its stop (smallest % distance). Source: existing positions + cached regime payload.
- **Model & system health strip** — pills: active meta-labeler version + OOF AUC + skill-gate state, HMM seed agreement (latest), calibration Brier delta, `decision_health` fallback count today, LLM attribution summary (resolved verdicts, win rate). Sources: model metadata sidecar, training log, `decision_health`, `get_llm_attribution_summary`.
- **Detail drawer** — every table currently on the page (candidate intake, IBKR reconciliation, agent LLM model config, competition detail, execution events) survives, collapsed under a "Details" section with expand/collapse per table. Nothing is deleted; it is demoted. Deep links like `#agents/intake` should expand the right one.

### 2. Context-sensitive help

- Every section header gets a help affordance: a small `?` button (`aria-label="About this section"`, `aria-expanded` toggled, Escape closes, focus returns to the button). Clicking opens an anchored popover (positioned, not `position:fixed`-overlaying everything) with 3–6 sentences: what the section shows, where the data comes from, and what a healthy vs concerning state looks like.
- **Single source of truth:** help content lives in ONE structure — a `HELP_CONTENT` map in a new `src/app/static/regime_help.js` (or JSON served from the route — your call, but one place only), keyed by section id. Glossary terms inside help text (hurdle rate, ML probability, skill gate, trailing stop, regime, exposure, time stop, drawdown pause) render as `<dfn>`-styled inline terms whose definitions come from a shared `GLOSSARY` map in the same file — a term is defined once, reused everywhere.
- **Use this draft copy** (edit for accuracy if the code disagrees with it — the code wins):
  - *Health ribbon:* "System status at a glance. Green means the broker link is up and trading is allowed. The cycle time is when agents next evaluate signals. If the kill switch or a VIX freeze is shown, all new buying is halted. Amber alerts need acknowledgment on the Trading tab."
  - *Agent cards:* "Each card is one autonomous agent running its own paper portfolio with a distinct mandate. Rank compares after-tax return. The bar shows progress toward the 2%-per-month target. 'Paused' means a safety rule (such as a drawdown limit) has halted that agent's buying — positions are still managed and exits still fire."
  - *Decision funnel:* "Every day candidates flow left to right; each stage removes names. A healthy funnel narrows sharply — most candidates should fail. 'Top blockers' shows which gate rejected the most candidates today. An empty 'executed' row means agents found nothing worth buying, which is normal on most days."
  - *Activity feed:* "A plain-language log of every agent decision, with the reason attached. Buys show the ML confidence and hurdle margin that justified them; sells show which exit rule fired; blocks show which gate refused and why. Use the filters to isolate trades or rejections."
  - *Positions board:* "Each open position is drawn between its stop (left) and target (right); the marker is the current price. The stop ratchets up as the price rises and never moves down. 'Held' counts calendar days toward the time stop, which closes positions that go nowhere. The callout flags the position most likely to exit next."
  - *Model health:* "Trust indicators for the models behind the decisions. The meta-labeler may only influence trades if its out-of-sample AUC clears the skill gate (0.55). Seed agreement near 1.0 means the regime model is confident; low agreement blocks new entries for that ticker. Rising data-fallback counts mean inputs are degrading."
  - *Details drawer:* "Raw tables for configuration and forensics: candidate-level intake decisions, broker reconciliation, per-agent LLM model settings, and the full execution log."
- A "What am I looking at?" link at the top of the pane opens a one-screen overview stitching the section summaries together for first-time users.

### 3. New read-only endpoints (additive, under `src/app/routes/`)

- `GET /regime/agents/funnel?date=today` → `{stages: [{key,label,count}], blockers: [{reason,count}]}`. Aggregate from: candidate intake records (screen/mandate outcomes), buy-plan gate logs/audit events (use the existing audit-event types; if gate rejections are currently only `logger.info`, add an audit event at rejection sites — one-line additions per gate, **no behavior change**), plan and execution counts.
- `GET /regime/agents/feed?limit=50&before=...` → merged, time-sorted feed from the audit trail (plan created/approved/blocked/guardrail events), executions (fills with net P&L for exits), and system events (retrain, pause, VIX freeze, kill switch). Each item: `{ts, agent_key, kind, text, detail}` — compose the sentence server-side so the client stays dumb. Paginated.
- Extend the existing agent-dashboard payload with: pause status per agent, model-health fields (AUC, skill-gate state, seed agreement, fallback count, calibration delta) — pull from the metadata sidecar and training log; tolerate their absence (older installs) by omitting the pill.

### 4. Tests

- Route tests (follow `tests/test_regime_route.py` patterns; run that file **in isolation** — known order flake): funnel endpoint aggregates correctly from seeded audit/intake fixtures; feed endpoint merges and sorts the three sources, paginates, composes expected sentences for a buy, a stop exit, and a gate block; dashboard payload carries pause status and model health; missing metadata omits fields without erroring.
- Template/static tests: `#agents` panel contains the six section landmarks (assert on stable `data-section` attributes); every `data-section` has a matching `HELP_CONTENT` key (parse the JS or serve it as JSON and import — pick the testable option); help button markup carries the aria attributes.
- No-behavior-change check: the audit-event additions at gate-rejection sites must not alter any gate decision — assert plan generation results are unchanged on an existing planning test.

### 5. Validation

- Full suite + `scripts/typecheck.sh` green (sprint57 and route file isolated per project notes).
- Chrome pass against `http://127.0.0.1:8000/regime#agents` at BOTH a wide window (~2000px — confirms the width fix) and ~1280px: all six sections render, help popovers open/close with mouse and keyboard, detail drawer expands, no console errors. Include screenshots in the PR.
- Verify polling doesn't multiply: switching tabs back and forth must not stack timers (existing `monitoringTimer` pattern — extend, don't duplicate).

## Constraints

- No new runtime dependencies, no frontend frameworks, no inline `onclick=` handlers (use addEventListener like the existing code), keep `.env` untouched, additive endpoints only, dark-mode-safe colors if the app has a dark theme (follow whatever `app.css` does today).
- Do not touch the alpha-campaign work, trading logic, or anything under `src/regime/` except the read-only aggregation helpers you need (put those in `src/regime/agent_dashboard.py` where the payload is already built).
- Commits: width fix → endpoints + tests → layout + help → validation screenshots. Each independently green.

## Definition of done

1. Width bug fixed everywhere; `#agents` matches the mockup's hierarchy with live data; old tables demoted but reachable; help on every section from a single content map with shared glossary.
2. New endpoints tested; full suite green; Chrome validation screenshots at two widths in the PR.
3. PR description: before/after screenshots, the width-bug root cause, endpoint shapes, and a note on any help-copy edits made because the code disagreed with the drafts.
