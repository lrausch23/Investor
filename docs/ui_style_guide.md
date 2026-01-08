# UI Style Guide (Investor MVP)

This guide defines lightweight UI primitives used across the web UI. It is intentionally minimal: no framework, just reusable HTML patterns + CSS utility classes in `src/app/static/app.css`.

## Typography

- **Page title**: `<h1>` (used once per page)
- **Section title**: `.ui-section-title`
- **Muted helper text**: `.ui-muted`
- **Numbers**:
  - Use `.ui-tabular-nums` for stable number alignment
  - Use `.ui-num` for right-aligned numeric cells

## Cards

Use cards for summary blocks and side panels.

**Classes**
- `.ui-card` (container)
- `.ui-card__label` (small title)
- `.ui-card__value` (large value)
- `.ui-card__subtext` (helper text)

**Tone variants**
- `.ui-tone-neutral` (default)
- `.ui-tone-positive` (gains/returns)
- `.ui-tone-negative` (losses)
- `.ui-tone-warning` (caution states)

**Example**
```html
<div class="ui-card ui-tone-neutral">
  <div class="ui-card__label">Total Value</div>
  <div class="ui-card__value ui-tabular-nums">$2,354,448.09</div>
  <div class="ui-card__subtext ui-muted">Positions + cash</div>
</div>
```

## KPI Grid

Use `.ui-kpi-grid` for responsive KPI layouts (auto-fit cards).

```html
<div class="ui-kpi-grid">
  <!-- 4 KPI cards -->
</div>
```

## Badges

Badges are short status chips.

**Classes**
- `.ui-badge` (base)
- `.ui-badge--neutral`, `.ui-badge--safe`, `.ui-badge--risk`, `.ui-badge--outline`

**Guidelines**
- Use green/red only for P&L semantics (gain/return) and explicit risk/safe statuses.
- Avoid green fills for dates; prefer outline badges + text.

## Tables

**Conventions**
- Headers: subtle background
- Numeric cells: `.ui-num` + `.ui-tabular-nums`
- Totals row: bold values

## Collapsible Sections

Use semantic `<details>` for progressive disclosure.

**Classes**
- `.ui-disclosure` for neutral expandable sections
- `.ui-accordion` for “section” style expanders (used for grouped metrics)

**Accessibility**
- `<summary>` is keyboard-navigable by default.
- Ensure focus outlines are visible (handled globally via `:focus-visible` styles).

## Do / Don’t

- Do keep the first screen “glanceable”: KPIs + primary table.
- Do hide technical notes behind `<details>` (“Pricing details”, “How this is calculated”).
- Don’t change valuation math or business logic in UI work.
- Don’t use red/green for neutral metadata (timestamps, labels).
