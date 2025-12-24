# Portfolio Bucket Manager — Architecture Guidance (Upload this to Codex every session)

## Mission
Build a Python application with a web UI to manage a **bucketized portfolio** across multiple accounts and taxpayers (Trust taxable + Personal IRA). The app must support:
- Tax-aware transitions (migration/rebalancing)
- Ongoing bucket maintenance (drift control, policy compliance)
- Tax efficiency guardrails (avoid ST gains, avoid wash sales)
- Auditability (immutable plans + change history)
- Fee awareness (expense ratios, transaction costs) and low-cost alternatives
- MVP uses **manual entry + CSV import**
- Phase 2 adds broker sync via adapters (IB + RJ)

This guide is the source of truth. If anything is ambiguous, choose reasonable defaults but **do not change the architecture**.

---

## Product Scope

### MVP (must ship)
1) **Policy-driven 4-bucket management**
- Buckets: B1 Liquidity, B2 Defensive/Income, B3 Growth, B4 Alpha
- Policy contains per-bucket min/target/max weights and allowed asset classes
- Show allocations by bucket/account/taxpayer and highlight drift vs policy

2) **Tax-aware trade planning**
- Generate trade plans (not execute trades)
- Lot-level sell selection to minimize tax:
  - prefer harvesting losses when safe
  - prefer LT gains over ST gains
  - avoid ST gains unless necessary for liquidity/policy
- Wash sale detection (MVP = ticker or substitute group match)
- Outputs: trade list with lot-level rationale + estimated tax impacts + warnings

3) **Tax dashboard (planning grade)**
- YTD realized ST/LT gains, income, withholding
- Estimated tax liability by taxpayer entity using editable assumptions (ordinary/LTCG/NIIT/state rates)
- IRA treated as non-taxable for cap gains; track distributions/withholding separately

4) **Fees + low-cost alternatives**
- Track expense ratios per security (manual entry)
- Compute weighted expense ratio per bucket/taxpayer
- Provide mapping table for “lower-cost alternatives” within same exposure group

5) **Auditability**
- Every user edit is recorded in AuditLog (old/new JSON + note)
- Every generated plan is saved immutably (inputs + outputs + metrics + warnings)
- Exports: CSV trade list + HTML/PDF plan report

### Phase 2 (design for; do not implement)
- Broker sync adapters:
  - Interactive Brokers (API/Flex/CSV)
  - Raymond James (CSV/OFX)
- More robust “substantially identical” resolver (beyond ticker/substitute group)
- Tax form reconciliation (1099 vs 1042-S)

---

## Non-Goals (avoid scope creep)
- No live trading / order placement
- No complex portfolio optimization / forecasting engine in MVP
- No real-time streaming quotes required (optional simple price fetch adapter OK)
- No CPA-grade filing output; keep tax as planning-grade with clear limitations

---

## Core Principles (do not compromise)
1) **Entity separation**
- Trust taxpayer and Personal taxpayer are separate
- Wash sale checks are scoped to a taxpayer entity
  - Trust wash checks only within Trust taxable accounts (IB + RJ)
  - Personal IRA is separate and does not create Trust wash sales

2) **Lot-based truth**
- Taxable accounts must track **PositionLot** records
- Trades must reference lots (specific ID) for sell planning

3) **Explainability**
Every recommendation must include: 
- bucket objective served
- policy drift addressed
- tax reasoning (ST vs LT, loss harvest)
- wash-risk reasoning and alternatives
- fee reasoning if relevant

4) **Audit-first**
- Save every change (AuditLog)
- Plans are immutable snapshots

5) **Adapter boundary**
- MVP uses manual/CSV ingestion
- Broker sync is a plug-in layer later; core engines must not depend on any broker SDK

---

## Technology Choices (fixed for MVP)
- Backend: **FastAPI**
- UI: **HTMX + Jinja templates** (preferred) OR Jinja only (acceptable)
- DB: **SQLite** + SQLAlchemy ORM
- Optional: Typer CLI for imports and exports
- Testing: pytest

Do not switch to heavy front-end frameworks. Keep it simple and shippable.

---

## Repository Layout (mandatory)

repo/
README.md
ARCHITECTURE_GUIDE.md
.env.example
src/
app/
main.py # FastAPI app entry
routes/ # route modules
templates/ # Jinja templates
static/ # minimal css/js (optional)
core/
policy_engine.py # policy + drift calc
allocation.py # compute bucket/account/taxpayer weights
lot_engine.py # lot math + ST/LT classification
wash_sale.py # wash sale detection + substitute logic
trade_planner.py # rebalance, raise cash, harvest loss, etc.
tax_engine.py # planning-grade tax calc (assumptions)
fee_engine.py # weighted ER + cost drag + alternatives
explain.py # rationale strings/structures
db/
models.py # SQLAlchemy models
session.py # engine/session setup
crud.py # CRUD helpers
importers/
csv_import.py # CSV ingest
schemas.py # CSV column schemas + validation
utils/
dates.py
money.py
tests/
test_wash_sale.py
test_lot_engine.py
test_policy_engine.py
test_trade_planner.py
fixtures/

