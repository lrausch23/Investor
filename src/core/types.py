from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


BucketCode = Literal["B1", "B2", "B3", "B4"]


class DriftBucketRow(BaseModel):
    code: BucketCode
    name: str
    min_pct: float
    target_pct: float
    max_pct: float
    value: float
    actual_pct: float
    traffic_light: str
    reason: str = ""


class DriftReport(BaseModel):
    policy_id: int
    total_value: float
    bucket_rows: list[DriftBucketRow]
    warnings: list[str] = Field(default_factory=list)


class TaxYtdRow(BaseModel):
    taxpayer: str
    st_gains: float
    lt_gains: float
    income: float
    withholding: float
    estimated_tax: float | None = None
    net_tax_due: float | None = None
    tax_note: str | None = None


class TaxYtdSummary(BaseModel):
    as_of: str
    rows: list[TaxYtdRow]
    totals: dict[str, float | None] | None = None
    assumptions: dict[str, Any] | None = None


class FeeRow(BaseModel):
    scope: str
    weighted_expense_ratio: float
    cost_drag: float


class FeeSummary(BaseModel):
    policy_id: int
    rows: list[FeeRow]
    warnings: list[str] = Field(default_factory=list)


class TradeRecommendation(BaseModel):
    action: Literal["BUY", "SELL"]
    account_id: int
    account_name: str
    ticker: str
    qty: float
    est_price: float
    est_value: float
    bucket_code: Optional[BucketCode] = None
    rationale: str
    requires_override: bool = False


class LotPick(BaseModel):
    ticker: str
    lot_id: int
    acquisition_date: str
    qty: float
    basis_allocated: float
    unrealized: float
    term: Literal["ST", "LT"]
    wash_risk: str


class TaxImpactRow(BaseModel):
    taxpayer: str
    st_delta: float
    lt_delta: float
    ordinary_delta: float
    estimated_tax_delta: float


class TaxImpactSummary(BaseModel):
    rows: list[TaxImpactRow]
    assumptions: dict[str, Any]


class PlannerResult(BaseModel):
    goal_json: dict[str, Any]
    inputs_json: dict[str, Any]
    outputs_json: dict[str, Any]
    trades: list[TradeRecommendation]
    lot_picks: list[LotPick]
    tax_impact: TaxImpactSummary
    post_trade: DriftReport
    warnings: list[str] = Field(default_factory=list)
