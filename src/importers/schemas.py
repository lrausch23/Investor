from __future__ import annotations

import datetime as dt
from typing import Literal, Optional

from pydantic import BaseModel, field_validator


def _none_if_blank(v):
    if v is None:
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


class LotsRow(BaseModel):
    account_name: str
    ticker: str
    acquisition_date: dt.date
    qty: float
    basis_total: float
    adjusted_basis_total: Optional[float] = None

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("adjusted_basis_total", mode="before")
    @classmethod
    def _adjusted_basis_blank_to_none(cls, v):
        return _none_if_blank(v)


class CashBalanceRow(BaseModel):
    account_name: str
    as_of_date: dt.date
    amount: float


class IncomeEventRow(BaseModel):
    account_name: str
    date: dt.date
    type: Literal["DIVIDEND", "INTEREST", "WITHHOLDING", "FEE"]
    ticker: Optional[str] = None
    amount: float

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        vv = v.strip().upper()
        return vv or None


class TransactionRow(BaseModel):
    account_name: str
    date: dt.date
    type: Literal["BUY", "SELL", "DIV", "INT", "FEE", "WITHHOLDING", "TRANSFER"]
    ticker: Optional[str] = None
    qty: Optional[float] = None
    amount: float
    lot_basis_total: Optional[float] = None
    lot_acquisition_date: Optional[dt.date] = None
    term: Optional[Literal["ST", "LT"]] = None

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        vv = v.strip().upper()
        return vv or None

    @field_validator("qty", "lot_basis_total", mode="before")
    @classmethod
    def _optional_numbers_blank_to_none(cls, v):
        return _none_if_blank(v)

    @field_validator("lot_acquisition_date", mode="before")
    @classmethod
    def _optional_date_blank_to_none(cls, v):
        return _none_if_blank(v)

    @field_validator("term", mode="before")
    @classmethod
    def _term_blank_to_none_and_upper(cls, v):
        v = _none_if_blank(v)
        if isinstance(v, str):
            return v.strip().upper()
        return v


class SecurityRow(BaseModel):
    ticker: str
    name: str
    asset_class: str
    expense_ratio: float = 0.0
    substitute_group: Optional[str] = None
    last_price: float = 0.0
    low_cost_ticker: Optional[str] = None

    @field_validator("ticker", "asset_class")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("low_cost_ticker")
    @classmethod
    def _low_cost(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        vv = v.strip().upper()
        return vv or None
