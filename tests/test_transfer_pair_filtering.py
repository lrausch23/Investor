from __future__ import annotations

import datetime as dt

from src.core.performance import _filter_internal_transfer_pairs


def test_filter_internal_transfer_pairs_drops_exact_canceling_pairs():
    d = dt.date(2025, 7, 3)
    transfers = [
        (d, 100.00, "TRANSFER", "REC FR SIS"),
        (d, -100.00, "TRANSFER", "TRSF TO SHADO FX SETTLEMENT"),
    ]
    assert _filter_internal_transfer_pairs(transfers) == []


def test_filter_internal_transfer_pairs_keeps_unpaired_residual():
    d = dt.date(2025, 7, 3)
    transfers = [
        (d, 100.00, "TRANSFER", "REC FR SIS"),
        (d, -100.00, "TRANSFER", "TRSF TO SHADO FX SETTLEMENT"),
        (d, -25.00, "TRANSFER", "WIRE TO VENDOR"),
    ]
    out = _filter_internal_transfer_pairs(transfers)
    assert out == [(d, -25.00)]


def test_filter_internal_transfer_pairs_excludes_unknown_multicurrency_transfers():
    d = dt.date(2025, 7, 3)
    transfers = [
        (d, -100.00, "UNKNOWN", "Transfer to Multi Currency Account"),
    ]
    assert _filter_internal_transfer_pairs(transfers) == []


def test_filter_internal_transfer_pairs_excludes_rj_shado_and_sis_transfers():
    d = dt.date(2025, 10, 3)
    transfers = [
        (d, -650.33, "TRANSFER", "Cash TRSF TO SHADO ACCT FOR FX TRAD"),
        (d, 650.33, "TRANSFER", "US Dollar REC FR SIS 870FW554 $650.33"),
        (d, -651.74, "TRANSFER", "Euro WIRE TO North Sails d.o.o. €(555.00)"),
    ]
    out = _filter_internal_transfer_pairs(transfers)
    assert out == [(d, -651.74)]
