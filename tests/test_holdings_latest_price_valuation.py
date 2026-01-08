from __future__ import annotations

import datetime as dt


def test_holdings_uses_latest_cached_price_for_market_value(session, tmp_path):
    from src.core.external_holdings import build_holdings_view
    from src.db.models import Account, ExternalAccountMap, ExternalConnection, ExternalHoldingSnapshot, TaxpayerEntity

    tp = TaxpayerEntity(name="Laszlo Rausch", type="PERSONAL")
    session.add(tp)
    session.flush()

    acct = Account(name="RJ Taxable", broker="RJ", account_type="TAXABLE", taxpayer_entity_id=tp.id)
    session.add(acct)
    session.flush()

    conn = ExternalConnection(
        name="RJ (Offline)",
        provider="RJ",
        broker="RJ",
        connector="RJ_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="RJ:TAXABLE", account_id=acct.id))
    session.flush()

    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2026, 1, 1, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={
                "as_of": "2026-01-01T00:00:00+00:00",
                "items": [{"provider_account_id": "RJ:TAXABLE", "symbol": "NVDA", "qty": 2.0, "market_value": 200.0}],
            },
        )
    )
    session.commit()

    prices_dir = tmp_path / "prices"
    prices_dir.mkdir(parents=True, exist_ok=True)
    (prices_dir / "NVDA.csv").write_text("Date,Close\n2026-01-02,150.00\n", encoding="utf-8")

    view = build_holdings_view(
        session,
        scope="household",
        account_id=int(acct.id),
        today=dt.date(2026, 1, 3),
        prices_dir=prices_dir,
    )
    nvda = next(p for p in view.positions if p.symbol == "NVDA")
    assert nvda.latest_price == 150.0
    assert nvda.market_value == 300.0
    assert view.total_market_value == 300.0


def test_holdings_prefers_root_price_csv_over_yfinance_cache(session, tmp_path):
    from src.core.external_holdings import build_holdings_view
    from src.db.models import Account, ExternalAccountMap, ExternalConnection, ExternalHoldingSnapshot, TaxpayerEntity

    tp = TaxpayerEntity(name="Laszlo Rausch", type="PERSONAL")
    session.add(tp)
    session.flush()

    acct = Account(name="RJ Taxable", broker="RJ", account_type="TAXABLE", taxpayer_entity_id=tp.id)
    session.add(acct)
    session.flush()

    conn = ExternalConnection(
        name="RJ (Offline)",
        provider="RJ",
        broker="RJ",
        connector="RJ_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="RJ:TAXABLE", account_id=acct.id))
    session.flush()

    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2026, 1, 7, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={
                "as_of": "2026-01-07T00:00:00+00:00",
                "items": [{"provider_account_id": "RJ:TAXABLE", "symbol": "MU", "qty": 10.0, "market_value": 2000.0}],
            },
        )
    )
    session.commit()

    prices_dir = tmp_path / "prices"
    (prices_dir / "yfinance").mkdir(parents=True, exist_ok=True)
    # Stale yfinance cache (ends at 2025-12-31).
    (prices_dir / "yfinance" / "MU.csv").write_text(
        "date,close\n2025-12-31,285.41\n",
        encoding="utf-8",
    )
    # Fresh root cache (has 2026-01-07).
    (prices_dir / "MU.csv").write_text(
        "Date,Close\n2026-01-07,337.99\n",
        encoding="utf-8",
    )

    view = build_holdings_view(
        session,
        scope="household",
        account_id=int(acct.id),
        today=dt.date(2026, 1, 7),
        prices_dir=prices_dir,
    )
    mu = next(p for p in view.positions if p.symbol == "MU")
    assert mu.latest_price == 337.99
    assert float(mu.market_value or 0.0) == 3379.9


def test_holdings_falls_back_to_snapshot_market_value_when_no_price_cache(session, tmp_path):
    from src.core.external_holdings import build_holdings_view
    from src.db.models import Account, ExternalAccountMap, ExternalConnection, ExternalHoldingSnapshot, TaxpayerEntity

    tp = TaxpayerEntity(name="Laszlo Rausch", type="PERSONAL")
    session.add(tp)
    session.flush()

    acct = Account(name="Chase IRA", broker="CHASE", account_type="IRA", taxpayer_entity_id=tp.id)
    session.add(acct)
    session.flush()

    conn = ExternalConnection(
        name="Chase IRA (Offline)",
        provider="CHASE",
        broker="CHASE",
        connector="CHASE_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="CHASE:IRA", account_id=acct.id))
    session.flush()

    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2026, 1, 1, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={
                "as_of": "2026-01-01T00:00:00+00:00",
                "items": [{"provider_account_id": "CHASE:IRA", "symbol": "VOO", "qty": 1.0, "market_value": 500.0}],
            },
        )
    )
    session.commit()

    prices_dir = tmp_path / "empty_prices"
    prices_dir.mkdir(parents=True, exist_ok=True)

    view = build_holdings_view(
        session,
        scope="household",
        account_id=int(acct.id),
        today=dt.date(2026, 1, 3),
        prices_dir=prices_dir,
    )
    voo = next(p for p in view.positions if p.symbol == "VOO")
    assert voo.latest_price is not None  # derived from snapshot MV/qty
    assert voo.market_value == 500.0
    assert view.total_market_value == 500.0
