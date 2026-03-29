from __future__ import annotations

import datetime as dt
import json

import numpy as np
import pandas as pd
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime.charts import build_regime_price_chart, build_transition_heatmap
from src.regime.data import download_market_frame


def _stream_client(monkeypatch) -> TestClient:
    regime_route._JOBS.clear()
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (None, "Regime analytics are unavailable."))
    app = create_app()
    return TestClient(app)


def test_download_market_frame_adds_open_column(monkeypatch) -> None:
    index = pd.date_range("2024-01-01", periods=3, freq="D")

    def fake_download(*, tickers, period, interval, auto_adjust, progress, threads):
        if tickers == "NVDA":
            return pd.DataFrame(
                {
                    "Open": [99.0, 100.0, 101.0],
                    "Close": [100.0, 101.0, 102.0],
                    "High": [101.0, 102.0, 103.0],
                    "Low": [98.0, 99.0, 100.0],
                    "Volume": [1_000_000, 1_100_000, 1_200_000],
                },
                index=index,
            )
        if tickers in {"^VIX", "^TNX"}:
            return pd.DataFrame({"Close": [20.0, 21.0, 22.0]}, index=index)
        raise AssertionError(f"Unexpected ticker request: {tickers}")

    monkeypatch.setattr("src.regime.data.yf.download", fake_download)
    series = download_market_frame("NVDA", period="3y", interval="1d")
    assert "open" in series.frame.columns
    assert pd.api.types.is_float_dtype(series.frame["open"])
    assert series.frame["open"].tolist() == [99.0, 100.0, 101.0]


def test_download_market_frame_falls_back_open_to_close(monkeypatch) -> None:
    index = pd.date_range("2024-01-01", periods=3, freq="D")

    def fake_download(*, tickers, period, interval, auto_adjust, progress, threads):
        if tickers == "NVDA":
            return pd.DataFrame(
                {
                    "Close": [100.0, 101.0, 102.0],
                    "High": [101.0, 102.0, 103.0],
                    "Low": [98.0, 99.0, 100.0],
                    "Volume": [1_000_000, 1_100_000, 1_200_000],
                },
                index=index,
            )
        if tickers in {"^VIX", "^TNX"}:
            return pd.DataFrame({"Close": [20.0, 21.0, 22.0]}, index=index)
        raise AssertionError(f"Unexpected ticker request: {tickers}")

    monkeypatch.setattr("src.regime.data.yf.download", fake_download)
    series = download_market_frame("NVDA", period="3y", interval="1d")
    pd.testing.assert_series_equal(series.frame["open"], series.frame["price"], check_names=False)


def test_candlestick_chart_has_price_and_volume_traces() -> None:
    dates = pd.date_range("2024-01-01", periods=10, freq="B")
    frame = pd.DataFrame(
        {
            "price": np.linspace(100, 109, 10),
            "open": np.linspace(99, 108, 10),
            "high": np.linspace(101, 110, 10),
            "low": np.linspace(98, 107, 10),
            "volume": np.linspace(1_000_000, 2_000_000, 10),
            "regime": ["Bull"] * 5 + ["Bear"] * 5,
        },
        index=dates,
    )
    payload = build_regime_price_chart(frame, "NVDA")
    trace_types = [trace.get("type") for trace in payload["data"]]
    assert "candlestick" in trace_types
    assert "bar" in trace_types
    assert payload["layout"]["height"] == 480
    assert payload["layout"]["xaxis"]["rangeslider"]["visible"] is False


def test_candlestick_chart_falls_back_without_open_column() -> None:
    dates = pd.date_range("2024-01-01", periods=4, freq="B")
    frame = pd.DataFrame(
        {
            "price": [100.0, 101.0, 102.0, 103.0],
            "high": [101.0, 102.0, 103.0, 104.0],
            "low": [99.0, 100.0, 101.0, 102.0],
            "volume": [1000.0, 1200.0, 1400.0, 1600.0],
        },
        index=dates,
    )
    payload = build_regime_price_chart(frame, "NVDA")
    candle = next(trace for trace in payload["data"] if trace.get("type") == "candlestick")
    assert candle["open"] == frame["price"].tolist()


def test_transition_heatmap_has_bold_annotations_and_diagonal_shapes() -> None:
    payload = build_transition_heatmap([[0.9, 0.1, 0.0], [0.2, 0.7, 0.1], [0.1, 0.2, 0.7]])
    heatmap = payload["data"][0]
    assert heatmap["texttemplate"] == "<b>%{text}</b>"
    assert heatmap["textfont"]["size"] == 16
    assert heatmap["zmin"] == 0.0
    assert heatmap["zmax"] == 1.0
    assert heatmap["colorbar"]["tickformat"] == ".0%"
    assert len(payload["layout"]["shapes"]) == 3
    assert payload["layout"]["height"] == 320


def test_sse_format_outputs_valid_event_frame() -> None:
    event = regime_route._sse_format("progress", {"progress": 1, "total": 2})
    assert event.startswith("event: progress\n")
    assert event.endswith("\n\n")
    payload = json.loads(event.split("data: ", 1)[1])
    assert payload == {"progress": 1, "total": 2}


def test_stream_endpoint_returns_unknown_job_error_event(monkeypatch) -> None:
    client = _stream_client(monkeypatch)
    with client.stream("GET", "/regime/stream/missing-job") as response:
        body = "".join(response.iter_text())
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert "event: error" in body
    assert "Unknown job." in body


def test_stream_endpoint_returns_done_payload(monkeypatch) -> None:
    client = _stream_client(monkeypatch)
    regime_route._JOBS["job-1"] = regime_route.RegimeJob(
        job_id="job-1",
        status="done",
        tickers=["NVDA"],
        benchmark="SOXX",
        period="3y",
        progress=1,
        total=1,
        payload={"rows": [{"ticker": "NVDA"}]},
        error=None,
        created_at=dt.datetime.now(dt.timezone.utc),
    )
    with client.stream("GET", "/regime/stream/job-1") as response:
        body = "".join(response.iter_text())
    assert "event: done" in body
    assert '"ticker": "NVDA"' in body


def test_stream_endpoint_includes_partial_result_for_latest_ticker(monkeypatch) -> None:
    client = _stream_client(monkeypatch)
    regime_route._JOBS["job-2"] = regime_route.RegimeJob(
        job_id="job-2",
        status="running",
        tickers=["NVDA"],
        benchmark="SOXX",
        period="3y",
        progress=1,
        total=2,
        payload=None,
        error=None,
        created_at=dt.datetime.now(dt.timezone.utc),
        current_ticker="NVDA",
        progress_text="Completed NVDA",
        partial_results={"NVDA": {"ticker": "NVDA", "regime": "Bull"}},
    )
    import threading

    def mark_done() -> None:
        regime_route._set_job_state("job-2", status="done", payload={"rows": [{"ticker": "NVDA"}]})

    threading.Timer(0.2, mark_done).start()
    with client.stream("GET", "/regime/stream/job-2") as response:
        body = "".join(response.iter_text())
    assert '"partial_result"' in body
    assert '"NVDA"' in body


def test_shell_context_exposes_stream_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(
        regime_route,
        "_load_hmm_runtime",
        lambda: (
            {
                "DEFAULT_TICKERS": ["NVDA"],
                "list_themes": lambda include_closed=False: [],
                "get_alerts": lambda **kwargs: [],
            },
            None,
        ),
    )
    monkeypatch.setattr(regime_route, "get_available_portfolio_scopes", lambda session: [])
    monkeypatch.setattr(regime_route, "load_payload", lambda: None)
    request = type("Req", (), {"url_for": lambda self, name, **kwargs: f"/regime/stream/{kwargs['job_id']}" if name == "regime_stream" else "/"})()
    context = regime_route.build_regime_page_context(request, session=None, actor="tester")
    config = json.loads(context["regime_config_json"])
    assert config["endpoints"]["stream"] == "/regime/stream/__JOB_ID__"


def test_regime_page_uses_updated_plotly_cdn(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": [], "warnings": []})
    monkeypatch.setattr(
        regime_route,
        "_load_hmm_runtime",
        lambda: (
            {
                "DEFAULT_TICKERS": ["NVDA"],
                "list_themes": lambda include_closed=False: [],
                "get_alerts": lambda **kwargs: [],
            },
            None,
        ),
    )
    monkeypatch.setattr(regime_route, "get_available_portfolio_scopes", lambda session: [])
    client = TestClient(create_app())
    response = client.get("/regime")
    assert "plotly-2.35.0.min.js" in response.text
    assert "regimeStreamBadge" in response.text


def test_transition_heatmap_fallback_shape_is_still_available(monkeypatch) -> None:
    import importlib
    import src.regime.charts as charts_module

    original_import = __import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("plotly"):
            raise ImportError("plotly unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)
    importlib.reload(charts_module)
    try:
        payload = charts_module.build_transition_heatmap([[0.5, 0.5], [0.5, 0.5]], labels=["Bull", "Bear"])
        assert payload["data"][0]["type"] == "heatmap"
    finally:
        monkeypatch.setattr("builtins.__import__", original_import)
        importlib.reload(charts_module)
