"""Interactive Plotly chart builders for regime analysis."""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd


logger = logging.getLogger(__name__)


def build_regime_price_chart(price_frame: pd.DataFrame, ticker: str) -> dict[str, Any]:
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.debug("Plotly is unavailable; returning lightweight fallback price chart payload.")
        return {
            "data": [{"x": price_frame.index.strftime("%Y-%m-%d").tolist(), "y": price_frame["price"].tolist(), "type": "scatter", "mode": "lines", "name": "Price"}],
            "layout": {"title": f"{ticker} — Regime History"},
        }

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=price_frame.index.strftime("%Y-%m-%d").tolist(),
            y=price_frame["price"].tolist(),
            mode="lines",
            name="Price",
            line=dict(color="#333", width=1.5),
            hovertemplate="%{x}<br>$%{y:.2f}<extra></extra>",
        )
    )

    regime_colors = {"Bull": "rgba(76,175,80,0.15)", "Neutral": "rgba(154,160,166,0.15)", "Bear": "rgba(217,83,79,0.15)"}
    if "regime" in price_frame.columns:
        current_regime = None
        start_date = None
        for date, row in price_frame.iterrows():
            regime = row["regime"]
            if regime != current_regime:
                if current_regime is not None and start_date is not None:
                    fig.add_vrect(
                        x0=start_date.strftime("%Y-%m-%d"),
                        x1=date.strftime("%Y-%m-%d"),
                        fillcolor=regime_colors.get(current_regime, "rgba(200,200,200,0.1)"),
                        layer="below",
                        line_width=0,
                    )
                current_regime = regime
                start_date = date
        if current_regime is not None and start_date is not None:
            last_date = price_frame.index[-1]
            fig.add_vrect(
                x0=start_date.strftime("%Y-%m-%d"),
                x1=last_date.strftime("%Y-%m-%d"),
                fillcolor=regime_colors.get(current_regime, "rgba(200,200,200,0.1)"),
                layer="below",
                line_width=0,
            )

    fig.update_layout(
        title=f"{ticker} — Regime History",
        xaxis_title="Date",
        yaxis_title="Price ($)",
        hovermode="x unified",
        template="plotly_white",
        margin=dict(l=50, r=20, t=40, b=40),
        height=400,
    )
    return fig.to_dict()


def build_transition_heatmap(transition_matrix: list[list[float]], labels: list[str] | None = None) -> dict[str, Any]:
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.debug("Plotly is unavailable; returning lightweight fallback transition heatmap payload.")
        return {"data": [{"z": transition_matrix, "x": labels or ["Bull", "Neutral", "Bear"], "y": labels or ["Bull", "Neutral", "Bear"], "type": "heatmap"}], "layout": {"title": "Transition Probabilities"}}

    labels = labels or ["Bull", "Neutral", "Bear"]
    fig = go.Figure(
        data=go.Heatmap(
            z=transition_matrix,
            x=labels,
            y=labels,
            text=[[f"{value:.1%}" for value in row] for row in transition_matrix],
            texttemplate="%{text}",
            colorscale="RdYlGn",
            reversescale=False,
            hovertemplate="From %{y} → To %{x}: %{z:.1%}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Transition Probabilities",
        xaxis_title="To State",
        yaxis_title="From State",
        template="plotly_white",
        margin=dict(l=60, r=20, t=40, b=40),
        height=300,
    )
    return fig.to_dict()


def build_confidence_timeline(price_frame: pd.DataFrame) -> dict[str, Any]:
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.debug("Plotly is unavailable; returning lightweight fallback confidence payload.")
        return {
            "data": [{"x": price_frame.index.strftime("%Y-%m-%d").tolist(), "y": (price_frame["state_probability"] * 100).tolist(), "type": "scatter", "mode": "lines", "name": "Regime Confidence"}],
            "layout": {"title": "Regime Confidence Over Time"},
        }

    if "state_probability" not in price_frame.columns:
        return {}

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=price_frame.index.strftime("%Y-%m-%d").tolist(),
            y=(price_frame["state_probability"] * 100).tolist(),
            mode="lines",
            name="Regime Confidence",
            fill="tozeroy",
            line=dict(color="#1976d2", width=1.5),
            hovertemplate="%{x}<br>Confidence: %{y:.1f}%<extra></extra>",
        )
    )
    fig.update_layout(
        title="Regime Confidence Over Time",
        xaxis_title="Date",
        yaxis_title="Confidence (%)",
        yaxis=dict(range=[0, 100]),
        template="plotly_white",
        margin=dict(l=50, r=20, t=40, b=40),
        height=250,
    )
    return fig.to_dict()
