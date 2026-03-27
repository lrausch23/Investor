from __future__ import annotations

from io import BytesIO
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from .hmm_engine import STATE_META, RegimeResult


def build_regime_figure(result: RegimeResult) -> plt.Figure:
    frame = result.price_frame.copy()
    frame["regime_group"] = (frame["regime"] != frame["regime"].shift()).cumsum()

    fig, (ax_price, ax_prob) = plt.subplots(
        2,
        1,
        figsize=(14, 8),
        sharex=True,
        constrained_layout=True,
        gridspec_kw={"height_ratios": [3.5, 1.1], "hspace": 0.08},
    )

    for _, segment in frame.groupby("regime_group"):
        label = segment["regime"].iloc[0]
        meta = STATE_META[label]
        ax_price.axvspan(segment.index[0], segment.index[-1], color=meta["color"], alpha=0.8)
        ax_prob.axvspan(segment.index[0], segment.index[-1], color=meta["color"], alpha=0.3)

    ax_price.plot(frame.index, frame["price"], color="#163d77", linewidth=2.1, label=f"{result.ticker} Adj Close")
    ax_prob.fill_between(frame.index, frame["state_probability"] * 100, color="#5b8ff9", alpha=0.45)
    ax_prob.plot(frame.index, frame["state_probability"] * 100, color="#2f5fad", linewidth=1.2)

    ax_price.set_title(f"{result.ticker} Viterbi Vision")
    ax_price.set_ylabel("Adj Close")
    ax_price.grid(alpha=0.18)
    ax_price.legend(loc="upper left")

    ax_prob.set_ylabel("Conf. %")
    ax_prob.set_ylim(0, 100)
    ax_prob.grid(alpha=0.18)
    ax_prob.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax_prob.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax_prob.xaxis.get_major_locator()))

    return fig


def save_regime_chart(result: RegimeResult, output_dir: str | Path) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    chart_path = output_path / f"{result.ticker.lower()}_regime_chart.png"
    fig = build_regime_figure(result)
    fig.savefig(chart_path, dpi=160)
    plt.close(fig)
    return chart_path


def figure_to_png_bytes(result: RegimeResult) -> BytesIO:
    fig = build_regime_figure(result)
    buffer = BytesIO()
    fig.savefig(buffer, format="png", dpi=160)
    plt.close(fig)
    buffer.seek(0)
    return buffer
