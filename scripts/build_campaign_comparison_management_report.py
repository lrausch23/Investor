from __future__ import annotations

import json
import os
import re
import textwrap
from html import escape
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CAMPAIGN_1_REPORT = PROJECT_ROOT / "ALPHA_CAMPAIGN_REPORT.md"
CAMPAIGN_2_REPORT = PROJECT_ROOT / "ALPHA_CAMPAIGN_2_REPORT.md"
CAMPAIGN_1_SUMMARY = PROJECT_ROOT / "data/campaign/phase0/summary.json"
CAMPAIGN_1_PHASE1_DIR = PROJECT_ROOT / "data/campaign/phase1/full"
CAMPAIGN_2_SUMMARY = PROJECT_ROOT / "data/campaign/portfolio_campaign2/summary.json"
OUTPUT_DIR = PROJECT_ROOT / "output/campaign_comparison_management"
ASSET_DIR = OUTPUT_DIR / "assets"
REPORT_PATH = OUTPUT_DIR / "management_overview_report.html"


TOKENS = {
    "surface": "#FCFCFD",
    "panel": "#FFFFFF",
    "ink": "#1F2430",
    "muted": "#6F768A",
    "grid": "#E6E8F0",
    "axis": "#D7DBE7",
}

COLORS = {
    "blue": "#5477C4",
    "blue_light": "#CEDFFE",
    "gold": "#B8A037",
    "orange": "#CC6F47",
    "olive": "#71B436",
    "pink": "#BD569B",
    "neutral": "#7A828F",
    "neutral_light": "#E2E5EA",
    "neutral_dark": "#464C55",
}


def pct(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.{digits}f}%"


def num(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def moneyish(value: float | None) -> str:
    if value is None:
        return "n/a"
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    return f"{value:.0f}"


def parse_percent_metric(markdown: str, label: str) -> float | None:
    pattern = re.compile(rf"\|\s*{re.escape(label)}\s*\|\s*([-+]?\d+(?:\.\d+)?)%\s*\|")
    match = pattern.search(markdown)
    if not match:
        return None
    return float(match.group(1)) / 100.0


def load_campaign_1() -> dict[str, float | int | str | None]:
    report_text = CAMPAIGN_1_REPORT.read_text()
    phase0 = json.loads(CAMPAIGN_1_SUMMARY.read_text())["aggregate"]

    phase1_files = sorted(CAMPAIGN_1_PHASE1_DIR.glob("*.json"))
    phase1_aggregate = None
    if phase1_files:
        rows = json.loads(phase1_files[0].read_text())
        phase1_aggregate = next(row for row in rows if row.get("ticker") == "__AGGREGATE__")

    return {
        "baseline_oos_return": phase0["oos_total_return"],
        "baseline_oos_sharpe": phase0["oos_sharpe_ratio"],
        "baseline_oos_max_dd": phase0["oos_max_drawdown"],
        "baseline_trades": phase0["oos_trade_count"],
        "spy_oos_return": parse_percent_metric(report_text, "SPY buy-and-hold benchmark return"),
        "phase1_true_oos_return": phase1_aggregate["oos_total_return_avg"] if phase1_aggregate else None,
        "phase1_true_oos_sharpe": phase1_aggregate["oos_sharpe_ratio_avg"] if phase1_aggregate else None,
        "phase1_true_oos_max_dd": phase1_aggregate["oos_max_drawdown_avg"] if phase1_aggregate else None,
        "phase1_true_oos_trades": phase1_aggregate["oos_trade_count_sum"] if phase1_aggregate else None,
        "phase1_full_return": phase1_aggregate["full_total_return_avg"] if phase1_aggregate else None,
        "phase1_full_trades": phase1_aggregate["full_trade_count_sum"] if phase1_aggregate else None,
    }


def load_campaign_2() -> dict[str, dict[str, float | str | list[dict]]]:
    summary = json.loads(CAMPAIGN_2_SUMMARY.read_text())
    return {row["arm"]: row for row in summary["rows"]} | {
        "__verdict__": summary["verdict"],
        "__cost_fragility__": summary["cost_fragility_result"],
    }


def set_chart_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "figure.facecolor": TOKENS["surface"],
            "axes.facecolor": TOKENS["panel"],
            "axes.edgecolor": TOKENS["axis"],
            "axes.labelcolor": TOKENS["ink"],
            "xtick.color": TOKENS["muted"],
            "ytick.color": TOKENS["muted"],
            "grid.color": TOKENS["grid"],
            "text.color": TOKENS["ink"],
            "axes.titleweight": "bold",
        }
    )


def add_header(fig: plt.Figure, ax: plt.Axes, title: str, subtitle: str) -> None:
    ax.set_title("")
    fig.subplots_adjust(top=0.82)
    left = ax.get_position().x0
    fig.text(left, 0.95, title, fontsize=15, fontweight="bold", ha="left", va="top", color=TOKENS["ink"])
    fig.text(
        left,
        0.90,
        textwrap.fill(subtitle, width=105),
        fontsize=10.5,
        ha="left",
        va="top",
        color=TOKENS["muted"],
    )


def annotate_bars(ax: plt.Axes, values: list[float], x_offset: float = 0.012, digits: int = 1) -> None:
    for patch, value in zip(ax.patches, values, strict=False):
        ax.text(
            patch.get_width() + x_offset,
            patch.get_y() + patch.get_height() / 2,
            f"{value * 100:.{digits}f}%",
            va="center",
            ha="left",
            fontsize=9,
            color=TOKENS["ink"],
        )


def save_figure(fig: plt.Figure, name: str) -> str:
    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    path = ASSET_DIR / f"{name}.png"
    fig.savefig(path, dpi=180, bbox_inches="tight", facecolor=TOKENS["surface"])
    plt.close(fig)
    return f"assets/{path.name}"


def build_charts(c1: dict, c2: dict) -> dict[str, str]:
    set_chart_style()
    chart_paths: dict[str, str] = {}

    # Chart 1: headline OOS returns.
    labels = ["Campaign 1 baseline", "Campaign 1 Phase 1 corrected", "SPY buy-hold", "Campaign 2 L0", "Campaign 2 L1"]
    returns = [
        c1["baseline_oos_return"],
        c1["phase1_true_oos_return"],
        c2["C1_spy_buy_hold"]["oos_total_return"],
        c2["L0"]["oos_total_return"],
        c2["L1"]["oos_total_return"],
    ]
    palette = [COLORS["neutral_light"], COLORS["neutral"], COLORS["gold"], COLORS["blue_light"], COLORS["blue"]]
    fig, ax = plt.subplots(figsize=(10.2, 5.4))
    y_positions = range(len(labels))
    ax.barh(y_positions, returns, color=palette, edgecolor=COLORS["neutral_dark"], linewidth=0.8)
    ax.set_yticks(list(y_positions))
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    add_header(
        fig,
        ax,
        "OOS return moved from strategy failure to portfolio candidate",
        "Out-of-sample period begins January 1, 2024. Campaign 1 Phase 1 uses corrected true OOS metrics from supporting JSON.",
    )
    ax.set_xlabel("OOS total return")
    ax.set_ylabel("")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x * 100:.0f}%"))
    ax.set_xlim(0, max(returns) * 1.18)
    annotate_bars(ax, returns)
    chart_paths["headline_return"] = save_figure(fig, "headline_oos_return")

    # Chart 2: Campaign 2 layer ablation.
    layer_labels = ["L0\nBasket", "L1\nVol target", "L2\nHMM", "L3\nMomentum", "SPY\nBuy-hold", "SPY\n200dma"]
    layer_arms = ["L0", "L1", "L2", "L3", "C1_spy_buy_hold", "C2_spy_200dma"]
    layer_returns = [c2[arm]["oos_total_return"] for arm in layer_arms]
    layer_sharpes = [c2[arm]["oos_sharpe_ratio"] for arm in layer_arms]
    fig, axes = plt.subplots(1, 2, figsize=(12.0, 5.2))
    layer_palette = [COLORS["blue_light"], COLORS["blue"], COLORS["orange"], COLORS["pink"], COLORS["gold"], COLORS["neutral"]]
    x_positions = range(len(layer_labels))
    axes[0].bar(x_positions, layer_returns, color=layer_palette, edgecolor=COLORS["neutral_dark"], linewidth=0.8)
    axes[1].bar(x_positions, layer_sharpes, color=layer_palette, edgecolor=COLORS["neutral_dark"], linewidth=0.8)
    add_header(
        fig,
        axes[0],
        "Only the volatility-target layer earns promotion support",
        "L1 is the best supported stack: it improves OOS Sharpe and Calmar while keeping OOS return degradation versus L0 under the 15% promotion threshold.",
    )
    axes[0].set_ylabel("OOS total return")
    axes[0].set_xlabel("")
    axes[0].yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x * 100:.0f}%"))
    axes[0].set_ylim(0, max(layer_returns) * 1.14)
    axes[1].set_ylabel("OOS Sharpe")
    axes[1].set_xlabel("")
    axes[1].set_ylim(0, max(layer_sharpes) * 1.16)
    for ax in axes:
        ax.set_xticks(list(x_positions))
        ax.set_xticklabels(layer_labels)
        ax.tick_params(axis="x", labelrotation=0, labelsize=9)
    for patch, value in zip(axes[0].patches, layer_returns, strict=False):
        axes[0].text(patch.get_x() + patch.get_width() / 2, patch.get_height() + 0.018, f"{value * 100:.0f}%", ha="center", va="bottom", fontsize=8)
    for patch, value in zip(axes[1].patches, layer_sharpes, strict=False):
        axes[1].text(patch.get_x() + patch.get_width() / 2, patch.get_height() + 0.04, f"{value:.2f}", ha="center", va="bottom", fontsize=8)
    chart_paths["layer_ablation"] = save_figure(fig, "campaign2_layer_ablation")

    # Chart 3: complexity cost.
    complexity_labels = ["L0", "L1", "L2", "L3"]
    turnover = [c2[arm]["annualized_turnover"] for arm in complexity_labels]
    costs = [c2[arm]["total_costs_paid"] for arm in complexity_labels]
    fig, ax1 = plt.subplots(figsize=(9.8, 5.2))
    x = range(len(complexity_labels))
    ax1.bar(x, turnover, color=[COLORS["blue_light"], COLORS["blue"], COLORS["orange"], COLORS["pink"]], edgecolor=COLORS["neutral_dark"], linewidth=0.8)
    add_header(
        fig,
        ax1,
        "HMM brake and momentum tilt add complexity faster than value",
        "Annualized turnover and total costs rise sharply after L1, while OOS Sharpe and OOS return both deteriorate.",
    )
    ax1.set_ylabel("Annualized turnover")
    ax1.set_xlabel("")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(complexity_labels)
    ax1.set_ylim(0, max(turnover) * 1.25)
    ax1.grid(axis="x", visible=False)
    for i, value in enumerate(turnover):
        if value > 5:
            ax1.text(i, value - 1.6, f"{value:.1f}x", ha="center", va="top", fontsize=9, color="#FFFFFF")
        else:
            ax1.text(i, value + 0.8, f"{value:.1f}x", ha="center", va="bottom", fontsize=9)
    ax2 = ax1.twinx()
    ax2.plot(list(x), costs, color=COLORS["neutral_dark"], marker="o", linewidth=1.8)
    ax2.set_ylabel("Total costs paid")
    ax2.set_ylim(0, max(costs) * 1.30)
    ax2.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y / 1000:.0f}k"))
    for i, value in enumerate(costs):
        ax2.text(i, value + max(costs) * 0.045, moneyish(value), ha="center", va="bottom", fontsize=8, color=COLORS["neutral_dark"])
    chart_paths["complexity"] = save_figure(fig, "campaign2_complexity_cost")

    return chart_paths


def table_row(cells: list[str], header: bool = False) -> str:
    tag = "th" if header else "td"
    return "<tr>" + "".join(f"<{tag}>{cell}</{tag}>" for cell in cells) + "</tr>"


def render_report(c1: dict, c2: dict, charts: dict[str, str]) -> str:
    verdict = c2["__verdict__"]
    l1 = c2["L1"]
    l0 = c2["L0"]
    l2 = c2["L2"]
    l3 = c2["L3"]
    spy = c2["C1_spy_buy_hold"]
    spy_200 = c2["C2_spy_200dma"]
    cost_fragility = c2["__cost_fragility__"]

    comparison_rows = [
        ["Campaign 1 baseline", pct(c1["baseline_oos_return"]), num(c1["baseline_oos_sharpe"], 3), pct(c1["baseline_oos_max_dd"]), "Not investable"],
        ["Campaign 1 Phase 1, corrected OOS", pct(c1["phase1_true_oos_return"]), num(c1["phase1_true_oos_sharpe"], 3), pct(c1["phase1_true_oos_max_dd"]), "Reject"],
        ["Campaign 2 L0", pct(l0["oos_total_return"]), num(l0["oos_sharpe_ratio"], 3), pct(l0["oos_max_drawdown"]), "Best raw OOS return"],
        ["Campaign 2 L1", pct(l1["oos_total_return"]), num(l1["oos_sharpe_ratio"], 3), pct(l1["oos_max_drawdown"]), "Best supported arm"],
        ["Campaign 2 L2", pct(l2["oos_total_return"]), num(l2["oos_sharpe_ratio"], 3), pct(l2["oos_max_drawdown"]), "Reject HMM brake"],
        ["SPY buy-hold control", pct(spy["oos_total_return"]), num(spy["oos_sharpe_ratio"], 3), pct(spy["oos_max_drawdown"]), "Benchmark"],
    ]
    comparison_table = (
        "<table><thead>"
        + table_row(["Arm", "OOS return", "OOS Sharpe", "OOS max DD", "Management read"], True)
        + "</thead><tbody>"
        + "".join(table_row(row) for row in comparison_rows)
        + "</tbody></table>"
    )

    promotion_rows = [
        ["L1 vs L0", "Supported", f"OOS return degradation {(l0['oos_total_return'] - l1['oos_total_return']) / l0['oos_total_return'] * 100:.1f}% with higher Sharpe and Calmar"],
        ["L2 vs L1", "Not supported", f"OOS return falls to {pct(l2['oos_total_return'])}; turnover rises to {l2['annualized_turnover']:.1f}x"],
        ["L3 vs L2", "Not supported", f"OOS Sharpe falls to {l3['oos_sharpe_ratio']:.3f} and max drawdown worsens"],
        ["L1 vs SPY 200dma", "Passes control hurdle", f"OOS Sharpe {l1['oos_sharpe_ratio']:.3f} versus {spy_200['oos_sharpe_ratio']:.3f}"],
        ["L1 at 2x costs", verdict["cost_fragility"], f"OOS return {pct(cost_fragility['oos_total_return'])}; OOS Sharpe {cost_fragility['oos_sharpe_ratio']:.3f}"],
    ]
    promotion_table = (
        "<table><thead>"
        + table_row(["Test", "Result", "Evidence"], True)
        + "</thead><tbody>"
        + "".join(table_row(row) for row in promotion_rows)
        + "</tbody></table>"
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Management Overview: Alpha Campaign Results</title>
  <style>
    :root {{
      --surface: #FCFCFD;
      --panel: #FFFFFF;
      --ink: #1F2430;
      --muted: #6F768A;
      --grid: #E6E8F0;
      --axis: #D7DBE7;
      --blue: #5477C4;
      --gold: #B8A037;
      --orange: #CC6F47;
      --olive: #71B436;
      --pink: #BD569B;
    }}
    body {{
      margin: 0;
      background: var(--surface);
      color: var(--ink);
      font-family: Inter, Aptos, "Segoe UI", Arial, sans-serif;
    }}
    main {{
      max-width: 1040px;
      margin: 0 auto;
      padding: 44px 22px 72px;
    }}
    header, section {{
      margin-bottom: 34px;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: clamp(2rem, 4vw, 3.5rem);
      line-height: 1.05;
      letter-spacing: 0;
    }}
    h2 {{
      margin: 0 0 12px;
      font-size: 1.55rem;
      line-height: 1.18;
      letter-spacing: 0;
    }}
    h3 {{
      margin: 24px 0 8px;
      font-size: 1.05rem;
      letter-spacing: 0;
    }}
    p, li {{
      line-height: 1.58;
      font-size: 1rem;
    }}
    a {{ color: var(--blue); }}
    .meta {{
      color: var(--muted);
      font-size: 0.95rem;
      margin: 0;
    }}
    .summary {{
      background: #F4F7FE;
      border: 1px solid #D8E2FA;
      border-radius: 16px;
      padding: 20px 22px;
    }}
    .summary ul {{
      margin: 0;
      padding-left: 22px;
    }}
    .summary li + li {{
      margin-top: 10px;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-top: 18px;
    }}
    .kpi {{
      background: var(--panel);
      border: 1px solid var(--grid);
      border-radius: 10px;
      padding: 14px;
      box-shadow: 0 8px 20px rgba(31, 36, 48, 0.04);
    }}
    .kpi .value {{
      display: block;
      font-size: 1.55rem;
      font-weight: 750;
      margin-bottom: 4px;
    }}
    .kpi .label {{
      color: var(--muted);
      font-size: 0.86rem;
      line-height: 1.35;
    }}
    figure {{
      margin: 20px 0 10px;
      background: var(--panel);
      border: 1px solid var(--grid);
      border-radius: 12px;
      padding: 12px;
    }}
    figure img {{
      display: block;
      width: 100%;
      height: auto;
    }}
    figcaption {{
      color: var(--muted);
      font-size: 0.9rem;
      margin: 8px 4px 0;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 16px 0;
      background: var(--panel);
      border: 1px solid var(--grid);
      border-radius: 10px;
      overflow: hidden;
      display: table;
    }}
    th, td {{
      border-bottom: 1px solid var(--grid);
      padding: 10px 12px;
      text-align: left;
      vertical-align: top;
      font-size: 0.94rem;
    }}
    th {{
      background: #F4F5F7;
      font-weight: 700;
    }}
    tr:last-child td {{
      border-bottom: 0;
    }}
    .callout {{
      background: #FFF7E6;
      border: 1px solid #F0D99A;
      border-radius: 12px;
      padding: 16px 18px;
    }}
    .recommendations li + li {{
      margin-top: 8px;
    }}
    @media (max-width: 760px) {{
      main {{ padding: 28px 16px 52px; }}
      .kpis {{ grid-template-columns: 1fr 1fr; }}
      th, td {{ font-size: 0.86rem; padding: 8px; }}
    }}
  </style>
</head>
<body>
  <main data-report-audience="product stakeholders">
    <header data-contract-section="title">
      <h1>Management Overview: Alpha Campaign Results</h1>
      <p class="meta">Comparison of Alpha Campaign 1 and Alpha Campaign 2 using reports generated June 12-13, 2026. OOS boundary: January 1, 2024.</p>
    </header>

    <section class="summary" data-contract-section="executive-summary">
      <h2>Executive Summary</h2>
      <ul>
        <li><strong>Campaign 2 is the clear strategic winner.</strong> The first campaign's per-name HMM regime strategy produced only {pct(c1["baseline_oos_return"])} OOS return and {num(c1["baseline_oos_sharpe"], 3)} Sharpe, while Campaign 2's portfolio framework produced {pct(l1["oos_total_return"])} OOS return and {num(l1["oos_sharpe_ratio"], 3)} Sharpe for the best supported arm, L1.</li>
        <li><strong>The improvement comes from architecture, not from the HMM brake.</strong> Campaign 2's base posture owns the basket by default and uses volatility targeting as a portfolio overlay. The HMM brake, L2, reduced full-period drawdown but failed promotion because OOS return, Sharpe, and Calmar all deteriorated versus L1.</li>
        <li><strong>Management should approve L1 for controlled paper validation, not production deployment.</strong> L1 passed the pre-registered support rules and was not cost-fragile at 2x costs, but the evidence still comes from a single OOS window with strong market participation and no live execution history.</li>
      </ul>
      <div class="kpis">
        <div class="kpi"><span class="value">{pct(c1["baseline_oos_return"])}</span><span class="label">Campaign 1 baseline OOS return</span></div>
        <div class="kpi"><span class="value">{pct(l1["oos_total_return"])}</span><span class="label">Campaign 2 L1 OOS return</span></div>
        <div class="kpi"><span class="value">{num(l1["oos_sharpe_ratio"], 2)}</span><span class="label">Campaign 2 L1 OOS Sharpe</span></div>
        <div class="kpi"><span class="value">{pct(l1["oos_max_drawdown"])}</span><span class="label">Campaign 2 L1 OOS max drawdown</span></div>
      </div>
    </section>

    <section data-contract-section="key-findings">
      <h2>Campaign 1 failed because it under-participated in upside</h2>
      <p><strong>The first campaign should remain a research negative.</strong> The baseline strategy generated {pct(c1["baseline_oos_return"])} OOS return versus roughly {pct(c1["spy_oos_return"])} for SPY buy-and-hold in the report. Its main positive was stress drawdown containment, but that came with too much opportunity cost in the OOS period.</p>
      <p><strong>The reported Phase 1 improvement should not be used as promotion evidence.</strong> The markdown table labeled the promoted family as if it improved OOS performance, but the supporting JSON shows true OOS return of {pct(c1["phase1_true_oos_return"])} and OOS Sharpe of {num(c1["phase1_true_oos_sharpe"], 3)}. The apparent stronger return came from full-sample metrics, not true OOS evidence.</p>
      <figure>
        <img src="{escape(charts["headline_return"])}" alt="Out-of-sample return comparison across Campaign 1, Campaign 2, and SPY controls">
        <figcaption>Campaign 2 changes the investment case from a weak per-name trading strategy to a credible portfolio-level candidate.</figcaption>
      </figure>
      {comparison_table}
    </section>

    <section data-contract-section="key-findings">
      <h2>Campaign 2 validates the portfolio-layer direction, with L1 as the only promoted layer</h2>
      <p><strong>The redesign answers the first campaign's main failure mode.</strong> Campaign 2 starts from full ownership of the 30-name basket, then tests whether overlays improve risk-adjusted return. L0 delivered the best raw OOS return at {pct(l0["oos_total_return"])}, while L1 gave up less than 10% relative OOS return and improved both Sharpe and Calmar.</p>
      <p><strong>The best management read is L1, not the highest-return arm.</strong> L1 is less aggressive than L0, has much lower full-period drawdown ({pct(l1["max_drawdown"])} versus {pct(l0["max_drawdown"])}), beats SPY buy-and-hold on OOS return and Sharpe, and passes the pre-registered control hurdle against SPY 200dma on OOS Sharpe.</p>
      <figure>
        <img src="{escape(charts["layer_ablation"])}" alt="Campaign 2 layer ablation showing OOS return and OOS Sharpe by arm">
        <figcaption>L1 is the only layer that earns promotion support; the HMM brake and momentum tilt fail the layer-ablation test.</figcaption>
      </figure>
      {promotion_table}
    </section>

    <section data-contract-section="key-findings">
      <h2>The HMM brake helps in stress but does not pay for its complexity</h2>
      <p><strong>L2's problem is not that it never reduces risk; it is that the cost of that protection is too high.</strong> The HMM brake improves some stress-window drawdowns, but it cuts OOS return from {pct(l1["oos_total_return"])} to {pct(l2["oos_total_return"])}, lowers OOS Sharpe from {num(l1["oos_sharpe_ratio"], 3)} to {num(l2["oos_sharpe_ratio"], 3)}, and increases annualized turnover from {num(l1["annualized_turnover"], 2)}x to {num(l2["annualized_turnover"], 2)}x.</p>
      <p><strong>L3 compounds the issue.</strong> Momentum tilt increases turnover to {num(l3["annualized_turnover"], 2)}x and lowers OOS Sharpe to {num(l3["oos_sharpe_ratio"], 3)}. These layers should stay out of defaults until they clear an independent OOS or paper-trading gate.</p>
      <figure>
        <img src="{escape(charts["complexity"])}" alt="Campaign 2 turnover and cost comparison for L0 through L3">
        <figcaption>Complexity accelerates after L1 while return quality deteriorates, which argues against promoting L2 or L3.</figcaption>
      </figure>
    </section>

    <section data-contract-section="recommended-next-steps">
      <h2>Recommended Next Steps</h2>
      <ol class="recommendations">
        <li><strong>Approve L1 for a limited paper-trading pilot.</strong> Run L1 side-by-side against L0, SPY buy-hold, SPY 200dma, and the current live/paper agent behavior using identical data, fill, cost, and rebalance assumptions.</li>
        <li><strong>Do not change production defaults yet.</strong> No live capital or default agent behavior should change until L1 survives a live paper window and a fresh walk-forward rerun.</li>
        <li><strong>Retire Campaign 1's per-name HMM strategy as a capital candidate.</strong> Keep it only as a research input or diagnostic stress filter; it failed the economic OOS test.</li>
        <li><strong>Do not promote the HMM brake or momentum tilt.</strong> L2 and L3 should remain research arms because they fail the pre-registered layer support rules and add substantial turnover.</li>
        <li><strong>Fix and lock the Campaign 1 reporting correction.</strong> The Phase 1 table should distinguish full-sample metrics from true OOS metrics before the report is reused in any approval package.</li>
      </ol>
    </section>

    <section data-contract-section="further-questions">
      <h2>Further Questions Before Any Production Decision</h2>
      <ul>
        <li>Does L1 still beat L0 and SPY controls after a fresh walk-forward rerun with a later OOS boundary or rolling OOS windows?</li>
        <li>How does L1 behave under current paper execution, including actual broker fills, residual cash, tax/friction assumptions, and rebalance timing?</li>
        <li>What is the minimum monitoring framework for L1: volatility breach, drawdown trigger, correlation spike, exposure cap, and monthly review cadence?</li>
        <li>Can a simpler non-HMM drawdown control replicate the stress benefit of L2 without the turnover penalty?</li>
      </ul>
    </section>

    <section data-contract-section="caveats-and-assumptions">
      <h2>Caveats and Assumptions</h2>
      <div class="callout">
        <p><strong>Campaigns are not perfectly like-for-like.</strong> Campaign 1 tested per-name HMM trading; Campaign 2 tested a portfolio-level layer stack. The comparison is decision-useful because both target the same 30-name basket and OOS boundary, but the performance jump should be attributed to the portfolio architecture rather than HMM forecast quality.</p>
        <p><strong>The OOS window is still limited.</strong> Campaign 2's strongest OOS evidence starts January 1, 2024, a market period favorable to equity participation. Treat L1 as paper-ready, not production-ready.</p>
        <p><strong>Sources reviewed.</strong> Primary inputs were {escape(str(CAMPAIGN_1_REPORT.relative_to(PROJECT_ROOT)))}, {escape(str(CAMPAIGN_2_REPORT.relative_to(PROJECT_ROOT)))}, {escape(str(CAMPAIGN_1_SUMMARY.relative_to(PROJECT_ROOT)))}, Campaign 1 Phase 1 supporting JSON, and {escape(str(CAMPAIGN_2_SUMMARY.relative_to(PROJECT_ROOT)))}.</p>
      </div>
    </section>
  </main>
</body>
</html>
"""
    return html


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    c1 = load_campaign_1()
    c2 = load_campaign_2()
    charts = build_charts(c1, c2)
    REPORT_PATH.write_text(render_report(c1, c2, charts))
    print(REPORT_PATH)


if __name__ == "__main__":
    main()
