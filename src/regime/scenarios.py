from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScenarioDefinition:
    scenario_id: str
    name: str
    description: str
    start_date: str
    end_date: str
    tickers: list[str]
    benchmark: str = "SPY"
    pre_buffer_days: int = 504


SCENARIOS: dict[str, ScenarioDefinition] = {
    "gfc_2008": ScenarioDefinition(
        scenario_id="gfc_2008",
        name="2008 Global Financial Crisis",
        description="Lehman collapse, credit freeze, 57% S&P drawdown",
        start_date="2007-10-01",
        end_date="2009-06-30",
        tickers=["SPY", "XLF", "GE", "BAC", "AAPL"],
    ),
    "covid_2020": ScenarioDefinition(
        scenario_id="covid_2020",
        name="2020 COVID Crash",
        description="Fastest 30% drawdown in history, V-shaped recovery",
        start_date="2020-01-02",
        end_date="2020-09-30",
        tickers=["SPY", "AAPL", "MSFT", "ZM", "AAL"],
    ),
    "rate_shock_2022": ScenarioDefinition(
        scenario_id="rate_shock_2022",
        name="2022 Rate Shock",
        description="Fed tightening cycle, growth-to-value rotation, 25% S&P drawdown",
        start_date="2022-01-03",
        end_date="2022-12-30",
        tickers=["SPY", "NVDA", "META", "XLE", "TLT"],
    ),
    "q4_2018": ScenarioDefinition(
        scenario_id="q4_2018",
        name="2018 Q4 Sell-Off",
        description="Fed rate hike fears, near-bear market, 20% drawdown",
        start_date="2018-09-01",
        end_date="2019-03-29",
        tickers=["SPY", "AAPL", "AMZN", "XLF", "IWM"],
    ),
    "china_2015": ScenarioDefinition(
        scenario_id="china_2015",
        name="2015 China Devaluation",
        description="Yuan devaluation, commodity rout, two flash crashes",
        start_date="2015-06-01",
        end_date="2016-03-31",
        tickers=["SPY", "FXI", "EEM", "XLE", "AAPL"],
    ),
}


def get_scenario(scenario_id: str) -> ScenarioDefinition:
    key = str(scenario_id or "").strip()
    if key not in SCENARIOS:
        raise ValueError(f"Unknown scenario_id: {scenario_id}")
    return SCENARIOS[key]


def list_scenarios() -> list[ScenarioDefinition]:
    return sorted(SCENARIOS.values(), key=lambda item: (item.start_date, item.scenario_id))
