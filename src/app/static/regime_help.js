window.REGIME_AGENT_HELP = {
  GLOSSARY: {
    "after-tax return": "Performance after estimated taxes and reserved tax drag. This is the competition basis for agent portfolios.",
    "autonomous mode": "The scheduler can approve and submit eligible paper trades without a manual review click, subject to guardrails.",
    "entry gate": "The final checks before a buy plan can be created: signal freshness, price premise, hurdle rate, duration, anti-churn, universe, and sizing.",
    "hurdle rate": "The minimum expected net return required before a candidate can become a buy plan.",
    "ratcheting stop": "A trailing stop that can move up after the position improves. It should not move lower for a long position.",
    "skill gate": "A model-quality guardrail that prevents a meta-labeler from influencing trades when its out-of-fold AUC is below the configured threshold.",
    "time stop": "An exit rule that closes a position when the planned holding window has expired even if stop or target was not hit.",
  },
  HELP_CONTENT: {
    "agent-overview": {
      title: "What Am I Looking At?",
      body: [
        "This pane is the operator view for the four paper-trading agents. It shows whether they are connected, what each agent owns, what the latest decision cycle did, and why candidates did or did not become trades.",
        "An empty executed row can be normal: autonomous mode can still create zero orders when the market is closed, the signal is stale, the price moved away from the entry premise, or a guardrail blocks the plan.",
        "The detail drawer keeps the older forensic tables available for reconciliation and debugging without making them the primary monitoring workflow.",
      ],
      terms: ["autonomous mode", "entry gate", "skill gate"],
    },
    "agent-health-ribbon": {
      title: "Health Ribbon",
      body: [
        "The ribbon summarizes connection state, market window, autonomous mode, pending alerts, and whether the kill switch is active.",
        "Paused means policy currently allows monitoring but blocks new buys for at least one agent. Existing exit rules can still operate unless a broker or guardrail blocks them.",
      ],
      terms: ["autonomous mode"],
    },
    "agent-leaderboard": {
      title: "Agent Leaderboard",
      body: [
        "Each card represents one agent portfolio with its current equity, after-tax return, exposure, pending orders, and rank in the paper competition.",
        "The winner is the agent with the highest estimated after-tax profit. Overlap warnings mean multiple agents hold the same ticker, which can reduce the value of running independent strategies.",
      ],
      terms: ["after-tax return"],
    },
    "agent-decision-funnel": {
      title: "Decision Funnel",
      body: [
        "The funnel explains how research candidates flowed through the current decision process: candidate intake, universe screen, agent mandate, entry gates, plan creation, and execution.",
        "Top blockers identify the main reasons candidates failed. Price above entry premise, stale signal, hurdle-rate failure, and anti-churn are common healthy blocks.",
      ],
      terms: ["entry gate", "hurdle rate"],
    },
    "agent-live-activity": {
      title: "Live Activity",
      body: [
        "This feed merges plans, broker events, guardrail blocks, and system events into backend-generated sentences.",
        "Use it to answer what the agents just did and why. A block is not necessarily an error; it often means a guardrail worked as designed.",
      ],
      terms: ["ratcheting stop", "time stop"],
    },
    "agent-position-risk": {
      title: "Open Position Risk",
      body: [
        "This board shows each open position's distance to stop, current price, and target when those levels are known.",
        "The stop marker reflects the current stored stop, including any ratcheting stop updates. Positions near their time stop or stop level are the most likely next exits.",
      ],
      terms: ["ratcheting stop", "time stop"],
    },
    "agent-model-health": {
      title: "Model And System Health",
      body: [
        "These pills show whether the meta-labeler skill gate is active, whether model metadata is available, whether HMM seed agreement is recorded, and whether fallback data paths fired today.",
        "Unknown does not mean failure. It means the latest stored training or runtime record did not include that metric, so the model should not be trusted more than the evidence supports.",
      ],
      terms: ["skill gate"],
    },
    "agent-details": {
      title: "Detail Tables",
      body: [
        "The detail drawer keeps the prior tables available for reconciliation: current activity, candidate intake, model settings, LLM attribution, IBKR reconciliation, competition, agent status, positions, and execution events.",
        "Use these tables when the monitor summary is not enough to explain a discrepancy between the app and the broker.",
      ],
      terms: ["after-tax return", "skill gate"],
    },
  },
};
