from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
import warnings
from dataclasses import dataclass
import logging
from typing import Any

from openai import OpenAI
warnings.filterwarnings("ignore", message=".*codecs.open.*", module="vaderSentiment")
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from .data import fetch_recent_news
from .exceptions import LLMProviderError
from .logging_config import setup_regime_logging

setup_regime_logging()
logger = logging.getLogger(__name__)

_VADER = SentimentIntensityAnalyzer()
_BEST_MODELS = {
    "openai": "gpt-4o",
    "gemini": "gemini-2.5-pro",
    "claude": "claude-sonnet-4-20250514",
    "ollama": "qwen3:32b",
}
_MODEL_ENV_KEYS = {
    "openai": "OPENAI_MODEL",
    "gemini": "GEMINI_MODEL",
    "claude": "ANTHROPIC_MODEL",
    "ollama": "OLLAMA_MODEL",
}
_MODEL_OVERRIDE_ENV = "FRONTIER_MODEL_OVERRIDE_PROVIDER"
_CODE_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.DOTALL | re.IGNORECASE)
META_LABELER_OVERRIDE_THRESHOLD = 0.30


@dataclass
class QualitativeAssessment:
    ticker: str
    catalyst_sentiment: str
    sentiment_score: int
    catalysts: list[dict[str, Any]]
    decision_prompt: str
    llm_response: dict[str, Any] | None
    fallback_confidence: int
    thesis_check_prompt: str | None
    thesis_check_response: dict[str, Any] | None
    source: str = "llm"
    frontier_provider: str = "auto"
    frontier_model: str = ""
    model_name: str = ""
    llm_used: bool = False


def _strip_code_fences(text: str) -> str:
    payload = str(text or "").strip()
    if not payload:
        return payload
    match = _CODE_FENCE_RE.match(payload)
    if match:
        return match.group(1).strip()
    return payload


def _score_text(text: str) -> int:
    return int(round(_VADER.polarity_scores(text)["compound"] * 5))


def _get_override_threshold() -> float:
    try:
        from .persistence import get_setting

        value = get_setting("meta_labeler_override_threshold")
        if value is not None:
            return float(value)
    except Exception:
        pass
    return META_LABELER_OVERRIDE_THRESHOLD


def _deterministic_defensive_response(
    ticker: str,
    state_name: str,
    meta_labeler_score: float,
    threshold: float,
) -> dict[str, Any]:
    if state_name == "Bear":
        verdict = "Exit"
        action = "exit"
        confidence_score = 2
        rationale = (
            f"Meta-labeler confidence critically low ({meta_labeler_score:.0%}). "
            f"Bear regime active. Deterministic override: exit to protect capital."
        )
    else:
        verdict = "Hold"
        action = "hold"
        confidence_score = 3
        rationale = (
            f"Meta-labeler confidence critically low ({meta_labeler_score:.0%}). "
            f"Deterministic override: hold position, no new entries until ML confidence recovers."
        )
    return {
        "action": action,
        "confidence": confidence_score * 10,
        "confidence_gauge": confidence_score,
        "rationale": rationale,
        "institutional_report": {
            "regime_validation": "Fundamental Pivot",
            "thesis_alignment": f"ML override at {meta_labeler_score:.0%} confidence",
            "divergence_check": "None — deterministic override",
            "verdict": verdict,
            "confidence_score": confidence_score,
            "risk_trigger": f"Meta-labeler below {threshold:.0%} threshold",
            "rationale": rationale,
            "moat_classification": "none",
            "moat_justification": "Deterministic override — ML score too low for moat assessment.",
        },
        "override_threshold": threshold,
        "ticker": ticker,
    }


def analyze_catalysts(
    ticker: str,
    context_symbols: list[str] | None = None,
    max_items_per_symbol: int = 4,
) -> tuple[list[dict[str, Any]], int, str]:
    context_symbols = context_symbols or ["SOXX", "SPY", "^TNX"]
    symbols = [ticker, *[symbol for symbol in context_symbols if symbol != ticker]]
    catalysts: list[dict[str, Any]] = []
    seen_titles: set[str] = set()

    for symbol in symbols:
        for item in fetch_recent_news(symbol, limit=max_items_per_symbol):
            title_key = item.get("title", "").strip().lower()
            if not title_key or title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            catalysts.append({**item, "source_symbol": symbol})

    if not catalysts:
        return [], 0, "Neutral"

    score = sum(_score_text(f"{item.get('title', '')} {item.get('summary', '')}") for item in catalysts)
    sentiment = "Positive" if score >= 2 else "Negative" if score <= -2 else "Neutral"
    return catalysts, score, sentiment


def _filter_relevant_catalysts(ticker: str, catalysts: list[dict[str, Any]], max_items: int = 6) -> list[dict[str, Any]]:
    priority_terms = {
        "nvda": ["blackwell", "yield", "gpu", "datacenter", "ai"],
        "avgo": ["vmware", "custom silicon", "ai", "networking"],
        "pltr": ["government", "contract", "aip", "defense"],
        "mtrn": ["industrial", "alloy", "demand", "supply chain", "manufacturing"],
        "plab": ["photomask", "wafer", "pricing", "foundry", "supply chain"],
    }
    macro_terms = ["treasury", "rates", "yield", "wafer", "tsm", "pricing", "soxx", "semiconductor"]
    terms = priority_terms.get(ticker.lower(), []) + macro_terms

    def score_item(item: dict[str, Any]) -> int:
        blob = f"{item.get('title', '')} {item.get('summary', '')}".lower()
        return sum(1 for term in terms if term in blob)

    ranked = sorted(catalysts, key=score_item, reverse=True)
    return ranked[:max_items]


def _raw_news_blob(catalysts: list[dict[str, Any]]) -> str:
    lines = []
    for item in catalysts:
        title = item.get("title", "").strip() or "Untitled"
        summary = item.get("summary", "").strip()
        source_symbol = item.get("source_symbol", "")
        lines.append(f"- [{source_symbol}] {title}" + (f" | {summary}" if summary else ""))
    return "\n".join(lines) if lines else "- No recent relevant catalysts were available."


def build_decision_prompt(
    ticker: str,
    previous_state: str,
    new_state: str,
    state_confidence: float,
    benchmark_state: str,
    catalysts: list[dict[str, Any]],
    meta_labeler_score: float | None = None,
) -> str:
    filtered = _filter_relevant_catalysts(ticker, catalysts)
    catalysts_block = _raw_news_blob(filtered)
    meta_section = ""
    if meta_labeler_score is not None:
        score = float(meta_labeler_score)
        if score >= 0.65:
            assessment = "HIGH"
            guidance = "The ML layer strongly confirms the HMM signal's probability of success."
        elif score < 0.50:
            assessment = "LOW"
            guidance = "The ML layer is skeptical and views the HMM signal as likely to fail."
        else:
            assessment = "MODERATE"
            guidance = "The ML layer is mixed and offers only partial confirmation of the HMM signal."
        meta_section = f"""

1b. XGBoost Meta-Labeler Assessment:
- Probability of Success: {score:.0%}
- Assessment: {assessment}
- Interpretation: {guidance}
""".rstrip()
    return f"""
You are a Senior Quantitative Strategist and Institutional Portfolio Manager. Your goal is to validate or invalidate technical regime shifts detected by a Hidden Markov Model. You prioritize long-term structural trends over short-term retail noise.

State mapping:
- State 0 = Bullish Expansion = Positive Mean + Low Volatility
- State 1 = Volatile Neutral = Near-zero Mean + Moderate Volatility
- State 2 = Bearish Contraction = Negative Mean + High Volatility

Reason internally before answering, but do not expose chain-of-thought. Use only the filtered catalyst feed below.

Market Intelligence Report: {ticker}

1. Quantitative Signal Input:
- HMM Transition: The model has shifted from {previous_state} to {new_state}.
- Model Confidence: {state_confidence:.0%}
- Relative Strength: The sector benchmark (SOXX) is currently in {benchmark_state}.
{meta_section}

2. Qualitative Data Feed (Raw):
{catalysts_block}

3. Executive Task:
Analyze the discrepancy between the math and the narrative.

4. Competitive Moat Assessment:
Evaluate the company's durable competitive advantage. Classify into exactly one of four categories:
- "Network Effect" — value grows with each additional user/participant
- "Switching Cost" — customers face significant cost to change providers
- "Intangibles" — durable brand equity, patents, regulatory licenses
- "Cost Advantage" — structural cost leadership through scale or process
If none of these categories applies, classify as "none".

Return strict JSON with these fields:
- regime_validation: string, either "Technical Glitch" or "Fundamental Pivot"
- thesis_alignment: short string on structural thesis impact
- divergence_check: short string on alpha factor or "None"
- verdict: one of ["Entry", "Hold", "Exit"]
- confidence_score: integer 1-10
- risk_trigger: short string
- rationale: short string
- moat_classification: one of ["Network Effect", "Switching Cost", "Intangibles", "Cost Advantage", "none"]
- moat_justification: short string explaining why this moat applies (or why none exists)
""".strip()


def build_thesis_check_prompt(
    ticker: str,
    initial_thesis: str,
    previous_label: str,
    current_label: str,
    current_regime_signal: str,
    sentiment: str,
    catalysts: list[dict[str, Any]],
) -> str:
    catalyst_lines = []
    for item in catalysts[:6]:
        title = item.get("title", "").strip() or "Untitled"
        source_symbol = item.get("source_symbol", ticker)
        catalyst_lines.append(f"- [{source_symbol}] {title}")
    catalysts_block = "\n".join(catalyst_lines) if catalyst_lines else "- No recent catalysts were available."
    return f"""
You are reviewing a live investment position against a new quantitative market regime signal.

Ticker: {ticker}
Previous Regime: {previous_label}
Current Regime: {current_label}
Current Regime Signal: {current_regime_signal}
Current News Sentiment: {sentiment}
Investment Context:
{initial_thesis}

Recent Catalysts:
{catalysts_block}

Based on the regime change and the investment context above, answer:
1. Does this regime shift invalidate the investment thesis for this position?
2. Given the position's role (Core/Critical-Path/Speculative) and time horizon (trade/tactical/strategic), how should the manager respond?
3. For trade-horizon positions: should this trigger an immediate exit?
4. For strategic-horizon positions: does this represent a buying opportunity or a thesis breakdown?

Return strict JSON with the fields:
- invalidates_thesis: boolean
- answer: string (2-3 sentences)
- rationale: short string
- action_bias: one of ["stay long", "add", "trim", "exit", "re-underwrite"]
- urgency: one of ["immediate", "this_week", "monitor"]
""".strip()


def _fallback_confidence(state_name: str, latest_probability: float, sentiment_score: int) -> int:
    base = {"Bull": 75, "Neutral": 50, "Bear": 25}[state_name]
    return max(0, min(100, base + int(round(latest_probability * 15)) + max(-15, min(15, sentiment_score * 4))))


def _fallback_llm_decision(state_name: str, latest_probability: float, sentiment_score: int) -> dict[str, Any]:
    confidence = _fallback_confidence(state_name, latest_probability, sentiment_score)
    if state_name == "Bull":
        action = "enter" if confidence >= 75 else "hold"
    elif state_name == "Neutral":
        action = "hold" if confidence >= 45 else "reduce"
    else:
        action = "exit" if confidence >= 35 else "reduce"
    return {
        "action": action,
        "confidence": confidence,
        "confidence_gauge": max(1, min(10, round(confidence / 10))),
        "rationale": "Fallback heuristic used because live frontier analysis is disabled or unavailable.",
    }


def _fallback_regime_validation(
    ticker: str,
    previous_label: str,
    state_name: str,
    benchmark_state: str,
    latest_probability: float,
    sentiment_score: int,
) -> dict[str, Any]:
    confidence_score = max(1, min(10, round(_fallback_confidence(state_name, latest_probability, sentiment_score) / 10)))
    validation = "Fundamental Pivot" if latest_probability >= 0.7 or abs(sentiment_score) >= 2 else "Technical Glitch"
    if state_name == "Bull":
        verdict = "Entry"
    elif state_name == "Neutral":
        verdict = "Hold"
    else:
        verdict = "Exit"
    divergence = "Alpha from idiosyncratic execution versus benchmark weakness." if state_name == "Bull" and benchmark_state in {"Neutral", "Bear"} else "None"
    thesis_note = "Structural thesis depends on sustained execution, durable unit economics, and supportive sector breadth."
    return {
        "regime_validation": validation,
        "thesis_alignment": thesis_note,
        "divergence_check": divergence,
        "verdict": verdict,
        "confidence_score": confidence_score,
        "risk_trigger": "If the 10-year Treasury yield breaks above 5.0%, re-underwrite immediately.",
        "rationale": f"Fallback institutional summary for transition {previous_label} -> {state_name}.",
        "moat_classification": "none",
        "moat_justification": "Fallback heuristic — no LLM moat assessment available.",
    }


def _fallback_thesis_check(previous_label: str, current_label: str, initial_thesis: str, time_horizon: str = "strategic") -> dict[str, Any]:
    invalidates = previous_label == "Bull" and current_label == "Bear"
    if invalidates:
        answer = "Yes. This quantitative shift invalidates the original thesis until it is re-underwritten."
        action_bias = "re-underwrite"
    elif previous_label != current_label:
        answer = "Not outright, but the regime change weakens the thesis and requires review."
        action_bias = "trim" if current_label == "Neutral" else "re-underwrite"
    else:
        answer = "No. The current quantitative shift does not invalidate the original thesis."
        action_bias = "stay long"
    urgency = "monitor"
    if time_horizon == "trade":
        urgency = "immediate"
    elif time_horizon == "tactical":
        urgency = "this_week"
    return {
        "invalidates_thesis": invalidates,
        "answer": answer,
        "rationale": f"Initial thesis under review: {initial_thesis}",
        "action_bias": action_bias,
        "urgency": urgency,
    }


def _theme_time_horizon(initial_thesis: str | None) -> str:
    text = str(initial_thesis or "")
    for value in ("trade", "tactical", "strategic"):
        if f"Time Horizon: {value}" in text:
            return value
    return "strategic"


def _apply_saved_model(provider: str) -> None:
    if str(os.getenv(_MODEL_OVERRIDE_ENV) or "").strip().lower() == str(provider or "").strip().lower():
        return
    try:
        from .persistence import get_setting
    except Exception:
        return
    saved_provider = str(get_setting("frontier_provider") or "").strip().lower()
    saved_model = str(get_setting("frontier_model") or "").strip()
    if saved_provider != provider or not saved_model:
        return
    env_key = _MODEL_ENV_KEYS.get(provider)
    if env_key:
        os.environ[env_key] = saved_model


def _list_openai_models() -> list[dict[str, str]]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return []
    client = OpenAI(api_key=api_key)
    try:
        raw = client.models.list()
    except Exception as exc:
        logger.warning("Failed to list OpenAI models.", exc_info=exc)
        raise LLMProviderError("Failed to list OpenAI models.") from exc
    results: list[dict[str, str]] = []
    for model in raw:
        model_id = str(getattr(model, "id", "") or "")
        if any(prefix in model_id for prefix in ("gpt-4", "gpt-3.5", "o1", "o3", "o4")):
            results.append({"id": model_id, "name": model_id, "owned_by": str(getattr(model, "owned_by", "") or "")})
    results.sort(key=lambda item: item["id"])
    return results


def _list_gemini_models() -> list[dict[str, str]]:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return []
    try:
        from google import genai  # type: ignore
    except Exception:
        return []
    client = genai.Client(api_key=api_key)
    try:
        raw = client.models.list()
    except Exception as exc:
        logger.warning("Failed to list Gemini models.", exc_info=exc)
        raise LLMProviderError("Failed to list Gemini models.") from exc
    results: list[dict[str, str]] = []
    for model in raw:
        actions = getattr(model, "supported_actions", []) or []
        if "generateContent" in actions:
            model_name = str(getattr(model, "name", "") or "")
            short_name = model_name.removeprefix("models/")
            display = str(getattr(model, "display_name", "") or short_name)
            results.append({"id": short_name, "name": display})
    results.sort(key=lambda item: item["id"])
    return results


def _list_claude_models() -> list[dict[str, str]]:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return []
    try:
        from anthropic import Anthropic  # type: ignore
    except Exception:
        return []
    client = Anthropic(api_key=api_key)
    try:
        page = client.models.list()
    except Exception as exc:
        logger.warning("Failed to list Claude models.", exc_info=exc)
        raise LLMProviderError("Failed to list Claude models.") from exc
    results: list[dict[str, str]] = []
    for model in page:
        model_id = str(getattr(model, "id", "") or "")
        display = str(getattr(model, "display_name", "") or model_id)
        results.append({"id": model_id, "name": display})
    results.sort(key=lambda item: item["id"])
    return results


def _list_ollama_models() -> list[dict[str, str]]:
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
    host = base_url.rsplit("/v1", 1)[0].rstrip("/")
    tags_url = f"{host}/api/tags"
    try:
        req = urllib.request.Request(tags_url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
    except Exception as exc:
        logger.warning("Failed to list Ollama models at %s.", tags_url, exc_info=exc)
        raise LLMProviderError("Failed to list Ollama models.") from exc
    results: list[dict[str, str]] = []
    for model in data.get("models", []) or []:
        model_name = str(model.get("name", "") or "")
        details = model.get("details", {}) or {}
        param_size = str(details.get("parameter_size", "") or "")
        quant = str(details.get("quantization_level", "") or "")
        display = model_name
        if param_size:
            display += f" ({param_size}"
            if quant:
                display += f", {quant}"
            display += ")"
        results.append({"id": model_name, "name": display})
    results.sort(key=lambda item: item["id"])
    return results


def list_provider_models(provider: str) -> list[dict[str, str]]:
    provider_key = str(provider or "").strip().lower()
    if provider_key == "openai":
        return _list_openai_models()
    if provider_key == "gemini":
        return _list_gemini_models()
    if provider_key == "claude":
        return _list_claude_models()
    if provider_key == "ollama":
        return _list_ollama_models()
    raise LLMProviderError(f"Unknown provider: {provider}")


def _request_openai(prompt: str) -> dict[str, Any] | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    _apply_saved_model("openai")
    client = OpenAI(api_key=api_key)
    model = os.getenv("OPENAI_MODEL", "gpt-4o")
    logger.info("Requesting Frontier analysis from OpenAI model=%s", model)
    try:
        response = client.responses.create(model=model, input=prompt)
    except Exception as exc:
        logger.warning("OpenAI request failed.", exc_info=exc)
        raise LLMProviderError("OpenAI request failed.") from exc
    text = getattr(response, "output_text", "").strip()
    if not text:
        return None
    cleaned = _strip_code_fences(text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("OpenAI response was not valid JSON after fence stripping: %r", text[:400])
        return {"raw_response": text}


def _request_gemini(prompt: str) -> dict[str, Any] | None:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return None
    _apply_saved_model("gemini")
    try:
        from google import genai  # type: ignore
    except Exception as exc:
        logger.debug("Gemini SDK unavailable.", exc_info=exc)
        return None

    client = genai.Client(api_key=api_key)
    model = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
    logger.info("Requesting Frontier analysis from Gemini model=%s", model)
    try:
        response = client.models.generate_content(model=model, contents=prompt)
    except Exception as exc:
        logger.warning("Gemini request failed.", exc_info=exc)
        raise LLMProviderError("Gemini request failed.") from exc
    text = getattr(response, "text", "") or ""
    text = text.strip()
    if not text:
        return None
    cleaned = _strip_code_fences(text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("Gemini response was not valid JSON after fence stripping: %r", text[:400])
        return {"raw_response": text}


def _request_claude(prompt: str) -> dict[str, Any] | None:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    _apply_saved_model("claude")
    try:
        from anthropic import Anthropic  # type: ignore
    except Exception as exc:
        logger.debug("Anthropic SDK unavailable.", exc_info=exc)
        return None

    client = Anthropic(api_key=api_key)
    model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
    logger.info("Requesting Frontier analysis from Claude model=%s", model)
    try:
        response = client.messages.create(
            model=model,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        logger.warning("Claude request failed.", exc_info=exc)
        raise LLMProviderError("Claude request failed.") from exc
    parts = []
    for block in getattr(response, "content", []) or []:
        text = getattr(block, "text", None)
        if text:
            parts.append(text)
    payload = "\n".join(parts).strip()
    if not payload:
        return None
    cleaned = _strip_code_fences(payload)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("Claude response was not valid JSON after fence stripping: %r", payload[:400])
        return {"raw_response": payload}


def _request_ollama(prompt: str) -> dict[str, Any] | None:
    _apply_saved_model("ollama")
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
    model = os.getenv("OLLAMA_MODEL", "qwen3:32b")
    client = OpenAI(base_url=base_url, api_key=os.getenv("OLLAMA_API_KEY", "ollama"))
    logger.info("Requesting Frontier analysis from Ollama model=%s base_url=%s", model, base_url)
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
    except Exception as exc:
        logger.warning("Ollama request failed.", exc_info=exc)
        raise LLMProviderError("Ollama request failed.") from exc
    text = ""
    choices = getattr(response, "choices", []) or []
    if choices:
        message = getattr(choices[0], "message", None)
        text = getattr(message, "content", "") or ""
    text = text.strip()
    if not text:
        return None
    cleaned = _strip_code_fences(text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("Ollama response was not valid JSON after fence stripping: %r", text[:400])
        return {"raw_response": text}


def _provider_request(prompt: str, provider: str, *, use_best: bool = False, model: str | None = None) -> dict[str, Any] | None:
    original: dict[str, str | None] = {
        "OPENAI_MODEL": os.getenv("OPENAI_MODEL"),
        "GEMINI_MODEL": os.getenv("GEMINI_MODEL"),
        "ANTHROPIC_MODEL": os.getenv("ANTHROPIC_MODEL"),
        "OLLAMA_MODEL": os.getenv("OLLAMA_MODEL"),
        _MODEL_OVERRIDE_ENV: os.getenv(_MODEL_OVERRIDE_ENV),
    }
    model_value = str(model or "").strip()
    if model_value and provider in _MODEL_ENV_KEYS:
        os.environ[_MODEL_ENV_KEYS[provider]] = model_value
        os.environ[_MODEL_OVERRIDE_ENV] = provider
    elif use_best:
        os.environ["OPENAI_MODEL"] = _BEST_MODELS["openai"]
        os.environ["GEMINI_MODEL"] = _BEST_MODELS["gemini"]
        os.environ["ANTHROPIC_MODEL"] = _BEST_MODELS["claude"]
        os.environ["OLLAMA_MODEL"] = _BEST_MODELS["ollama"]
        os.environ[_MODEL_OVERRIDE_ENV] = provider
    try:
        if provider == "openai":
            return _request_openai(prompt)
        if provider == "gemini":
            return _request_gemini(prompt)
        if provider == "claude":
            return _request_claude(prompt)
        if provider == "ollama":
            return _request_ollama(prompt)
        return None
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def request_frontier_decision(prompt: str, enabled: bool, provider: str = "auto", model: str | None = None) -> dict[str, Any] | None:
    if not enabled:
        return None
    provider = str(provider or "auto").strip().lower() or "auto"
    model_value = str(model or "").strip()
    try:
        if provider == "best":
            return (
                _provider_request(prompt, "claude", use_best=True)
                or _provider_request(prompt, "openai", use_best=True)
                or _provider_request(prompt, "gemini", use_best=True)
                or _provider_request(prompt, "ollama", use_best=True)
            )
        if provider == "openai":
            return _provider_request(prompt, "openai", model=model_value) if model_value else _request_openai(prompt)
        if provider == "gemini":
            return _provider_request(prompt, "gemini", model=model_value) if model_value else _request_gemini(prompt)
        if provider == "claude":
            return _provider_request(prompt, "claude", model=model_value) if model_value else _request_claude(prompt)
        if provider == "ollama":
            return _provider_request(prompt, "ollama", model=model_value) if model_value else _request_ollama(prompt)
        return _request_openai(prompt) or _request_gemini(prompt) or _request_claude(prompt) or _request_ollama(prompt)
    except LLMProviderError:
        logger.warning("Frontier provider request failed; returning fallback decision path.")
        return None


def configured_frontier_model(provider: str = "auto", model: str | None = None) -> str:
    provider_key = str(provider or "auto").strip().lower() or "auto"
    model_value = str(model or "").strip()
    if provider_key in {"openai", "gemini", "claude", "ollama"}:
        if model_value:
            label = {"openai": "OpenAI", "gemini": "Gemini", "claude": "Claude", "ollama": "Ollama"}[provider_key]
            return f"{label}: {model_value}"
        original: dict[str, str | None] = {
            "OPENAI_MODEL": os.getenv("OPENAI_MODEL"),
            "GEMINI_MODEL": os.getenv("GEMINI_MODEL"),
            "ANTHROPIC_MODEL": os.getenv("ANTHROPIC_MODEL"),
            "OLLAMA_MODEL": os.getenv("OLLAMA_MODEL"),
        }
        try:
            _apply_saved_model(provider_key)
            if provider_key == "openai":
                return f"OpenAI: {os.getenv('OPENAI_MODEL', 'gpt-4o')}"
            if provider_key == "gemini":
                return f"Gemini: {os.getenv('GEMINI_MODEL', 'gemini-2.5-pro')}"
            if provider_key == "claude":
                return f"Claude: {os.getenv('ANTHROPIC_MODEL', 'claude-sonnet-4-20250514')}"
            return f"Ollama: {os.getenv('OLLAMA_MODEL', 'qwen3:32b')}"
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
    if provider == "best":
        if os.getenv("ANTHROPIC_API_KEY"):
            return f"Claude: {_BEST_MODELS['claude']} (best)"
        if os.getenv("OPENAI_API_KEY"):
            return f"OpenAI: {_BEST_MODELS['openai']} (best)"
        if os.getenv("GEMINI_API_KEY"):
            return f"Gemini: {_BEST_MODELS['gemini']} (best)"
        return f"Ollama: {_BEST_MODELS['ollama']} (best)"
    if os.getenv("OPENAI_API_KEY"):
        return f"OpenAI: {os.getenv('OPENAI_MODEL', 'gpt-4o')}"
    if os.getenv("GEMINI_API_KEY"):
        return f"Gemini: {os.getenv('GEMINI_MODEL', 'gemini-2.5-pro')}"
    if os.getenv("ANTHROPIC_API_KEY"):
        return f"Claude: {os.getenv('ANTHROPIC_MODEL', 'claude-sonnet-4-20250514')}"
    return f"Ollama: {os.getenv('OLLAMA_MODEL', 'qwen3:32b')}"


def build_qualitative_assessment(
    ticker: str,
    regime_signal: str,
    state_name: str,
    latest_probability: float,
    context_symbols: list[str] | None = None,
    frontier_enabled: bool = False,
    frontier_provider: str = "auto",
    frontier_model: str | None = None,
    initial_thesis: str | None = None,
    previous_label: str | None = None,
    benchmark_state: str = "Neutral",
    meta_labeler_score: float | None = None,
) -> QualitativeAssessment:
    catalysts, sentiment_score, sentiment = analyze_catalysts(ticker, context_symbols=context_symbols)
    previous_state = previous_label or state_name
    threshold = _get_override_threshold()
    provider_key = str(frontier_provider or "auto").strip().lower() or "auto"
    model_value = str(frontier_model or "").strip()
    model_name = configured_frontier_model(provider_key, model_value or None)
    if meta_labeler_score is not None and meta_labeler_score < threshold:
        logger.info(
            "Deterministic LLM override for %s: meta_labeler_score=%.3f < %.3f threshold",
            ticker,
            meta_labeler_score,
            threshold,
        )
        decision_prompt = build_decision_prompt(
            ticker=ticker,
            previous_state=previous_state,
            new_state=state_name,
            state_confidence=latest_probability,
            benchmark_state=benchmark_state,
            catalysts=catalysts,
            meta_labeler_score=meta_labeler_score,
        )
        return QualitativeAssessment(
            ticker=ticker,
            catalyst_sentiment=sentiment,
            sentiment_score=sentiment_score,
            catalysts=catalysts,
            decision_prompt=decision_prompt,
            llm_response=_deterministic_defensive_response(ticker, state_name, meta_labeler_score, threshold),
            fallback_confidence=_fallback_confidence(state_name, latest_probability, sentiment_score),
            thesis_check_prompt=None,
            thesis_check_response=None,
            source="meta_labeler_override",
            frontier_provider=provider_key,
            frontier_model=model_value,
            model_name=model_name,
            llm_used=False,
        )
    decision_prompt = build_decision_prompt(
        ticker=ticker,
        previous_state=previous_state,
        new_state=state_name,
        state_confidence=latest_probability,
        benchmark_state=benchmark_state,
        catalysts=catalysts,
        meta_labeler_score=meta_labeler_score,
    )
    llm_response = request_frontier_decision(
        decision_prompt,
        enabled=frontier_enabled,
        provider=provider_key,
        model=model_value or None,
    )
    fallback_confidence = _fallback_confidence(state_name, latest_probability, sentiment_score)
    source = "llm"
    if llm_response is None:
        llm_response = _fallback_llm_decision(state_name, latest_probability, sentiment_score)
        source = "vader_fallback"
    llm_response.setdefault(
        "institutional_report",
        _fallback_regime_validation(ticker, previous_state, state_name, benchmark_state, latest_probability, sentiment_score),
    )

    thesis_check_prompt = None
    thesis_check_response = None
    if initial_thesis and previous_label and previous_label != state_name:
        thesis_check_prompt = build_thesis_check_prompt(
            ticker=ticker,
            initial_thesis=initial_thesis,
            previous_label=previous_label,
            current_label=state_name,
            current_regime_signal=regime_signal,
            sentiment=sentiment,
            catalysts=catalysts,
        )
        thesis_check_response = request_frontier_decision(
            thesis_check_prompt,
            enabled=frontier_enabled,
            provider=provider_key,
            model=model_value or None,
        ) or _fallback_thesis_check(previous_label, state_name, initial_thesis, _theme_time_horizon(initial_thesis))

    return QualitativeAssessment(
        ticker=ticker,
        catalyst_sentiment=sentiment,
        sentiment_score=sentiment_score,
        catalysts=catalysts,
        decision_prompt=decision_prompt,
        llm_response=llm_response,
        fallback_confidence=fallback_confidence,
        thesis_check_prompt=thesis_check_prompt,
        thesis_check_response=thesis_check_response,
        source=source,
        frontier_provider=provider_key,
        frontier_model=model_value,
        model_name=model_name,
        llm_used=source == "llm",
    )
