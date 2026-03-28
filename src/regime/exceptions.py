"""Custom exceptions for regime analysis."""
from __future__ import annotations


class RegimeError(Exception):
    """Base exception for all regime analysis errors."""


class DataFetchError(RegimeError):
    """Failed to fetch market data for a ticker."""


class InsufficientDataError(RegimeError):
    """Not enough historical data to fit the model."""


class ModelFittingError(RegimeError):
    """HMM model fitting failed to converge or produced invalid results."""


class PersistenceError(RegimeError):
    """Database operation failed."""


class LLMProviderError(RegimeError):
    """LLM API call failed."""


class ConfigurationError(RegimeError):
    """Missing or invalid configuration."""


class DuplicateThemeError(PersistenceError):
    """Attempted to create or rename a theme with a name that already exists."""
