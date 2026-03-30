"""Custom exception hierarchy for the Investor platform."""
from __future__ import annotations


class InvestorError(Exception):
    """Base exception for all Investor application errors."""


class RegimeError(InvestorError):
    """Base for regime analysis errors."""


class DataFetchError(RegimeError):
    """Failed to fetch market data for a ticker."""


class InsufficientDataError(RegimeError):
    """Not enough historical data to fit the model."""


class ModelFittingError(RegimeError):
    """HMM model fitting failed to converge or produced invalid results."""


class PersistenceError(InvestorError):
    """Database operation failed."""


class LLMProviderError(RegimeError):
    """LLM API call failed."""


class ConfigurationError(InvestorError):
    """Missing or invalid configuration."""


class BrokerError(InvestorError):
    """Broker connection or execution failure."""


class BrokerConnectionError(BrokerError):
    """Cannot connect to broker backend."""


class BrokerExecutionError(BrokerError):
    """Trade execution failed."""


class DataValidationError(InvestorError):
    """Input data failed validation."""


class DuplicateThemeError(PersistenceError):
    """Attempted to create or rename a theme with a name that already exists."""
