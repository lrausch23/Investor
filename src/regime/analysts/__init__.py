"""Analyst plugins for the ensemble framework."""

from .kalman_stub import KalmanFilterAnalyst
from .lstm_stub import LSTMSequenceAnalyst

__all__ = ["LSTMSequenceAnalyst", "KalmanFilterAnalyst"]
