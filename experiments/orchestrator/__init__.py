"""
Package experiments.orchestrator provides automated, reproducible benchmarking orchestration
for self-balancing-router across Docker deployments, k6 workload generation, and runtime administrative events.
"""

from .config import ExperimentConfig, ValidationError, load_scenario, validate_scenario
from .runner import ExperimentRunner, TrialResult

__all__ = [
    "ExperimentConfig",
    "ExperimentRunner",
    "TrialResult",
    "ValidationError",
    "load_scenario",
    "validate_scenario",
]

