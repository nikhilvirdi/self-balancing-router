"""
Configuration, scenario resolution, and validation for experiment orchestration.
"""

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import socket
import sys
from typing import Any, Dict, List, Optional, Set

# Attempt import of existing scenario validation
try:
    from experiments.validate_scenarios import validate_scenario_domain
except ImportError:
    # If run from another directory or package path
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
        from experiments.validate_scenarios import validate_scenario_domain
    except ImportError:
        validate_scenario_domain = None

try:
    import jsonschema
except ImportError:
    jsonschema = None

SUPPORTED_POLICIES: Set[str] = {
    "first-eligible",
    "round-robin",
    "least-requests",
    "p2c",
    "p2c-peak-ewma",
}

POLICY_NORMALIZATION: Dict[str, str] = {
    "first_eligible": "first-eligible",
    "first-eligible": "first-eligible",
    "round_robin": "round-robin",
    "round-robin": "round-robin",
    "least_requests": "least-requests",
    "least-requests": "least-requests",
    "p2c": "p2c",
    "p2c_peak_ewma": "p2c-peak-ewma",
    "p2c-peak-ewma": "p2c-peak-ewma",
}

DEFAULT_PROXY_PORT = 8080
DEFAULT_ADMIN_PORTS: Dict[str, int] = {
    "backend-a": 19091,
    "backend-b": 19092,
    "backend-c": 19093,
    "backend-d": 19094,
    "backend-e": 19095,
}


class ValidationError(ValueError):
    """Raised when an experimental scenario or configuration fails validation."""

    def __init__(self, message: str, errors: Optional[List[str]] = None):
        super().__init__(message)
        self.errors = errors or []


class PortConflictError(ValidationError):
    """Raised when one or more required network ports are already bound."""

    def __init__(self, conflicts: Dict[str, int]):
        conflict_desc = ", ".join(f"{name}:{port}" for name, port in sorted(conflicts.items()))
        super().__init__(f"Port conflict detected for: {conflict_desc}")
        self.conflicts = conflicts


def normalize_policy(policy_name: str) -> str:
    """Normalizes policy name into canonical hyphenated form."""
    clean = (policy_name or "").strip().lower()
    if clean in POLICY_NORMALIZATION:
        return POLICY_NORMALIZATION[clean]
    raise ValidationError(
        f"Unsupported routing policy: '{policy_name}'. Supported policies: {sorted(list(SUPPORTED_POLICIES))}"
    )


def resolve_repo_root() -> Path:
    """Resolves repository root path."""
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / "deployments" / "compose.yaml").exists():
            return parent
    return Path.cwd()


def load_scenario(
    scenario_key: str,
    catalog_path: Optional[str] = None,
    scenarios_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Loads and resolves a scenario definition from a file path, catalog, or scenario directory.
    """
    repo_root = resolve_repo_root()

    # 1. Direct file path check
    key_path = Path(scenario_key)
    if not key_path.is_absolute():
        direct_path = repo_root / key_path
    else:
        direct_path = key_path

    if direct_path.is_file():
        try:
            with open(direct_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "scenarios" in data and isinstance(data["scenarios"], list):
                if len(data["scenarios"]) == 1:
                    return data["scenarios"][0]
                raise ValidationError(f"File {direct_path} contains multiple scenarios, specify a scenario ID")
            return data
        except Exception as e:
            raise ValidationError(f"Failed to read scenario file {direct_path}: {e}")

    # 2. Check individual scenarios directory
    if not scenarios_dir:
        scenarios_dir = str(repo_root / "experiments" / "scenarios")

    scenarios_path = Path(scenarios_dir)
    if scenarios_path.is_dir():
        # Match e.g. 01_stable_homogeneous.json or scenario_1_homogeneous
        clean_key = scenario_key.lower().replace(".json", "")
        for entry in scenarios_path.glob("*.json"):
            entry_clean = entry.stem.lower()
            if entry_clean == clean_key or entry_clean.endswith(clean_key):
                with open(entry, "r", encoding="utf-8") as f:
                    return json.load(f)

    # 3. Check scenario catalog
    if not catalog_path:
        catalog_path = str(repo_root / "experiments" / "scenarios.json")

    cat_file = Path(catalog_path)
    if not cat_file.is_file():
        raise ValidationError(f"Scenario catalog not found at {catalog_path}")

    with open(cat_file, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    scenarios = catalog.get("scenarios", [])
    if not scenarios:
        raise ValidationError("Scenario catalog does not contain a 'scenarios' list")

    # Match by exact id
    for sc in scenarios:
        if sc.get("id") == scenario_key:
            return sc

    # Match by case-insensitive id or name
    lower_key = scenario_key.lower()
    for sc in scenarios:
        if sc.get("id", "").lower() == lower_key or sc.get("name", "").lower() == lower_key:
            return sc

    # Match by index (1-based)
    try:
        idx = int(scenario_key)
        if 1 <= idx <= len(scenarios):
            return scenarios[idx - 1]
    except ValueError:
        pass

    # Match by numeric prefix (e.g., '1' or '01')
    match = re.match(r"^0?([1-7])$", scenario_key)
    if match:
        num = int(match.group(1))
        if 1 <= num <= len(scenarios):
            return scenarios[num - 1]

    available = [s.get("id") for s in scenarios if "id" in s]
    raise ValidationError(
        f"Unknown scenario identifier: '{scenario_key}'. Available scenarios: {', '.join(available)}"
    )


def validate_scenario(scenario: Dict[str, Any], schema_path: Optional[str] = None) -> None:
    """
    Validates scenario against schema and domain rules.
    """
    errors: List[str] = []

    # Schema validation
    if jsonschema is not None:
        if schema_path is None:
            schema_path = str(resolve_repo_root() / "experiments" / "schema.json")
        if os.path.exists(schema_path):
            try:
                with open(schema_path, "r", encoding="utf-8") as f:
                    schema = json.load(f)
                validator = jsonschema.Draft202012Validator(schema)
                for err in validator.iter_errors(scenario):
                    path_str = ".".join(str(p) for p in err.path) or "root"
                    errors.append(f"Schema error at {path_str}: {err.message}")
            except Exception as e:
                errors.append(f"Failed to load JSON schema {schema_path}: {e}")

    # Domain validation
    if validate_scenario_domain is not None:
        domain_errors = validate_scenario_domain(scenario, source_name=scenario.get("id", "scenario"))
        errors.extend(domain_errors)
    else:
        # Fallback basic domain checks
        if "id" not in scenario:
            errors.append("Missing required field 'id'")
        if "duration_seconds" not in scenario or scenario["duration_seconds"] <= 0:
            errors.append("Missing or non-positive 'duration_seconds'")
        if "workload" not in scenario or not scenario["workload"].get("stages"):
            errors.append("Missing or empty workload stages")
        if "backends" not in scenario or len(scenario["backends"]) != 5:
            errors.append("Scenario must specify all 5 backends (backend-a through backend-e)")

    if errors:
        raise ValidationError(
            f"Scenario '{scenario.get('id', 'unknown')}' failed validation with {len(errors)} error(s)",
            errors=errors,
        )


def is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """Checks if a local TCP port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        res = s.connect_ex((host, port))
        if res == 0:
            return True
    return False


def verify_no_port_conflicts(proxy_port: int, admin_ports: Dict[str, int], host: str = "127.0.0.1") -> None:
    """Verifies all required ports are positive, distinct, and available."""
    all_ports: Dict[str, int] = {"proxy": proxy_port}
    for name, p in admin_ports.items():
        all_ports[name] = p

    # Distinct check
    seen: Dict[int, str] = {}
    for name, p in all_ports.items():
        if p <= 0 or p > 65535:
            raise ValidationError(f"Invalid port number {p} for {name}: must be between 1 and 65535")
        if p in seen:
            raise ValidationError(f"Duplicate port {p} configured for {name} and {seen[p]}")
        seen[p] = name

    # In-use check
    conflicts: Dict[str, int] = {}
    for name, p in all_ports.items():
        if is_port_in_use(p, host):
            conflicts[name] = p

    if conflicts:
        raise PortConflictError(conflicts)


@dataclass
class ExperimentConfig:
    """Complete specification for an experimental run."""

    scenario: Dict[str, Any]
    routing_policy: str
    repetitions: int = 1
    seed: int = 42
    output_dir: Path = field(default_factory=lambda: resolve_repo_root() / "experiments" / "results")
    compose_project: str = "sbr-experiment"
    proxy_port: int = DEFAULT_PROXY_PORT
    admin_ports: Dict[str, int] = field(default_factory=lambda: dict(DEFAULT_ADMIN_PORTS))
    k6_exec: str = "auto"  # 'auto', 'local', 'docker'
    vus_preallocated: int = 0
    vus_max: int = 0
    admin_token: str = ""
    http_timeout: float = 5.0
    readiness_timeout: float = 30.0
    dry_run: bool = False

    def __post_init__(self):
        self.routing_policy = normalize_policy(self.routing_policy)
        if self.repetitions < 1:
            raise ValidationError(f"Repetitions must be >= 1, got {self.repetitions}")
        if self.k6_exec not in ("auto", "local", "docker"):
            raise ValidationError(f"Invalid k6_exec mode '{self.k6_exec}': expected 'auto', 'local', or 'docker'")
        if not self.admin_token:
            self.admin_token = os.environ.get("SBR_BACKEND_ADMIN_TOKEN", "")

    def validate(self, check_ports: bool = True) -> None:
        """Validates scenario and configuration parameters before starting infrastructure."""
        validate_scenario(self.scenario)
        if not self.admin_token and not self.dry_run:
            raise ValidationError(
                "SBR_BACKEND_ADMIN_TOKEN is required. Pass via --admin-token or set the SBR_BACKEND_ADMIN_TOKEN environment variable."
            )
        if check_ports and not self.dry_run:
            verify_no_port_conflicts(self.proxy_port, self.admin_ports)
