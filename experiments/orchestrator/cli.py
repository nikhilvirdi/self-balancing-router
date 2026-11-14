"""
Command-line interface for self-balancing-router experiment orchestrator.
"""

import argparse
import logging
import os
from pathlib import Path
import sys
from typing import Dict, List

from .config import (
    DEFAULT_ADMIN_PORTS,
    DEFAULT_PROXY_PORT,
    SUPPORTED_POLICIES,
    ExperimentConfig,
    ValidationError,
    load_scenario,
    normalize_policy,
    resolve_repo_root,
)
from .runner import ExperimentRunner, TrialResult


def parse_admin_ports(arg: str) -> Dict[str, int]:
    """Parses admin ports from comma-separated list or name=port pairs."""
    backends = ["backend-a", "backend-b", "backend-c", "backend-d", "backend-e"]
    ports: Dict[str, int] = {}
    tokens = [t.strip() for t in arg.split(",") if t.strip()]

    if len(tokens) == 5 and all(t.isdigit() for t in tokens):
        for b_id, p_str in zip(backends, tokens):
            ports[b_id] = int(p_str)
        return ports

    for token in tokens:
        if "=" in token:
            name, val = token.split("=", 1)
            ports[name.strip()] = int(val.strip())
        else:
            raise ValueError(
                f"Invalid admin ports format: '{arg}'. Expected 5 comma-separated integers or 'backend-a=19091,...'"
            )

    for b in backends:
        if b not in ports:
            ports[b] = DEFAULT_ADMIN_PORTS[b]
    return ports


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_experiment",
        description="Reproducible experiment orchestrator for self-balancing-router.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        default="scenario_1_homogeneous",
        help="Scenario ID, index (1-7), or path to scenario JSON",
    )
    parser.add_argument(
        "--catalog",
        default=None,
        help="Path to scenario catalog JSON (default: experiments/scenarios.json)",
    )
    parser.add_argument(
        "--policy",
        default="round-robin",
        choices=sorted(list(SUPPORTED_POLICIES)),
        help="Routing policy to evaluate",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=1,
        help="Number of independent experimental trials to execute",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Base pseudorandom seed for the experiment",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to store experimental results (default: experiments/results)",
    )
    parser.add_argument(
        "--compose-project",
        default="sbr-experiment",
        help="Isolated Docker Compose project name",
    )
    parser.add_argument(
        "--proxy-port",
        type=int,
        default=DEFAULT_PROXY_PORT,
        help="Port on which the reverse proxy will listen",
    )
    parser.add_argument(
        "--admin-ports",
        default="19091,19092,19093,19094,19095",
        help="Comma-separated admin ports for backend-a through backend-e",
    )
    parser.add_argument(
        "--k6-exec",
        choices=["auto", "local", "docker"],
        default="auto",
        help="Method to execute Grafana k6 workload generator",
    )
    parser.add_argument(
        "--vus-preallocated",
        type=int,
        default=0,
        help="Explicit override for k6 preallocated virtual users",
    )
    parser.add_argument(
        "--vus-max",
        type=int,
        default=0,
        help="Explicit override for k6 maximum virtual users",
    )
    parser.add_argument(
        "--admin-token",
        default=None,
        help="Secret administrative token (defaults to SBR_BACKEND_ADMIN_TOKEN env var)",
    )
    parser.add_argument(
        "--http-timeout",
        type=float,
        default=5.0,
        help="Timeout in seconds for administrative HTTP requests",
    )
    parser.add_argument(
        "--readiness-timeout",
        type=float,
        default=30.0,
        help="Timeout in seconds for container health polling",
    )
    parser.add_argument(
        "--note",
        default="",
        help="Custom observation or description to attach to trial metadata and research journal",
    )
    parser.add_argument(
        "--hypothesis",
        default="",
        help="Scientific hypothesis being tested during this experiment evaluation round",
    )
    parser.add_argument(
        "--raw-metrics",
        choices=["gz", "json", "none"],
        default="gz",
        help="Raw measurement preservation mode ('gz' compressed, 'json' raw, or 'none')",
    )
    parser.add_argument(
        "--rebuild-index",
        action="store_true",
        help="Scans experiments/results/ and rebuilds index.json and INDEX.md from disk",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate scenario and configuration without launching containers or traffic",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable detailed debug logging",
    )
    return parser


def main(argv: List[str] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("sbr.orchestrator.cli")

    repo_root = resolve_repo_root()

    try:
        # Load and resolve scenario
        scenario = load_scenario(args.scenario, catalog_path=args.catalog)

        # Parse ports
        admin_ports = parse_admin_ports(args.admin_ports)

        # Output directory
        output_dir = Path(args.output_dir) if args.output_dir else (repo_root / "experiments" / "results")

        if args.rebuild_index:
            from .history import ExperimentHistory
            history = ExperimentHistory(output_dir)
            discovered_count = history.scan_and_rebuild()
            print(f"[SUCCESS] Rebuilt experiment index ({output_dir / 'index.json'} and {output_dir / 'INDEX.md'}) across {discovered_count} trials.")
            return 0

        # Admin token
        token = args.admin_token or os.environ.get("SBR_BACKEND_ADMIN_TOKEN", "")
        if not token and not args.dry_run:
            # Check if ephemeral token can be generated if not specified
            import secrets
            token = secrets.token_hex(16)
            logger.info("Generated ephemeral administrative token for experiment session.")

        config = ExperimentConfig(
            scenario=scenario,
            routing_policy=args.policy,
            repetitions=args.repetitions,
            seed=args.seed,
            output_dir=output_dir,
            compose_project=args.compose_project,
            proxy_port=args.proxy_port,
            admin_ports=admin_ports,
            k6_exec=args.k6_exec,
            vus_preallocated=args.vus_preallocated,
            vus_max=args.vus_max,
            admin_token=token,
            http_timeout=args.http_timeout,
            readiness_timeout=args.readiness_timeout,
            dry_run=args.dry_run,
            note=args.note,
            hypothesis=args.hypothesis,
            raw_metrics=args.raw_metrics,
        )

        config.validate()

        runner = ExperimentRunner(config)
        results = runner.run_all()

        # Display summary report
        print("\n" + "=" * 78)
        print(" EXPERIMENT EXECUTION SUMMARY REPORT")
        print("=" * 78)
        print(f" Scenario:  {config.scenario.get('id')} ({config.scenario.get('name')})")
        print(f" Policy:    {config.routing_policy}")
        print(f" Trials:    {len(results)}")
        print("-" * 78)

        all_valid = True
        for r in results:
            status = "VALID" if r.valid else "INVALID"
            timing = r.metadata.get("timing_summary", {})
            max_drift_ms = timing.get("max_scheduling_drift_seconds", 0.0) * 1000.0
            max_lat_ms = timing.get("max_request_latency_seconds", 0.0) * 1000.0
            sync_m = timing.get("sync_method", "none")

            print(
                f" Run #{r.trial_index:03d}: [{status}] | Sync: {sync_m:7s} | "
                f"Max Drift: {max_drift_ms:6.1f}ms | Max Latency: {max_lat_ms:6.1f}ms | Dir: {r.run_dir.name}"
            )
            if not r.valid and r.failure_reason:
                print(f"          Reason: {r.failure_reason}")
                all_valid = False

        print("=" * 78 + "\n")
        return 0 if all_valid else 1

    except ValidationError as e:
        print(f"\n[VALIDATION ERROR] {e}", file=sys.stderr)
        if hasattr(e, "errors") and e.errors:
            for err in e.errors:
                print(f"  - {err}", file=sys.stderr)
        return 1
    except Exception as e:
        logger.exception("Experiment execution failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
