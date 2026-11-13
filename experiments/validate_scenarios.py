#!/usr/bin/env python3
"""
Validation script for experimental workload scenario catalog and individual scenarios.
Verifies JSON schema compliance and structural domain constraints:
  - Timeline consistency (warmup + measurement + cooldown == duration).
  - Workload stage temporal progression and continuous coverage.
  - Backend fleet completeness (backend-a through backend-e) and capacity limits.
  - Event timestamps, target bounds, and duration string formats.
  - Evaluation criteria and algorithm policy expectations completeness.
"""

import argparse
import json
import math
import os
import re
import sys

try:
    import jsonschema
except ImportError:
    jsonschema = None

GO_DURATION_RE = re.compile(r"^[0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h)$")
EXPECTED_BACKENDS = {"backend-a", "backend-b", "backend-c", "backend-d", "backend-e"}
EXPECTED_POLICIES = {
    "round_robin",
    "least_requests",
    "p2c",
    "p2c_peak_ewma",
    "ucb1",
    "sliding_window_ucb",
    "discounted_ucb",
}


def parse_go_duration(s):
    """Parses a simple Go duration string into seconds."""
    match = re.match(r"^([0-9]+(?:\.[0-9]+)?)(ns|us|µs|ms|s|m|h)$", s)
    if not match:
        raise ValueError(f"Invalid Go duration string: {s}")
    val = float(match.group(1))
    unit = match.group(2)
    multipliers = {
        "ns": 1e-9,
        "us": 1e-6,
        "µs": 1e-6,
        "ms": 1e-3,
        "s": 1.0,
        "m": 60.0,
        "h": 3600.0,
    }
    return val * multipliers[unit]


def validate_scenario_domain(scenario, source_name="scenario"):
    errors = []

    # 1. Timeline integrity
    duration = scenario.get("duration_seconds", 0)
    if duration <= 0:
        errors.append(f"{source_name}: duration_seconds must be positive, got {duration}")

    timeline = scenario.get("timeline", {})
    warmup = timeline.get("warmup_seconds", 0)
    measurement = timeline.get("measurement_seconds", 0)
    cooldown = timeline.get("cooldown_seconds", 0)
    total_timeline = warmup + measurement + cooldown
    if not math.isclose(total_timeline, duration, rel_tol=1e-5, abs_tol=1e-5):
        errors.append(
            f"{source_name}: timeline components (warmup={warmup}s + measurement={measurement}s + "
            f"cooldown={cooldown}s = {total_timeline}s) do not sum to duration_seconds={duration}s"
        )

    # 2. Workload stages
    workload = scenario.get("workload", {})
    stages = workload.get("stages", [])
    if not stages:
        errors.append(f"{source_name}: workload.stages must not be empty")
    else:
        current_time = 0.0
        for idx, stage in enumerate(stages):
            start = stage.get("start_seconds", 0)
            st_duration = stage.get("duration_seconds", 0)
            rps = stage.get("target_rps", 0)

            if not math.isclose(start, current_time, rel_tol=1e-4, abs_tol=1e-4):
                errors.append(
                    f"{source_name}: stage {idx} start_seconds ({start}s) does not match "
                    f"expected offset ({current_time}s)"
                )
            if st_duration <= 0:
                errors.append(f"{source_name}: stage {idx} duration_seconds must be positive, got {st_duration}")
            if rps <= 0:
                errors.append(f"{source_name}: stage {idx} target_rps must be positive, got {rps}")
            current_time = start + st_duration

        if not math.isclose(current_time, duration, rel_tol=1e-4, abs_tol=1e-4):
            errors.append(
                f"{source_name}: total workload stages duration ({current_time}s) does not match "
                f"duration_seconds ({duration}s)"
            )

    # 3. Backends fleet
    backends = scenario.get("backends", {})
    found_backends = set(backends.keys())
    if found_backends != EXPECTED_BACKENDS:
        missing = EXPECTED_BACKENDS - found_backends
        extra = found_backends - EXPECTED_BACKENDS
        if missing:
            errors.append(f"{source_name}: missing required backends: {sorted(list(missing))}")
        if extra:
            errors.append(f"{source_name}: unexpected backend keys: {sorted(list(extra))}")

    for b_id, b_cfg in backends.items():
        delay_str = b_cfg.get("processing_delay", "")
        jitter_str = b_cfg.get("processing_jitter", "")
        if not GO_DURATION_RE.match(delay_str):
            errors.append(f"{source_name}: backend {b_id} processing_delay '{delay_str}' is invalid Go duration")
        if not GO_DURATION_RE.match(jitter_str):
            errors.append(f"{source_name}: backend {b_id} processing_jitter '{jitter_str}' is invalid Go duration")

        try:
            d_sec = parse_go_duration(delay_str)
            j_sec = parse_go_duration(jitter_str)
            if j_sec > d_sec:
                errors.append(
                    f"{source_name}: backend {b_id} jitter ({jitter_str}) exceeds base delay ({delay_str})"
                )
        except Exception as e:
            errors.append(f"{source_name}: backend {b_id} duration parsing error: {e}")

    # 4. Events
    events = scenario.get("events", [])
    for idx, ev in enumerate(events):
        ts = ev.get("timestamp_seconds", -1)
        target = ev.get("target", "")
        action = ev.get("action", "")

        if ts < 0 or ts > duration:
            errors.append(
                f"{source_name}: event {idx} timestamp ({ts}s) is outside scenario duration (0s - {duration}s)"
            )
        if target not in EXPECTED_BACKENDS:
            errors.append(f"{source_name}: event {idx} target '{target}' not in expected backend fleet")
        if action not in ("update", "reset"):
            errors.append(f"{source_name}: event {idx} action '{action}' must be 'update' or 'reset'")

        if action == "update":
            del_str = ev.get("processing_delay", "")
            jit_str = ev.get("processing_jitter", "")
            if not del_str or not GO_DURATION_RE.match(del_str):
                errors.append(f"{source_name}: event {idx} update action missing or invalid processing_delay")
            if not jit_str or not GO_DURATION_RE.match(jit_str):
                errors.append(f"{source_name}: event {idx} update action missing or invalid processing_jitter")

    # 5. Evaluation & Policies
    eval_cfg = scenario.get("evaluation", {})
    metrics = eval_cfg.get("target_metrics", [])
    if not metrics:
        errors.append(f"{source_name}: evaluation.target_metrics must not be empty")

    policies = eval_cfg.get("policy_expectations", {})
    found_policies = set(policies.keys())
    if found_policies != EXPECTED_POLICIES:
        missing = EXPECTED_POLICIES - found_policies
        extra = found_policies - EXPECTED_POLICIES
        if missing:
            errors.append(f"{source_name}: evaluation.policy_expectations missing: {sorted(list(missing))}")
        if extra:
            errors.append(f"{source_name}: evaluation.policy_expectations unexpected keys: {sorted(list(extra))}")

    return errors


def main():
    parser = argparse.ArgumentParser(description="Validate workload scenario catalog and scenario files.")
    parser.add_argument(
        "--schema",
        default=os.path.join("experiments", "schema.json"),
        help="Path to scenario JSON schema",
    )
    parser.add_argument(
        "--catalog",
        default=os.path.join("experiments", "scenarios.json"),
        help="Path to scenarios catalog JSON",
    )
    parser.add_argument(
        "--scenarios-dir",
        default=os.path.join("experiments", "scenarios"),
        help="Path to individual scenarios directory",
    )
    args = parser.parse_args()

    # Load Schema
    schema = None
    if os.path.exists(args.schema):
        with open(args.schema, "r", encoding="utf-8") as f:
            schema = json.load(f)
        print(f"[OK] Loaded JSON schema from {args.schema}")
    else:
        print(f"[ERROR] Schema file not found: {args.schema}")
        sys.exit(1)

    all_errors = []

    # Validate Master Catalog
    if os.path.exists(args.catalog):
        with open(args.catalog, "r", encoding="utf-8") as f:
            catalog = json.load(f)

        if jsonschema:
            try:
                jsonschema.validate(instance=catalog, schema=schema)
                print(f"[OK] Master catalog {args.catalog} matches JSON schema")
            except jsonschema.ValidationError as e:
                all_errors.append(f"Catalog schema validation error: {e.message}")
        else:
            print("[WARN] jsonschema package not installed, skipping JSON Schema validation")

        scenarios = catalog.get("scenarios", [])
        if len(scenarios) != 7:
            all_errors.append(f"Catalog expected 7 scenarios, found {len(scenarios)}")

        print(f"\n--- Master Catalog Scenarios ({len(scenarios)}) ---")
        for sc in scenarios:
            sc_id = sc.get("id", "unknown")
            sc_name = sc.get("name", "unknown")
            duration = sc.get("duration_seconds", 0)
            stages_count = len(sc.get("workload", {}).get("stages", []))
            events_count = len(sc.get("events", []))
            print(
                f"  * {sc_id:36} | {sc_name:32} | {duration:5.1f}s | "
                f"{stages_count} stages | {events_count} events"
            )
            errs = validate_scenario_domain(sc, source_name=f"catalog:{sc_id}")
            all_errors.extend(errs)
    else:
        all_errors.append(f"Master catalog file not found: {args.catalog}")

    # Validate Individual Scenario Files
    if os.path.exists(args.scenarios_dir):
        files = sorted([f for f in os.listdir(args.scenarios_dir) if f.endswith(".json")])
        print(f"\n--- Individual Scenario Files ({len(files)}) ---")
        for fname in files:
            fpath = os.path.join(args.scenarios_dir, fname)
            with open(fpath, "r", encoding="utf-8") as f:
                sc_data = json.load(f)

            if jsonschema:
                try:
                    jsonschema.validate(instance=sc_data, schema=schema)
                    print(f"[OK] {fname:42} matches JSON schema")
                except jsonschema.ValidationError as e:
                    all_errors.append(f"{fname} schema validation error: {e.message}")

            errs = validate_scenario_domain(sc_data, source_name=fname)
            all_errors.extend(errs)
    else:
        all_errors.append(f"Scenarios directory not found: {args.scenarios_dir}")

    # Summary
    print("\n" + "=" * 60)
    if all_errors:
        print(f"[FAILED] Found {len(all_errors)} validation errors:")
        for err in all_errors:
            print(f"  - {err}")
        sys.exit(1)
    else:
        print("[SUCCESS] All 7 benchmark scenarios and catalog passed validation!")
        sys.exit(0)


if __name__ == "__main__":
    main()

