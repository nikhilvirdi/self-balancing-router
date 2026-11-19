"""Select the archived matched cohorts and validate their source records."""

from collections import Counter
import csv
from dataclasses import dataclass
import gzip
import json
import math
from pathlib import Path
import re
from statistics import median

from ..degradation_recovery import BACKENDS, audit as audit_recovery, read_json, workload_start
from ..orchestrator.config import SUPPORTED_POLICIES
from ..orchestrator.data import REQUESTS_CSV_COLUMNS, compute_latency_stats, parse_iso_to_epoch

STABLE = "scenario_2_heterogeneous"
SUDDEN = "scenario_3_sudden_degradation"
RECOVERY = "scenario_8_degradation_recovery"
SCENARIOS = (STABLE, SUDDEN, RECOVERY)
POLICIES = ("first-eligible", "round-robin", "least-requests", "p2c", "p2c-peak-ewma",
            "ucb1", "sliding-window-ucb", "discounted-ucb")
PILOT_MANIFEST = "exploratory-pilot-2026-11-14-manifest.jsonl"
RECOVERY_MANIFEST = f"{RECOVERY}/matched-evaluation-manifest.jsonl"
PILOT_REPORT = "exploratory-pilot-2026-11-14.md"
RECOVERY_TABLE = f"{RECOVERY}/comparison-intervals.csv"
RECOVERY_REPORT = f"{RECOVERY}/EVALUATION.md"
POLICY_DEFAULTS = {
    "SBR_PROXY_P2C_SEED": "1",
    "SBR_PROXY_PEAK_EWMA_DECAY": "10s",
    "SBR_PROXY_PEAK_EWMA_INITIAL_RTT": "10ms",
    "SBR_PROXY_MAB_LATENCY_SCALE": "100ms",
    "SBR_PROXY_MAB_SW_WINDOW": "1000",
    "SBR_PROXY_MAB_DISCOUNT_FACTOR": "0.99",
    "SBR_PROXY_MAB_PENDING_PENALTY": "0.05",
}
FAILURES = ("transport", "backend_503", "proxy_503", "unknown_503", "other_http")


@dataclass(frozen=True)
class TrialSpec:
    scenario: str
    policy: str
    block: int
    seed: int
    path: str
    revision: str
    requests: int


@dataclass(frozen=True)
class Request:
    status: str
    failure: str
    backend: str
    duration_ms: float
    completion: float

    @property
    def dispatch(self):
        if self.status == "0" and self.duration_ms == 0:
            return None
        return self.completion - self.duration_ms / 1000


@dataclass
class Trial:
    spec: TrialSpec
    requests: list[Request]
    duration: float
    events: dict[str, float]
    traces: dict
    start: float
    protocol: dict
    quality: dict
    artifacts: list[Path]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_jsonl(path):
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def recovery_results(records):
    """A revalidation may revise the result of the same archived attempt."""
    selected = {}
    for item in records:
        if item["kind"] not in ("result", "revalidation"):
            continue
        key = (item["rep"], item["position"])
        previous = selected.get(key)
        if item["kind"] == "revalidation":
            fields = ("policy", "seed", "project", "path")
            require(previous and all(item[field] == previous[field] for field in fields),
                    f"Revalidation changed attempt identity for {key}")
        else:
            require(previous is None, f"Duplicate comparison attempt for {key}")
        selected[key] = item
    return list(selected.values())


def discover(root):
    pilot = read_jsonl(root / PILOT_MANIFEST)
    recovery = read_jsonl(root / RECOVERY_MANIFEST)
    plan, protocol = pilot[0], recovery[0]
    require(plan["kind"] == "plan" and pilot[-1]["kind"] == "complete"
            and protocol["kind"] == "protocol" and recovery[-1]["kind"] == "complete",
            "Expected both completed campaign manifests")
    pilot_rows = [row for row in pilot if row["kind"] == "trial" and row["valid"] is True]
    require(Counter((r["rep"], r["scenario"], r["policy"]) for r in pilot_rows)
            == Counter(map(tuple, plan["schedule"])), "Pilot trials differ from the planned matched blocks")
    recovery_rows = [row for row in recovery_results(recovery) if row["valid"] is True]
    controls = {row["path"] for row in protocol["controls"]}
    require(not any(row["path"] in controls for row in recovery_rows), "Recovery control in comparison cohort")
    require(not any(row["path"] == plan["original_invalid"] for row in pilot_rows), "Pilot control in comparison cohort")
    require(set(POLICIES) <= SUPPORTED_POLICIES, "Archived policy is no longer supported")
    specs = [
        TrialSpec(row["scenario"], row["policy"], row["rep"], plan["base_seed"] + row["rep"] - 1,
                  row["path"], plan["git_commit"], row["requests"])
        for row in pilot_rows
    ]
    for row in recovery_rows:
        require(row["seed"] == protocol["seeds"][row["rep"] - 1], "Recovery seed differs from matched block")
        specs.append(TrialSpec(RECOVERY, row["policy"], row["rep"], row["seed"], row["path"],
                               protocol["git_commit"], row["audit"]["requests"]))
    expected = {(scenario, policy, block) for scenario in SCENARIOS for policy in POLICIES for block in (1, 2, 3)}
    actual = {(spec.scenario, spec.policy, spec.block) for spec in specs}
    require(len(specs) == len(expected) and actual == expected, "Incomplete or duplicate matched trial grid")
    require(len({spec.path for spec in specs}) == len(specs), "Comparison trial path used twice")
    return sorted(specs, key=lambda s: (SCENARIOS.index(s.scenario), POLICIES.index(s.policy), s.block))


def csv_records(path):
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        require(reader.fieldnames == REQUESTS_CSV_COLUMNS, "Unexpected request CSV schema")
        return list(reader)


def classify(status, origin):
    if status == "200":
        return "success"
    if status == "0":
        return "transport"
    if status == "503":
        require(origin in ("backend", "proxy", "unknown"), "Missing or invalid HTTP 503 origin")
        return f"{origin}_503"
    return "other_http"


def parse_requests(rows, spec, start):
    requests = []
    for index, row in enumerate(rows):
        status = row["status_code"]
        duration = float(row["duration_ms"])
        completion = parse_iso_to_epoch(row["timestamp_iso"])
        require(row["request_index"] == str(index) and row["scenario_id"] == spec.scenario
                and row["routing_policy"] == spec.policy, f"Request identity mismatch at row {index}")
        require((status == "0" or (status.isdecimal() and 100 <= int(status) <= 599))
                and math.isfinite(duration) and duration >= 0 and completion > 0,
                f"Invalid status, duration, or timestamp at row {index}")
        require(row["backend_id"] in ("", *BACKENDS), f"Unknown backend at row {index}")
        failure = classify(status, row["overload_origin"])
        outcome = {"success": "success", "transport": "transport_error", "other_http": "http_error"}.get(
            failure, "overload_503")
        require(row["outcome"] == outcome and row["is_success"] == str(status == "200")
                and row["is_transport_error"] == str(status == "0") and row["is_overload"] == str(status == "503"),
                f"Conflicting client classifications at row {index}")
        require(status != "200" or duration > 0, f"Missing successful latency at row {index}")
        requests.append(Request(status, failure, row["backend_id"], duration, completion - start))
    return requests


def raw_errors(path, rows):
    def key(ts, status, duration, backend, origin):
        return ts, status, float(duration), backend, origin

    expected = Counter(key(row["timestamp_iso"], row["status_code"], row["duration_ms"],
                           row["backend_id"], row["overload_origin"]) for row in rows)
    actual, errors = Counter(), Counter()
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            if row["type"] != "Point" or row["metric"] != "sbr_request_observation":
                continue
            data = row["data"]
            tags = data["tags"]
            status = tags["status"]
            actual[key(data["time"], status, data["value"], tags["backend_id"], tags["overload_origin"])] += 1
            if status == "0":
                message = tags["error"].lower()
                category = "timeout" if "timeout" in message or "deadline exceeded" in message else "other_or_unknown"
                errors[(category, tags["phase"])] += 1
    require(expected == actual, "Raw k6 and CSV request observations differ")
    return errors


def validate_summaries(requests, metrics, k6, spec, nominal):
    counts = Counter(r.status for r in requests)
    failures = Counter(r.failure for r in requests)
    outcome = metrics["outcomes"]
    require(len(requests) == spec.requests and abs(len(requests) - nominal) <= 3
            and counts == outcome["status_distribution"] and counts["200"] == outcome["successful_requests"],
            "CSV, manifest, or metrics request counts differ")
    for summary in (metrics, k6):
        integrity = summary["workload_integrity"]
        require(integrity["total_requests"] == len(requests) and integrity["dropped_iterations"] == 0
                and integrity["valid_offered_load"] is True, "Incomplete workload or dropped iterations")
    k6out = k6["outcomes"]
    observed = {origin: failures[f"{origin}_503"] for origin in ("backend", "proxy", "unknown")}
    require(k6out["successful_200"] == counts["200"] and k6out["overload_503"] == counts["503"]
            and k6out["other_http_errors"] == outcome["http_error_count"] == failures["other_http"]
            and k6out["transport_errors"] == outcome["transport_error_count"] == counts["0"]
            and k6out["overload_503_breakdown"] == outcome["overload_503_breakdown"] == observed,
            "k6 and CSV outcome classifications differ")
    lat = compute_latency_stats([r.duration_ms for r in requests if r.status == "200"])
    metrics_lat = metrics["latency_ms"]["successful_requests"]
    require(metrics_lat["count"] == lat["count"] and all(
        math.isclose(lat[field], metrics_lat[field], rel_tol=1e-9)
        and math.isclose(lat[field], k6["latency_ms"]["successful_only"][source_field], rel_tol=1e-9)
        for field, source_field in (("p50", "med"), ("p95", "p95"), ("p99", "p99"))
    ), "Successful latency percentiles differ from archived summaries")
    names = Counter(r.backend for r in requests if r.backend)
    require(names == Counter({b: info["requests"] for b, info in metrics["backend_traffic_distribution"].items()}),
            "CSV and metrics backend response counts differ")


def load_trial(root, spec):
    relative = Path(spec.path)
    require(relative.parts[:2] == (spec.scenario, spec.policy) and len(relative.parts) == 3
            and relative.parts[2].startswith("run-") and relative.parts[2][4:].isdigit(), "Invalid trial path")
    path = root / relative
    require(path.resolve().is_relative_to(root.resolve()), "Trial path escapes archive")
    files = [path / name for name in ("metadata.json", "scenario.json", "metrics.json", "k6-summary.json",
                                      "events.json", "k6-output.log", "requests.csv", "raw-metrics.json.gz",
                                      "compose.override.yaml")]
    for file in files:
        require(file.is_file(), f"Missing trial artifact {file.name}")
    meta, scenario, metrics, k6, events = (read_json(file) for file in files[:5])
    require(meta["valid"] is True and meta["git_commit"] == spec.revision and meta["scenario_id"] == spec.scenario
            and meta["routing_policy"] == spec.policy and meta["random_seed"] == spec.seed,
            "Trial metadata conflicts with the manifest")
    require(meta["scenario_definition"] == scenario and scenario["id"] == spec.scenario
            and meta["backend_seeds"] == {b: spec.seed + i for i, b in enumerate(BACKENDS, 1)}
            and meta["backend_configurations"]["baseline"] == scenario["backends"], "Scenario or backend seeds differ")
    cfg = meta["configuration"]
    workload = cfg["workload"]
    policy = dict(cfg["policy_configuration"])
    require(policy.pop("SBR_PROXY_POLICY") == spec.policy and (POLICY_DEFAULTS | policy) == POLICY_DEFAULTS,
            "Unexpected policy configuration")
    duration = float(scenario["duration_seconds"])
    nominal = sum(stage["target_rps"] * stage["duration_seconds"] for stage in scenario["workload"]["stages"])
    is_recovery = spec.scenario == RECOVERY
    require(workload["vus_max"] == workload["vus_preallocated"] == 400 and cfg["http_timeout"] == 5
            and bool(workload.get("k6_compose_network")) == is_recovery
            and bool(meta.get("observability")) == is_recovery
            and meta["timing_summary"]["sync_method"] == "stdout", "Unexpected network, instrumentation, or workload settings")
    traces = {}
    quality = {}
    if is_recovery:
        checked, rows, traces, start = audit_recovery(path)
        require(not checked["errors"] and traces, "Recovery trace audit failed: " + "; ".join(checked["errors"]))
        files.extend([path / "diagnostic-events.jsonl", *sorted(path.glob("observability-*.prom"))])
        quality["event_clock"] = checked["event_clock"]
    else:
        rows, start = csv_records(files[6]), workload_start(files[5])
    requests = parse_requests(rows, spec, start)
    errors = raw_errors(files[7], rows)
    quality["transport_by_k6_phase"] = {f"{category}/{phase}": n for (category, phase), n in sorted(errors.items())}
    validate_summaries(requests, metrics, k6, spec, nominal)
    require(len(events) == len(scenario["events"]), "Missing administrative event")
    changes = {}
    for event, planned in zip(events, scenario["events"]):
        trigger, latency = event["actual_trigger_relative_seconds"], event["request_latency_seconds"]
        require(event["success"] is True and event["verified"] is True and event["target"] == planned["target"]
                and event["action"] == planned["action"] and event["planned_timestamp_seconds"] == planned["timestamp_seconds"]
                and abs(trigger - planned["timestamp_seconds"]) <= .1 and 0 <= latency <= .1,
                "Invalid or mistimed workload change")
        if event["action"] == "update":
            require(all(event["parameters"][key] == planned[key] for key in ("processing_delay", "processing_jitter")),
                    "Applied workload change differs from the scenario")
        at = ((event["triggered_at_epoch_seconds"] + event["acknowledged_at_epoch_seconds"]) / 2 - start
              if is_recovery else trigger + latency / 2)
        changes["degradation" if event["action"] == "update" else "recovery"] = at
    require(list(changes.values()) == sorted(changes.values()) and all(0 < at < duration for at in changes.values()),
            "Event falls outside the workload or out of order")
    quality["archive_completion_span_rps"] = metrics["workload_integrity"]["total_throughput_rps"]
    quality["archive_boundary_spans"] = metrics["workload_integrity"]["timing_semantics"]["total_boundary_spanning_requests"]
    protocol = {"scenario": scenario, "revision": spec.revision, "versions": meta["versions"],
                "workload": workload, "client_timeout": cfg["http_timeout"], "policy_defaults": POLICY_DEFAULTS,
                "sample_every": meta["observability"]["sample_every"] if is_recovery else 0}
    return Trial(spec, requests, duration, changes, traces, start, protocol, quality, files)


def verify_reference_reports(root, measurements):
    by_key = {(r["trial"], r["scope"]): r["metrics"] for r in measurements if r["offset_seconds"] is None}
    checked = set()
    for line in (root / PILOT_REPORT).read_text(encoding="utf-8").splitlines():
        parts = line.split("|")
        if len(parts) < 5 or parts[1].strip() not in POLICIES:
            continue
        links = re.findall(r"\]\((scenario_[23][^)]*/run-\d+)/\)", parts[2])
        if not links:
            continue
        require(len(links) == 3 and len(set(links)) == 3, "Unexpected pilot report trial links")
        key = (links[0].split("/")[0], parts[1].strip())
        require(all(tuple(path.split("/")[:2]) == key for path in links), "Pilot report mixes policy groups")
        require(key not in checked, "Duplicate pilot report group")
        expected = re.findall(r"([\d.]+)\s+\[", parts[3])
        require(len(expected) == 3, "Missing pilot report percentile")
        for metric, reported in zip(("p50_ms", "p95_ms", "p99_ms"), expected):
            value = median(by_key[(link, "whole")][metric] for link in links)
            require(abs(value - float(reported)) <= .05000001, f"Pilot report mismatch for {key} {metric}")
        checked.add(key)
    require(len(checked) == 2 * len(POLICIES), "Incomplete pilot report cross-check")
    phases = set()
    with (root / RECOVERY_TABLE).open(encoding="utf-8", newline="") as stream:
        for old in csv.DictReader(stream):
            key = (old["trial"], old["phase"])
            new = by_key[key]
            fields = ("requests", "successful", "p50_ms", "p95_ms", "p99_ms", "mean_selection_in_flight",
                      "queue_wait_p95_ms", *(f"{b}_{m}" for b in BACKENDS for m in ("assignments", "in_flight_peak")))
            for metric in fields:
                require(math.isclose(new[metric], float(old[metric]), rel_tol=1e-9, abs_tol=1e-12),
                        f"Recovery report mismatch for {key} {metric}")
            for b in BACKENDS:
                require(new[f"{b}_queue_full_count"] == int(old[f"{b}_queue_rejections"]),
                        f"Recovery report rejection counts differ for {key} {b}")
                measured = new[f"{b}_queue_peak"]
                require(measured == int(old[f"{b}_queue_peak"]) if measured is not None else old[f"{b}_queue_peak"] == "0",
                        f"Recovery report queue peak differs for {key} {b}")
            require(key not in phases, "Duplicate recovery report phase")
            phases.add(key)
    require(len(phases) == 3 * 24, "Incomplete recovery report cross-check")
    return {"pilot_policy_scenario_percentile_rows": len(checked), "recovery_trial_phase_rows": len(phases)}
