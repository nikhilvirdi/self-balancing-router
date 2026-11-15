"""Compute per-trial measurements, descriptive summaries, and paired effects."""

from collections import Counter, defaultdict
from math import comb
from statistics import mean, median, stdev

from .data import BACKENDS, FAILURES
from ..orchestrator.data import compute_latency_stats, parse_iso_to_epoch

BIN_SECONDS = 5
EVENT_WINDOW = (-15, 30)
REFERENCE_POLICY = "round-robin"
EFFECT_METRICS = ("p50_ms", "p95_ms", "p99_ms", "successful_rps", "success_pct", "backend_503_pct")


def latency_fields(values, prefix=""):
    stats = compute_latency_stats(values)
    return {f"{prefix}{field}_ms": stats[field] if values else None for field in ("p50", "p95", "p99")}


def client_statistics(requests, seconds, event_bounds):
    n = len(requests)
    successes = [r for r in requests if r.status == "200"]
    identified = Counter(r.backend for r in successes if r.backend)
    failures = Counter(r.failure for r in requests)
    statuses = Counter(r.status for r in requests)
    dispatches = [(r.dispatch, r.completion) for r in requests]
    return {
        **latency_fields([r.duration_ms for r in successes]),
        "requests": n, "successful": len(successes), "total_rps": n / seconds,
        "successful_rps": len(successes) / seconds,
        "success_pct": 100 * len(successes) / n if n else None,
        "unassigned_dispatches": sum(dispatch is None for dispatch, _ in dispatches),
        "boundary_spans": sum(dispatch is not None and any(dispatch < at <= completion for at in event_bounds)
                              for dispatch, completion in dispatches),
        "known_success_backend_pct": 100 * identified.total() / len(successes) if successes else None,
        **{f"status_{status}_count": count for status, count in sorted(statuses.items())},
        **{f"{kind}_count": failures[kind] for kind in FAILURES},
        **{f"{kind}_pct": 100 * failures[kind] / n if n else None for kind in FAILURES},
        **{f"{b}_successful_response_pct": 100 * identified[b] / identified.total() if identified else None
           for b in BACKENDS},
    }


def trace_observations(trial):
    selections, queue = [], []
    for trace in trial.traces.values():
        if "backend_selected" in trace:
            stamp, selected = trace["backend_selected"]
            done_stamp, done = trace["request_completed"]
            load = {entry["BackendID"]: entry["Count"] for entry in selected["in_flight"]}
            duration = done["duration_ns"] / 1e6 if done["status"] == 200 and done["outcome"] == "upstream_response" else None
            selections.append((stamp - trial.start, selected["backend_id"], load, done_stamp - trial.start, duration))
        for kind in ("queue_admitted", "queue_rejected", "work_completed"):
            if kind in trace:
                stamp, event = trace[kind]
                wait = event["queue_wait_ns"] / 1e6 if kind == "work_completed" and event["outcome"] == "processed" else None
                rejected = kind == "queue_rejected" and event["outcome"] == "queue_full"
                queue.append((stamp - trial.start, event["backend_id"], event["queued"], wait, rejected, kind))
    return selections, queue


def trace_statistics(selections, queue, event_bounds):
    assigned = Counter(b for _, b, _, _, _ in selections)
    rejected = Counter(b for _, b, _, _, reject, _ in queue if reject)
    attempts = Counter(b for _, b, _, _, _, kind in queue if kind in ("queue_admitted", "queue_rejected"))
    values = {
        "proxy_assignments": len(selections),
        "proxy_boundary_spans": sum(any(at < boundary <= done for boundary in event_bounds)
                                    for at, _, _, done, _ in selections),
        "mean_selection_in_flight": mean(sum(load.values()) for _, _, load, _, _ in selections) if selections else None,
        "backend_queue_full_count": rejected.total(),
        "backend_queue_full_attempt_pct": 100 * rejected.total() / attempts.total() if attempts else None,
        **latency_fields([wait for _, _, _, wait, _, _ in queue if wait is not None], "queue_wait_"),
        **latency_fields([duration for _, _, _, _, duration in selections if duration is not None],
                         "proxy_http200_forwarding_"),
    }
    for b in BACKENDS:
        values.update({
            f"{b}_assignments": assigned[b],
            f"{b}_assignment_pct": 100 * assigned[b] / len(selections) if selections else None,
            f"{b}_queue_full_count": rejected[b],
            f"{b}_queue_full_attempt_pct": 100 * rejected[b] / attempts[b] if attempts[b] else None,
            f"{b}_in_flight_peak": max((load[b] for _, _, load, _, _ in selections), default=None),
            f"{b}_queue_peak": max((depth for _, backend, depth, _, _, _ in queue if backend == b), default=None),
        })
    return values


def windows(duration, events):
    yield "whole", None, -float("inf"), float("inf"), duration
    if not events:
        return
    names = ["normal", "degraded"] + (["recovery"] if "recovery" in events else [])
    bounds = [0, *events.values(), duration]
    for i, name in enumerate(names):
        lower = bounds[i] if i else -float("inf")
        upper = bounds[i + 1] if i + 1 < len(names) else float("inf")
        yield name, None, lower, upper, bounds[i + 1] - bounds[i]
    for i, (name, event) in enumerate(events.items(), 1):
        for offset in range(*EVENT_WINDOW, BIN_SECONDS):
            lower, upper = event + offset, event + offset + BIN_SECONDS
            if lower >= bounds[i - 1] and upper <= bounds[i + 1]:
                yield f"event_{name}", offset, lower, upper, BIN_SECONDS


def measure_trial(trial):
    spec = trial.spec
    selections, queue = trace_observations(trial)
    event_bounds = tuple(trial.events.values())
    measured = []
    for scope, offset, lower, upper, duration in windows(trial.duration, trial.events):
        requests = trial.requests if scope == "whole" else [
            r for r in trial.requests if (dispatch := r.dispatch) is not None and lower <= dispatch < upper]
        metrics = client_statistics(requests, duration, event_bounds)
        if trial.traces:
            metrics.update(trace_statistics([r for r in selections if lower <= r[0] < upper],
                                            [r for r in queue if lower <= r[0] < upper], event_bounds))
        measured.append({"scenario": spec.scenario, "policy": spec.policy, "block": spec.block, "seed": spec.seed,
                         "trial": spec.path, "scope": scope, "offset_seconds": offset, "window_seconds": duration,
                         "metrics": metrics})
    return measured


def describe(values):
    return {"n": len(values), "median": median(values), "min": min(values), "max": max(values),
            "mean": mean(values), "sd": stdev(values) if len(values) > 1 else None}


def group_trials(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["scenario"], row["policy"], row["scope"], row["offset_seconds"])].append(row)
    return groups


def aggregate(rows):
    result = []
    for (scenario, policy, scope, offset), rows in group_trials(rows).items():
        values = defaultdict(list)
        status_metrics = {metric for row in rows for metric in row["metrics"] if metric.startswith("status_")}
        for row in rows:
            for metric in status_metrics - row["metrics"].keys():
                values[metric].append(0)
            for metric, value in row["metrics"].items():
                if value is not None:
                    values[metric].append(value)
        result.append({"scenario": scenario, "policy": policy, "scope": scope, "offset_seconds": offset,
                       "n": len(rows), "metrics": {name: describe(metric) for name, metric in values.items()}})
    return result


def median_interval(values, confidence=.95):
    """Exact order-statistic interval under independent continuous paired differences."""
    ordered = sorted(values)
    n = len(ordered)
    interval = None
    for trim in range((n + 1) // 2):
        coverage = 1 - 2 * sum(comb(n, i) / 2**n for i in range(trim + 1))
        if coverage >= confidence:
            interval = {"lower": ordered[trim], "upper": ordered[-1 - trim], "coverage": coverage}
        else:
            break
    return interval


def paired_effects(rows, reference=REFERENCE_POLICY):
    groups = group_trials([r for r in rows if r["offset_seconds"] is None])
    result = []
    for (scenario, policy, scope, _), rows in groups.items():
        if policy == reference:
            continue
        base = groups[(scenario, reference, scope, None)]
        ref = {(r["block"], r["seed"]): r for r in base}
        candidate = {(r["block"], r["seed"]): r for r in rows}
        if len(ref) != len(base) or len(candidate) != len(rows) or ref.keys() != candidate.keys():
            raise ValueError(f"Unmatched seed blocks for {scenario} {policy} {scope}")
        for metric in EFFECT_METRICS:
            pairs = []
            for block, seed in sorted(candidate):
                value = candidate[block, seed]["metrics"].get(metric)
                baseline = ref[block, seed]["metrics"].get(metric)
                if value is not None and baseline is not None:
                    pairs.append({"block": block, "seed": seed, "delta": value - baseline})
            if pairs:
                deltas = [p["delta"] for p in pairs]
                result.append({"scenario": scenario, "policy": policy, "scope": scope, "metric": metric,
                               "reference": reference, "pairs": pairs, **describe(deltas),
                               "median_ci95": median_interval(deltas)})
    return result
