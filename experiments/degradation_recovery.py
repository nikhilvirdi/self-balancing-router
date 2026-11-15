"""Audit and summarize the constant-rate degradation and recovery trials."""

import argparse
from collections import Counter
import csv
import gzip
import json
from pathlib import Path
from statistics import mean

from .orchestrator.config import load_scenario
from .orchestrator.data import compute_latency_stats, parse_iso_to_epoch
from .orchestrator.sync import parse_workload_start


SCENARIO = "scenario_8_degradation_recovery"
BACKENDS = tuple(f"backend-{suffix}" for suffix in "abcde")
PHASES = ("normal", "degraded", "recovery")
TOTAL_REQUESTS = 12800
# Distinct processes log realtime timestamps. Go's per-request durations use a monotonic clock.
EVENT_CLOCK_TOLERANCE_S = 0.010


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_metrics(path):
    return {
        key: float(value)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
        for key, value in [line.rsplit(maxsplit=1)]
    }


def metric_statuses(metrics):
    return Counter({
        key.split('"')[1]: int(count)
        for key, count in metrics.items()
        if key.startswith("sbr_proxy_requests_total{")
    })


def workload_start(path):
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            start = parse_workload_start(line)
            if start is not None:
                return start
    raise ValueError("k6 workload start token missing")


def phase_at(at, changes):
    return PHASES[sum(at >= change for change in changes)]


def raw_counts(path):
    statuses = Counter()
    active_vus = []
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            record = json.loads(line)
            if record["type"] != "Point":
                continue
            if record["metric"] == "sbr_request_observation":
                statuses[record["data"]["tags"]["status"]] += 1
            elif record["metric"] == "vus":
                active_vus.append(record["data"]["value"])
    return statuses, max(active_vus, default=None)


def event_traces(path):
    traces = {}
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            record = json.loads(line)
            event = record["event"]
            trace = traces.setdefault(event["request_id"], {})
            kind = event["kind"]
            if kind in trace:
                raise ValueError(f"Duplicate {kind} for {event['request_id']}")
            trace[kind] = (parse_iso_to_epoch(record["time"]), event)
    return traces


def verify_traces(traces, metrics, backend_metrics):
    """Cross-check all assignments, completions and overloads against counters."""
    statuses = Counter()
    routed = Counter()
    rejected = Counter()
    errors = set()
    inversions = 0
    worst_inversion = 0.0
    worst_proxy_duration_error = 0.0
    for trace in traces.values():
        done = trace.get("request_completed")
        if done is None:
            errors.add("Missing proxy completion")
            continue
        statuses[str(done[1]["status"])] += 1
        selected = trace.get("backend_selected")
        if selected is None:
            if done[1]["outcome"] not in ("unavailable", "internal_error"):
                errors.add("Proxy completion without matching selection")
            continue
        selected_at, s = selected
        backend = s["backend_id"]
        routed[backend] += 1
        in_flight_backends = {load["BackendID"] for load in s.get("in_flight", [])}
        if in_flight_backends != set(BACKENDS) or done[1]["backend_id"] != backend:
            errors.add("Invalid proxy correlation or in-flight snapshot")
        duration = done[1]["duration_ns"] / 1e9
        worst_proxy_duration_error = max(worst_proxy_duration_error, abs(done[0] - selected_at - duration))
        clock_inversion = max(0.0, selected_at - done[0])
        admitted = trace.get("queue_admitted")
        completed = trace.get("work_completed")
        rejection = trace.get("queue_rejected")
        if rejection and rejection[1]["outcome"] == "queue_full":
            rejected[backend] += 1
        if done[1]["status"] == 200 or (done[1]["status"] == 503 and not rejection):
            if not (admitted and completed and completed[1]["duration_ns"] >= 0 and completed[1]["queue_wait_ns"] >= 0):
                errors.add("Missing or inconsistent backend work events")
            else:
                clock_inversion = max(clock_inversion, selected_at - admitted[0], admitted[0] - completed[0], completed[0] - done[0])
        if clock_inversion:
            inversions += 1
            worst_inversion = max(worst_inversion, clock_inversion)
        if any(pair and pair[1]["backend_id"] != backend for pair in (admitted, completed, rejection)):
            errors.add("Backend identity differs from the proxy selection")
    if worst_inversion > EVENT_CLOCK_TOLERANCE_S or worst_proxy_duration_error > EVENT_CLOCK_TOLERANCE_S:
        errors.add("Event clocks disagree beyond the 10 ms alignment tolerance")
    if statuses != metric_statuses(metrics):
        errors.add("Diagnostic and operational proxy status counts differ")
    for backend in BACKENDS:
        if routed[backend] != metrics[f'sbr_proxy_routed_requests_total{{backend="{backend}"}}']:
            errors.add(f"Diagnostic and operational routing counts differ for {backend}")
        if rejected[backend] != backend_metrics[backend]['sbr_backend_rejections_total{reason="queue_full"}']:
            errors.add(f"Diagnostic and operational rejection counts differ for {backend}")
    return sorted(errors), {
        "wall_clock_inversions": inversions,
        "max_wall_clock_inversion_ms": worst_inversion * 1000,
        "max_proxy_wall_duration_error_ms": worst_proxy_duration_error * 1000,
    }


def audit(path):
    meta = read_json(path / "metadata.json")
    summary = read_json(path / "k6-summary.json")
    metrics = read_json(path / "metrics.json")
    events = read_json(path / "events.json")
    scenario = read_json(path / "scenario.json")
    start = workload_start(path / "k6-output.log")
    raw, active_vus = raw_counts(path / "raw-metrics.json.gz")
    with (path / "requests.csv").open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    statuses = Counter(row["status_code"] for row in rows)
    errors = []
    if not meta["valid"]:
        errors.append(meta["failure_reason"])
    if scenario != load_scenario(SCENARIO) or meta["scenario_definition"] != scenario:
        errors.append("Scenario differs from the registered protocol")
    if not (
        abs(len(rows) - TOTAL_REQUESTS) <= 3
        and raw == statuses == metrics["outcomes"]["status_distribution"]
        and len(rows) == summary["workload_integrity"]["total_requests"]
        and summary["workload_integrity"]["dropped_iterations"] == 0
    ):
        errors.append("Invalid offered load, dropped iterations, or inconsistent request counts")
    workload = meta["configuration"]["workload"]
    if (not workload.get("k6_compose_network") or workload["vus_preallocated"] != 400
            or workload["vus_max"] != 400 or active_vus is None or active_vus >= 360):
        errors.append("Unverified k6 networking or generator headroom")
    if meta["timing_summary"]["sync_method"] != "stdout" or len(events) != 2:
        errors.append("Expected stdout synchronization and two administrative events")
    for event, (time_s, action, delay) in zip(events, ((40, "update", "300ms"), (100, "reset", "20ms"))):
        trigger = event["triggered_at_epoch_seconds"] - start
        ack = event["acknowledged_at_epoch_seconds"]
        if not (
            event["action"] == action and event["target"] == "backend-a" and event["planned_timestamp_seconds"] == time_s
            and event["success"] and event["verified"] and abs(trigger - time_s) <= 0.1
            and ack is not None and 0 <= ack - event["triggered_at_epoch_seconds"] <= 0.1
            and event["observed_workload"] == {"processing_delay": delay, "processing_jitter": "5ms"}
        ):
            errors.append(f"Unverified or mistimed administrative event at {time_s}s")
    traces = {}
    event_clock = None
    observability = meta.get("observability")
    if observability is not None:
        if not (observability["complete"] and observability["sample_every"] == 1
                and observability["dropped_events"] == 0 and len(observability["snapshots"]) == 12):
            errors.append("Incomplete metrics or every-request diagnostics")
        before = read_metrics(path / "observability-before-proxy.prom")
        after = read_metrics(path / "observability-after-proxy.prom")
        backend_metrics = {b: read_metrics(path / f"observability-after-{b}.prom") for b in BACKENDS}
        for b in BACKENDS:
            baseline = read_metrics(path / f"observability-before-{b}.prom")
            if baseline["sbr_backend_workers_capacity"] != 4 or baseline["sbr_backend_queue_capacity"] != 32:
                errors.append(f"Unexpected capacity for {b}")
            if after[f'sbr_proxy_backend_health{{backend="{b}",state="healthy"}}'] != 1:
                errors.append(f"{b} was unhealthy at trial end")
        if metric_statuses(before) or metric_statuses(after) != statuses or after["sbr_proxy_request_duration_seconds_count"] != len(rows):
            errors.append("Proxy metrics and k6 request counts differ")
        traces = event_traces(path / "diagnostic-events.jsonl")
        trace_errors, event_clock = verify_traces(traces, after, backend_metrics)
        errors.extend(trace_errors)
    return {
        "policy": meta["routing_policy"], "seed": meta["random_seed"], "requests": len(rows),
        "peak_sampled_vus": active_vus, "statuses": dict(statuses), "errors": errors,
        "event_clock": event_clock,
        "event_timing": [
            {
                "trigger_seconds": e["triggered_at_epoch_seconds"] - start,
                "ack_seconds": e["acknowledged_at_epoch_seconds"] - start if e["acknowledged_at_epoch_seconds"] is not None else None,
            }
            for e in events
        ],
    }, rows, traces, start


def shares(counter):
    total = counter.total()
    return {b: counter[b] / total if total else 0 for b in BACKENDS}


def sustained_crossing(bins, meets):
    """Return the end of the first two consecutive qualifying five-second bins."""
    for first, second in zip(bins, bins[1:]):
        if first["starts"] and second["starts"] and meets(first["a_share"]) and meets(second["a_share"]):
            return second["from_change_s"] + 5
    return None


def analyze(audit_result, rows, traces, start):
    timing = audit_result["event_timing"]
    changes = [start + (event["trigger_seconds"] + event["ack_seconds"]) / 2 for event in timing]
    boundaries = (start, *changes, start + 160)
    intervals = {phase: {
        "status": Counter(), "failure_categories": Counter(), "latencies": [], "spanning_client": 0,
        "spanning_proxy": 0, "routing": Counter(), "rejections": Counter(), "queue_peak": Counter(),
        "in_flight_peak": Counter(), "total_in_flight": [], "queue_wait_ms": [],
    } for phase in PHASES}
    client_dispatches = []
    for row in rows:
        complete = parse_iso_to_epoch(row["timestamp_iso"])
        dispatch = complete - float(row["duration_ms"]) / 1000
        client_dispatches.append((dispatch, row["status_code"], float(row["duration_ms"])))
        phase = phase_at(dispatch, changes)
        i = intervals[phase]
        i["status"][row["status_code"]] += 1
        i["spanning_client"] += phase != phase_at(complete, changes)
        if row["status_code"] == "200":
            i["latencies"].append(float(row["duration_ms"]))
        else:
            category = f"503_{row['overload_origin']}" if row["status_code"] == "503" else row["outcome"]
            i["failure_categories"][category] += 1
    selections = []
    backend_a_queue_samples = []
    backend_a_feedback_crossing = Counter()
    for trace in traces.values():
        selected = trace.get("backend_selected")
        if selected:
            ts, event = selected
            phase = phase_at(ts, changes)
            i = intervals[phase]
            backend = event["backend_id"]
            i["routing"][backend] += 1
            i["spanning_proxy"] += phase != phase_at(trace["request_completed"][0], changes)
            load = {b["BackendID"]: b["Count"] for b in event["in_flight"]}
            i["total_in_flight"].append(sum(load.values()))
            for b, pending in load.items():
                i["in_flight_peak"][b] = max(i["in_flight_peak"][b], pending)
            selections.append((ts, backend))
        for kind in ("queue_admitted", "work_completed", "queue_rejected"):
            if kind not in trace:
                continue
            ts, event = trace[kind]
            b = event["backend_id"]
            i = intervals[phase_at(ts, changes)]
            i["queue_peak"][b] = max(i["queue_peak"][b], event["queued"])
            if kind == "queue_rejected" and event["outcome"] == "queue_full":
                i["rejections"][b] += 1
            if b == BACKENDS[0]:
                backend_a_queue_samples.append((ts, event["queued"], kind == "queue_rejected"))
            if kind == "work_completed" and event["outcome"] == "processed":
                i["queue_wait_ms"].append(event["queue_wait_ns"] / 1e6)
                if b == BACKENDS[0] and selected:
                    started = ts - event["duration_ns"] / 1e9
                    delivered = trace["request_completed"][0]
                    for change, destination in zip(changes, PHASES[1:]):
                        if selected[0] < change <= delivered:
                            key = f"{destination}_started_before_change" if started < change else f"{destination}_started_after_change"
                            backend_a_feedback_crossing[key] += 1
    for phase, begin, end in zip(PHASES, boundaries, boundaries[1:]):
        i = intervals[phase]
        count = i["status"].total()
        success = i["status"]["200"]
        duration = end - begin
        i.update({
            "successful_latency_ms": compute_latency_stats(i.pop("latencies")),
            "queue_wait_ms": compute_latency_stats(i["queue_wait_ms"]),
            "routing_share": shares(i["routing"]), "success_rate": success / count,
            "request_rate": count / duration, "successful_rate": success / duration,
            "mean_selection_in_flight": mean(i["total_in_flight"]) if i["total_in_flight"] else None,
        })
        del i["total_in_flight"]
    audit_result["intervals"] = intervals
    if selections:
        baseline = shares(Counter(b for ts, b in selections if changes[0] - 20 <= ts < changes[0]))[BACKENDS[0]]
        aligned = {}
        for name, (begin, end) in zip(PHASES[1:], zip(changes, boundaries[2:])):
            bins = []
            for offset in range(0, int(end - begin - 5) + 1, 5):
                left, right = begin + offset, begin + offset + 5
                counts = Counter(b for ts, b in selections if left <= ts < right)
                responses = [(code, duration) for ts, code, duration in client_dispatches if left <= ts < right]
                successful = [duration for code, duration in responses if code == "200"]
                queue = [(depth, reject) for ts, depth, reject in backend_a_queue_samples if left <= ts < right]
                bins.append({
                    "from_change_s": offset, "starts": counts.total(), "a_share": shares(counts)[BACKENDS[0]],
                    "http_200": len(successful), "client_requests": len(responses),
                    "successful_p95_ms": compute_latency_stats(successful)["p95"] if successful else None,
                    "a_queue_peak": max((depth for depth, _ in queue), default=None),
                    "a_rejections": sum(reject for _, reject in queue),
                })
            aligned[name] = bins
        degraded = shares(Counter(b for ts, b in selections if changes[1] - 20 <= ts < changes[1]))[BACKENDS[0]]
        audit_result.update({
            "baseline_a_share_last20": baseline, "degraded_a_share_last20": degraded, "aligned_a_share": aligned,
            "backend_a_feedback_crossing": backend_a_feedback_crossing,
            "adaptation_s": sustained_crossing(aligned["degraded"], lambda share: share <= baseline * 0.5) if baseline > 0 else None,
            "recovery_s": sustained_crossing(aligned["recovery"], lambda share: share >= baseline * 0.8) if baseline - degraded >= 0.05 else None,
        })
    return audit_result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trials", type=Path, nargs="+")
    args = parser.parse_args()
    failed = False
    for path in args.trials:
        try:
            result, rows, traces, start = audit(path)
            if not result["errors"]:
                analyze(result, rows, traces, start)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            result = {"errors": [str(exc)]}
        failed |= bool(result["errors"])
        print(json.dumps(dict(result, path=path.as_posix())), flush=True)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
