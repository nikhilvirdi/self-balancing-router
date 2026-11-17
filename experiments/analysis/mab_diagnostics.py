"""Audit and analyze request-level routing mechanism traces."""

import argparse
from bisect import bisect_right
from collections import Counter, defaultdict, deque
import csv
from itertools import groupby
import json
from math import isclose, isfinite, log, sqrt
from pathlib import Path
from statistics import mean

from ..degradation_recovery import (
    BACKENDS, event_traces, raw_counts, read_json, read_metrics, shares, sustained_crossing, verify_traces, workload_start,
)
from ..orchestrator.data import compute_latency_stats
from .data import POLICY_DEFAULTS, require

MAB = ("ucb1", "sliding-window-ucb", "discounted-ucb")
BURST_LENGTH = 8


def feedback_reward(completion, scale_ms):
    status = completion["status"]
    outcome = completion["outcome"]
    if outcome == "upstream_response":
        if 200 <= status < 300:
            return 1 / (1 + completion["duration_ns"] / (scale_ms * 1e6))
        if 500 <= status < 600:
            return 0.0
    elif outcome in ("upstream_timeout", "transport_failure"):
        return 0.0
    return None


def selection_winner(decision, pending):
    """Check the recorded candidates using the same precedence as the selector."""
    arms = decision["arms"]
    require(arms and len({arm["backend_id"] for arm in arms}) == len(arms), "Duplicate or missing decision arm")
    for arm in arms:
        require(all(isfinite(arm[k]) for k in ("effective_count", "mean_reward", "score"))
                and arm["effective_count"] >= 0 and 0 <= arm["mean_reward"] <= 1 and arm["score"] >= 0,
                "Invalid arm score components")
    cold = [arm for arm in arms if not arm["observed"]]
    observed = [arm for arm in arms if arm["observed"]]
    first_available = next((arm for arm in cold if pending[arm["backend_id"]] == 0), None)
    if first_available:
        require(first_available is arms[-1], "Cold decision failed to short-circuit at the selected arm")
        return first_available["backend_id"], "cold_no_pending"
    if observed:
        return max(observed, key=lambda arm: arm["score"])["backend_id"], "maximum_ucb"
    return min(cold, key=lambda arm: pending[arm["backend_id"]])["backend_id"], "cold_least_pending"


def check_bandit(selections, policy, scale_ms):
    transitions = []
    for _, selected, trace in selections:
        decision = selected["bandit"]
        applied = trace["bandit_feedback"][1]
        feedback = applied["feedback"]
        completed = trace["request_completed"][1]
        require(applied["backend_id"] == selected["backend_id"] and decision["order"] < feedback["order"]
                and feedback["age_ns"] >= completed["duration_ns"], "Invalid selection-feedback correlation")
        expected = feedback_reward(completed, scale_ms)
        require(feedback["applied"] == (expected is not None) and (
            not feedback["applied"] or isclose(feedback["reward"], expected, rel_tol=1e-12)
        ), "Feedback eligibility or reward differs from source rule")
        transitions.extend([(decision["order"], selected, True), (feedback["order"], applied, False)])
    pending = Counter()
    counts = Counter()
    rewards = Counter()
    window = deque()
    total = 0
    sequence = 0
    for order, event, is_selection in sorted(transitions, key=lambda row: row[0]):
        sequence += 1
        require(order == sequence, "Missing or duplicated selection-feedback order")
        backend = event["backend_id"]
        if is_selection:
            loads = {load["BackendID"]: load["Count"] for load in event["in_flight"]}
            require(all(loads[b] == pending[b] for b in BACKENDS), "Pending reservations disagree with trace order")
            decision = event["bandit"]
            for arm in decision["arms"]:
                candidate = arm["backend_id"]
                count = counts[candidate]
                observed = count > (1e-12 if policy == "discounted-ucb" else 0)
                estimate = rewards[candidate] / count if observed else 0
                score = estimate + sqrt(2 * log(max(1, total)) / count) if observed else 0
                require(arm["observed"] == observed and isclose(arm["effective_count"], count, rel_tol=1e-9)
                        and isclose(arm["mean_reward"], estimate, abs_tol=1e-11)
                        and isclose(arm["score"], score, abs_tol=1e-11, rel_tol=1e-9),
                        "Recorded score components differ from feedback replay")
            require(selection_winner(decision, loads) == (backend, decision["reason"]),
                    "Scores or pending state disagree with the selected backend")
            pending[backend] += 1
        else:
            pending[backend] -= 1
            require(pending[backend] >= 0, "Feedback preceded its selection")
            feedback = event["feedback"]
            if not feedback["applied"]:
                continue
            if policy == "discounted-ucb":
                total *= .99
                for arm in BACKENDS:
                    counts[arm] *= .99
                    rewards[arm] *= .99
            elif policy == "sliding-window-ucb":
                if len(window) == 1000:
                    expired_arm, expired_reward = window.popleft()
                    counts[expired_arm] -= 1
                    rewards[expired_arm] = rewards[expired_arm] - expired_reward if counts[expired_arm] else 0
                    total -= 1
                window.append((backend, feedback["reward"]))
            counts[backend] += 1
            rewards[backend] += feedback["reward"]
            total += 1
    require(not any(pending.values()), "Pending requests remain after trace end")


def audit(path):
    meta = read_json(path / "metadata.json")
    scenario = read_json(path / "scenario.json")
    stats = read_json(path / "metrics.json")
    summary = read_json(path / "k6-summary.json")
    events = read_json(path / "events.json")
    start = workload_start(path / "k6-output.log")
    obs = meta["observability"]
    require(meta["valid"] and obs["complete"] and obs["sample_every"] == 1 and obs["dropped_events"] == 0
            and len(obs["snapshots"]) == 12, "Invalid trial or incomplete every-request diagnostics")
    raw, vus = raw_counts(path / "raw-metrics.json.gz")
    with (path / "requests.csv").open(encoding="utf-8", newline="") as stream:
        counts = Counter(row["status_code"] for row in csv.DictReader(stream))
    nominal = sum(stage["duration_seconds"] * stage["target_rps"] for stage in scenario["workload"]["stages"])
    require(raw == counts and counts == Counter(stats["outcomes"]["status_distribution"])
            and abs(counts.total() - nominal) <= 3
            and summary["workload_integrity"]["total_requests"] == counts.total()
            and summary["workload_integrity"]["dropped_iterations"] == 0 and vus is not None and vus < 360,
            "k6, CSV or summary counts disagree, or offered load failed validation")
    config = meta["configuration"]
    settings = config["workload"]
    policy_settings = dict(config["policy_configuration"])
    require(policy_settings.pop("SBR_PROXY_POLICY") == meta["routing_policy"] and
            (POLICY_DEFAULTS | policy_settings) == POLICY_DEFAULTS and config["http_timeout"] == 5,
            "Unsupported policy parameters or client timeout")
    stage, = scenario["workload"]["stages"]
    require(stage["duration_seconds"] == 160 and stage["target_rps"] in (40, 80),
            "Unsupported arrival schedule")
    require(scenario == meta["scenario_definition"] and meta["timing_summary"]["sync_method"] == "stdout"
            and settings["k6_compose_network"] and settings["vus_preallocated"] == settings["vus_max"] == 400
            and len(events) == len(scenario["events"]) == 2, "Unexpected workload or start synchronization settings")
    changes = []
    for event, planned, delay in zip(events, scenario["events"], ("300ms", "20ms")):
        trigger = event["triggered_at_epoch_seconds"] - start
        ack = event["acknowledged_at_epoch_seconds"] - start
        require(event["success"] and event["verified"] and event["target"] == planned["target"] == "backend-a"
                and event["action"] == planned["action"] and abs(trigger - planned["timestamp_seconds"]) <= .1
                and 0 <= ack - trigger <= .1 and event["observed_workload"] == {"processing_delay": delay, "processing_jitter": "5ms"},
                "Unexpected administrative change or timing")
        changes.append(start + (trigger + ack) / 2)
    traces = event_traces(path / "diagnostic-events.jsonl")
    proxy = read_metrics(path / "observability-after-proxy.prom")
    backend_metrics = {b: read_metrics(path / f"observability-after-{b}.prom") for b in BACKENDS}
    errors, clocks = verify_traces(traces, proxy, backend_metrics)
    require(not errors, "; ".join(errors))
    require(all(proxy.get(f'sbr_proxy_requests_total{{status="{code}"}}', 0) == n for code, n in counts.items())
            and len(traces) == counts.total(), "Proxy terminal counts and k6 disagree")
    for b in BACKENDS:
        before = read_metrics(path / f"observability-before-{b}.prom")
        require(before["sbr_backend_workers_capacity"] == 4 and before["sbr_backend_queue_capacity"] == 32
                and proxy[f'sbr_proxy_backend_health{{backend="{b}",state="healthy"}}'] == 1,
                "Backend health or capacity differs from protocol")
    selections = [(at, event, trace) for trace in traces.values() for at, event in [trace["backend_selected"]]]
    selections.sort(key=lambda x: int(x[1]["request_id"].rsplit("-", 1)[1]))
    traced = obs.get("bandit_trace", False) and meta["routing_policy"] in MAB
    if traced:
        check_bandit(selections, meta["routing_policy"], 100)
    else:
        require(all("bandit" not in s and "bandit_feedback" not in t for _, s, t in selections), "Unexpected bandit trace")
    return meta, stats, counts, changes, start, selections, clocks, vus


def percentile(values, key="p95"):
    return compute_latency_stats(values)[key] if values else None


def phase(at, changes):
    return ("normal", "degraded", "recovery")[bisect_right(changes, at)]


def queue_by_burst(selections):
    result = []
    for backend, group in groupby(selections, key=lambda row: row[1]["backend_id"]):
        burst = list(group)
        if len(burst) < BURST_LENGTH:
            continue
        admissions = [trace["queue_admitted"] for _, _, trace in burst if "queue_admitted" in trace]
        admissions.sort(key=lambda row: row[0])
        depths = [entry["queued"] for _, entry in admissions]
        result.append({"backend": backend, "start": burst[0][0], "length": len(burst), "queue_first": depths[0] if depths else None,
                       "queue_max": max(depths, default=None)})
    return result


def prior_feedback_mass(policy, feedback, order, boundary, backend="backend-a"):
    history = [(selection, arm) for applied, selection, arm in feedback if applied < order]
    if policy == "sliding-window-ucb":
        history = history[-1000:]
    weights = [(selection, .99 ** (len(history) - i - 1) if policy == "discounted-ucb" else 1)
               for i, (selection, arm) in enumerate(history) if arm == backend]
    total = sum(weight for _, weight in weights)
    older = sum(weight for selection, weight in weights if selection < boundary)
    return total, older / total if total else None


def summarize(path):
    meta, metrics, counts, changes, start, selections, clocks, vus = audit(path)
    policy = meta["routing_policy"]
    duration = meta["scenario_definition"]["duration_seconds"]
    detailed = policy in MAB and meta["observability"].get("bandit_trace", False)
    feedback = []
    phases = defaultdict(list)
    reward_groups = defaultdict(list)
    processing = defaultdict(list)
    for at, selected, trace in selections:
        name = phase(at, changes)
        phases[name].append((at, selected, trace))
        if "work_completed" in trace:
            ended, work = trace["work_completed"]
            elapsed = work["duration_ns"] / 1e9
            if elapsed > 0:
                processing[work["backend_id"]].append((ended - elapsed, ended))
        if detailed:
            applied = trace["bandit_feedback"][1]["feedback"]
            if applied["applied"]:
                feedback.append((applied["order"], at, selected["backend_id"]))
                reward_groups[(name, selected["backend_id"])].append(applied["reward"])
    feedback.sort()
    per_phase = {}
    bounds = dict(zip(("normal", "degraded", "recovery"), zip((start, *changes), (*changes, start + duration))))
    for name, rows in phases.items():
        left, right = bounds[name]
        loads = [{item["BackendID"]: item["Count"] for item in s["in_flight"]} for _, s, _ in rows]
        pending = [load[s["backend_id"]] for load, (_, s, _) in zip(loads, rows)]
        bursts = queue_by_burst(rows)
        measured = [b for b in bursts if b["queue_first"] is not None]
        done = [(t["request_completed"][1]["duration_ns"], t) for _, _, t in rows
                if t["request_completed"][1]["status"] == 200]
        tail = percentile([ns for ns, _ in done], "p99")
        tail_requests = [(ns, t) for ns, t in done if ns and ns >= tail]
        tail_wait = [t["work_completed"][1]["queue_wait_ns"] / ns for ns, t in tail_requests if "work_completed" in t]
        queue = [t[kind][1] for _, _, t in rows for kind in ("queue_admitted", "queue_rejected", "work_completed") if kind in t]
        result = {
            "assignments": len(rows),
            "a_share": shares(Counter(s["backend_id"] for _, s, _ in rows))["backend-a"],
            "pending_pct": 100 * sum(p > 0 for p in pending) / len(rows),
            "selected_pending_p95": percentile(pending),
            "burst_count": len(bursts), "burst_selection_pct": 100 * sum(b["length"] for b in bursts) / len(rows),
            "longest_burst": max((b["length"] for b in bursts), default=None),
            "bursts_with_queue_growth": sum(b["queue_max"] > b["queue_first"] for b in measured),
            "bursts_with_measured_queue": len(measured),
            "queue_peaks": {b: max((q["queued"] for q in queue if q["backend_id"] == b), default=None) for b in BACKENDS},
            "worker_capacity_sample_pct": 100 * sum(q["active_workers"] == 4 for q in queue) / len(queue) if queue else None,
            "processing_capacity_pct": {
                b: 100 * sum(max(0, min(end, right) - max(begin, left)) for begin, end in processing[b]) / (4 * (right - left))
                for b in BACKENDS
            },
            "queue_full": sum(t.get("queue_rejected", (None, {}))[1].get("outcome") == "queue_full" for _, _, t in rows),
            "proxy_http200_p99_ms": tail / 1e6 if tail is not None else None,
            "tail_backend_counts": dict(Counter(t["backend_selected"][1]["backend_id"] for _, t in tail_requests)),
            "tail_majority_queue_pct": 100 * sum(fraction > .5 for fraction in tail_wait) / len(tail_wait) if tail_wait else None,
        }
        if detailed:
            scores = [s["bandit"] for _, s, _ in rows]
            rewards = [t["bandit_feedback"][1]["feedback"] for _, _, t in rows]
            eligible = [r for r in rewards if r["applied"]]
            lower_mean = 0
            for _, selected, _ in rows:
                decision = selected["bandit"]
                if decision["reason"] == "maximum_ucb":
                    by_id = {arm["backend_id"]: arm for arm in decision["arms"] if arm["observed"]}
                    winner = by_id[selected["backend_id"]]["mean_reward"]
                    lower_mean += winner + 1e-12 < max(arm["mean_reward"] for arm in by_id.values())
            result.update({"decision_reasons": dict(Counter(s["reason"] for s in scores)),
                           "bonus_overrides_mean_pct": 100 * lower_mean / len(rows),
                           "feedback_age_p95_ms": percentile([r["age_ns"] / 1e6 for r in eligible]),
                           "feedback_after_forward_p95_ms": percentile([
                               (t["bandit_feedback"][1]["feedback"]["age_ns"] - t["request_completed"][1]["duration_ns"]) / 1e6
                               for _, _, t in rows if t["bandit_feedback"][1]["feedback"]["applied"]
                           ]),
                           "excluded_feedback": len(rewards) - len(eligible),
                           "zero_rewards": sum(r["reward"] == 0 for r in eligible),
                           "reward_by_backend": {b: compute_latency_stats(reward_groups[name, b]) for b in BACKENDS
                                                 if reward_groups[name, b]}})
        per_phase[name] = result
    transitions = {}
    baseline = shares(Counter(s["backend_id"] for at, s, _ in selections if changes[0] - 20 <= at < changes[0]))["backend-a"]
    last_degraded = shares(Counter(s["backend_id"] for at, s, _ in selections if changes[1] - 20 <= at < changes[1]))["backend-a"]
    for index, name in enumerate(("degradation", "recovery")):
        boundary, end = changes[index], (*changes[1:], start + duration)[index]
        bins = []
        for offset in range(-10, int(end - boundary - 5) + 1, 5):
            group = [(s, t) for at, s, t in selections if boundary + offset <= at < boundary + offset + 5]
            bins.append({"from_change_s": offset, "starts": len(group),
                         "a_share": shares(Counter(s["backend_id"] for s, _ in group))["backend-a"] if group else None,
                         "a_pending_p95": percentile([next(l["Count"] for l in s["in_flight"] if l["BackendID"] == "backend-a")
                                                      for s, _ in group]),
                         "proxy_success_p95_ms": percentile([t["request_completed"][1]["duration_ns"] / 1e6 for _, t in group
                                                             if t["request_completed"][1]["status"] == 200])})
        equiv = (lambda share: share <= .5 * baseline) if index == 0 else (lambda share: share >= .8 * baseline)
        row = {"bins": bins, "sustained_share_crossing_s": sustained_crossing([b for b in bins if b["from_change_s"] >= 0], equiv)
               if baseline and (index == 0 or baseline - last_degraded >= .05) else None}
        if detailed:
            post_a = [(at, s, t["bandit_feedback"]) for at, s, t in selections if boundary <= at < end
                      and s["backend_id"] == "backend-a" and t["bandit_feedback"][1]["feedback"]["applied"]]
            row["late_a_feedback_from_prechange_work"] = sum(
                s["backend_id"] == "backend-a"
                and "work_completed" in t
                and t["work_completed"][1]["outcome"] == "processed"
                and t["work_completed"][0] - t["work_completed"][1]["duration_ns"] / 1e9 < boundary
                <= t["bandit_feedback"][0] < end
                and t["bandit_feedback"][1]["feedback"]["applied"]
                for _, s, t in selections)
            if post_a:
                _, _, (seen_at, seen) = min(post_a, key=lambda x: x[2][1]["feedback"]["order"])
                first_order = seen["feedback"]["order"]
                row["first_new_a_feedback_s"] = seen_at - boundary
                row["first_a_feedback_age_ms"] = seen["feedback"]["age_ns"] / 1e6
                row["first_a_feedback_reward"] = seen["feedback"]["reward"]
                waiting = [s for at, s, _ in selections if at >= boundary and s["backend_id"] == "backend-a"
                           and s["bandit"]["order"] < first_order]
                row["a_selected_before_new_feedback"] = len(waiting)
                row["a_pending_peak_before_new_feedback"] = max(
                    (next(load["Count"] for load in s["in_flight"] if load["BackendID"] == "backend-a") for s in waiting),
                    default=None)
            for offset in (5, 20):
                after = next((s for at, s, _ in selections if at >= boundary + offset), None)
                if after is not None:
                    decision = after["bandit"]
                    arm = next((a for a in decision["arms"] if a["backend_id"] == "backend-a"), None)
                    total, older = prior_feedback_mass(policy, feedback, decision["order"], boundary)
                    if arm is not None:
                        require(isclose(total, arm["effective_count"], rel_tol=1e-9, abs_tol=1e-10),
                                "Reconstructed Backend A feedback mass differs from direct effective count")
                    row[f"a_prechange_mass_fraction_at_{offset}s"] = older
        transitions[name] = row
    return {"path": path.as_posix(), "policy": policy, "seed": meta["random_seed"], "bandit_trace": detailed,
            "offered_rps": meta["scenario_definition"]["workload"]["stages"][0]["target_rps"], "client_statuses": dict(counts),
            "successful_rps": counts["200"] / duration, "client_latency_ms": metrics["latency_ms"]["successful_requests"],
            "peak_sampled_vus": vus, "clock_audit": clocks, "phases": per_phase, "transitions": transitions}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trials", type=Path, nargs="+")
    args = parser.parse_args()
    for path in args.trials:
        print(json.dumps(summarize(path), allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
