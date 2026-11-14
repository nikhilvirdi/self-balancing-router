"""
Structured experimental data collection, stream processing, and validation.
Produces requests.csv, metrics.json, and verifies trial data integrity.
"""

import array
import csv
import datetime
import json
import logging
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger("sbr.orchestrator.data")

REQUESTS_CSV_COLUMNS: List[str] = [
    "request_index",
    "timestamp_iso",
    "elapsed_seconds",
    "duration_ms",
    "status_code",
    "outcome",
    "is_success",
    "is_overload",
    "is_transport_error",
    "backend_id",
    "phase",
    "interval",
    "scenario_id",
    "routing_policy",
    "trial_index",
]

VALID_OUTCOMES: Set[str] = {
    "success",
    "overload_503",
    "http_error",
    "transport_error",
}

VALID_PHASES: Set[str] = {
    "warmup",
    "measurement",
    "cooldown",
}


def parse_iso_to_epoch(ts: str) -> float:
    """
    Parses an ISO 8601 UTC timestamp string with nanosecond or microsecond precision
    into floating-point seconds since the Unix epoch.
    """
    if not ts:
        return 0.0
    try:
        clean = ts.strip()
        dt = datetime.datetime.fromisoformat(clean.replace("Z", "+00:00") if clean.endswith("Z") else clean)
        # Extract extra sub-microsecond fractional digits if present
        if "." in clean:
            dot_part = clean.split(".")[1].rstrip("Z").split("+")[0].split("-")[0]
            if len(dot_part) > 6:
                nano_fraction = float("0." + dot_part)
                base_dt = dt.replace(microsecond=0)
                return base_dt.timestamp() + nano_fraction
        return dt.timestamp()
    except Exception:
        pass
    return 0.0


def compute_latency_stats(durations: Sequence[float]) -> Dict[str, float]:
    """
    Computes summary latency distribution metrics (count, mean, min, p50, p90, p95, p99, max)
    using standard linear percentile interpolation.
    """
    if not durations:
        return {
            "count": 0,
            "mean": 0.0,
            "min": 0.0,
            "p50": 0.0,
            "p90": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
        }

    sorted_vals = sorted(durations)
    n = len(sorted_vals)

    def percentile(p: float) -> float:
        if n == 1:
            return float(sorted_vals[0])
        k = (n - 1) * (p / 100.0)
        f = int(math.floor(k))
        c = int(math.ceil(k))
        if f == c:
            return float(sorted_vals[f])
        d0 = sorted_vals[f] * (c - k)
        d1 = sorted_vals[c] * (k - f)
        return float(d0 + d1)

    return {
        "count": n,
        "mean": float(sum(sorted_vals) / n),
        "min": float(sorted_vals[0]),
        "p50": percentile(50.0),
        "p90": percentile(90.0),
        "p95": percentile(95.0),
        "p99": percentile(99.0),
        "max": float(sorted_vals[-1]),
    }


class IntervalTimeline:
    """
    Partitions the experimental timeline into named intervals (e.g. baseline,
    degradation, recovery, steady_state) based on actual recorded event timestamps.
    """

    def __init__(self, events: List[Dict[str, Any]], scenario: Dict[str, Any]):
        self.scenario = scenario
        self.total_duration = float(scenario.get("duration_seconds", 60.0))
        self.intervals: List[Dict[str, Any]] = []
        self._build_intervals(events)

    def _build_intervals(self, events: List[Dict[str, Any]]) -> None:
        if not events:
            self.intervals.append({
                "name": "steady_state",
                "start_relative_seconds": 0.0,
                "end_relative_seconds": self.total_duration,
                "duration_seconds": self.total_duration,
            })
            return

        sorted_events = sorted(
            events,
            key=lambda e: float(e.get("actual_trigger_relative_seconds", e.get("planned_timestamp_seconds", 0.0))),
        )

        first_ts = float(sorted_events[0].get("actual_trigger_relative_seconds", sorted_events[0].get("planned_timestamp_seconds", 0.0)))
        if first_ts > 0.0:
            self.intervals.append({
                "name": "baseline",
                "start_relative_seconds": 0.0,
                "end_relative_seconds": first_ts,
                "duration_seconds": first_ts,
            })

        for i, ev in enumerate(sorted_events):
            start_ts = float(ev.get("actual_trigger_relative_seconds", ev.get("planned_timestamp_seconds", 0.0)))
            if i + 1 < len(sorted_events):
                next_ev = sorted_events[i + 1]
                end_ts = float(next_ev.get("actual_trigger_relative_seconds", next_ev.get("planned_timestamp_seconds", 0.0)))
            else:
                end_ts = self.total_duration

            dur = max(0.0, end_ts - start_ts)
            action = ev.get("action", f"event_{i}")
            target = ev.get("target", "")

            if action == "update":
                name = f"degradation:{target}" if target else "degradation"
            elif action == "reset":
                name = f"recovery:{target}" if target else "recovery"
            else:
                name = f"{action}:{target}" if target else action

            self.intervals.append({
                "name": name,
                "start_relative_seconds": start_ts,
                "end_relative_seconds": end_ts,
                "duration_seconds": dur,
            })

    def get_interval_name(self, elapsed_seconds: float) -> str:
        """Determines which experimental interval an elapsed timestamp falls into."""
        if not self.intervals:
            return "steady_state"
        for inter in self.intervals:
            if inter["start_relative_seconds"] <= elapsed_seconds < inter["end_relative_seconds"]:
                return inter["name"]
        return self.intervals[-1]["name"]

    def get_definitions(self) -> List[Dict[str, Any]]:
        """Returns the list of interval definitions."""
        return list(self.intervals)


class DataCollector:
    """
    Streams raw k6 metrics points, transforms them into structured request records in requests.csv,
    and computes derived summary metrics in metrics.json.
    """

    @staticmethod
    def collect_and_export(
        raw_metrics_path: Path,
        output_dir: Path,
        workload_start_wall: float,
        timeline: IntervalTimeline,
        scenario: Dict[str, Any],
        policy: str,
        trial_index: int,
        dropped_iterations: int = 0,
        keep_raw_metrics: bool = False,
        dry_run: bool = False,
    ) -> Tuple[Path, Path, Dict[str, Any]]:
        """
        Processes raw metrics into requests.csv and metrics.json using streaming execution.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        requests_csv_path = output_dir / "requests.csv"
        metrics_json_path = output_dir / "metrics.json"

        # Statistical accumulators
        all_durations = array.array("d")
        successful_durations = array.array("d")

        phase_durations_all: Dict[str, array.array] = {
            "warmup": array.array("d"),
            "measurement": array.array("d"),
            "cooldown": array.array("d"),
        }
        phase_durations_successful: Dict[str, array.array] = {
            "warmup": array.array("d"),
            "measurement": array.array("d"),
            "cooldown": array.array("d"),
        }

        interval_durations_all: Dict[str, array.array] = {}
        interval_durations_successful: Dict[str, array.array] = {}
        for inter in timeline.get_definitions():
            interval_durations_all[inter["name"]] = array.array("d")
            interval_durations_successful[inter["name"]] = array.array("d")

        status_distribution: Dict[str, int] = {}
        outcome_distribution: Dict[str, int] = {}
        backend_distribution: Dict[str, int] = {}

        phase_total_counts: Dict[str, int] = {"warmup": 0, "measurement": 0, "cooldown": 0}
        phase_success_counts: Dict[str, int] = {"warmup": 0, "measurement": 0, "cooldown": 0}

        interval_total_counts: Dict[str, int] = {inter["name"]: 0 for inter in timeline.get_definitions()}
        interval_success_counts: Dict[str, int] = {inter["name"]: 0 for inter in timeline.get_definitions()}

        total_requests = 0
        scenario_id = scenario.get("id", "unknown_scenario")
        first_req_wall: Optional[float] = None
        last_req_wall: Optional[float] = None

        if not raw_metrics_path.is_file():
            if not dry_run:
                logger.warning("Raw metrics file '%s' not found; generating empty datasets.", raw_metrics_path)
        else:
            # Check if custom observation metric is present
            target_metric = "http_req_duration"
            with open(raw_metrics_path, "r", encoding="utf-8", errors="replace") as check_f:
                for check_line in check_f:
                    if '"sbr_request_observation"' in check_line:
                        target_metric = "sbr_request_observation"
                        break

            with open(requests_csv_path, "w", newline="", encoding="utf-8") as csv_f:
                writer = csv.writer(csv_f)
                writer.writerow(REQUESTS_CSV_COLUMNS)

                with open(raw_metrics_path, "r", encoding="utf-8", errors="replace") as raw_f:
                    for line in raw_f:
                        line = line.strip()
                        if not line or not line.startswith("{"):
                            continue

                        try:
                            item = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        # Filter for target request observation metric
                        metric_name = item.get("metric", "")
                        item_type = item.get("type", "")
                        if item_type != "Point":
                            continue

                        if metric_name != target_metric:
                            continue

                        data = item.get("data", {})
                        tags = data.get("tags", {})

                        raw_status = tags.get("status", "0")
                        status_code = int(raw_status) if raw_status and raw_status.isdigit() else 0

                        duration_val = float(data.get("value", 0.0))
                        time_str = data.get("time", "")
                        req_wall = parse_iso_to_epoch(time_str)

                        if first_req_wall is None or req_wall < first_req_wall:
                            first_req_wall = req_wall
                        if last_req_wall is None or req_wall > last_req_wall:
                            last_req_wall = req_wall

                        elapsed = max(0.0, req_wall - workload_start_wall) if workload_start_wall > 0 else 0.0
                        elapsed_rounded = round(elapsed, 6)

                        # Outcome classification
                        outcome = tags.get("outcome")
                        if not outcome:
                            if status_code == 200:
                                outcome = "success"
                            elif status_code == 503:
                                outcome = "overload_503"
                            elif status_code > 0:
                                outcome = "http_error"
                            else:
                                outcome = "transport_error"

                        is_success = (status_code == 200 and outcome == "success")
                        is_overload = (status_code == 503 or outcome == "overload_503")
                        is_transport = (status_code == 0 or outcome == "transport_error")

                        backend_id = tags.get("backend_id", "")
                        phase = tags.get("phase", "measurement")
                        if phase not in VALID_PHASES:
                            phase = "measurement"

                        interval_name = timeline.get_interval_name(elapsed)

                        # Write row to requests.csv
                        writer.writerow([
                            total_requests,
                            time_str,
                            elapsed_rounded,
                            round(duration_val, 6),
                            status_code,
                            outcome,
                            is_success,
                            is_overload,
                            is_transport,
                            backend_id,
                            phase,
                            interval_name,
                            scenario_id,
                            policy,
                            trial_index,
                        ])

                        # Accumulate
                        total_requests += 1
                        all_durations.append(duration_val)
                        if is_success:
                            successful_durations.append(duration_val)

                        st_key = str(status_code)
                        status_distribution[st_key] = status_distribution.get(st_key, 0) + 1
                        outcome_distribution[outcome] = outcome_distribution.get(outcome, 0) + 1

                        if backend_id:
                            backend_distribution[backend_id] = backend_distribution.get(backend_id, 0) + 1

                        phase_total_counts[phase] = phase_total_counts.get(phase, 0) + 1
                        phase_durations_all[phase].append(duration_val)
                        if is_success:
                            phase_success_counts[phase] = phase_success_counts.get(phase, 0) + 1
                            phase_durations_successful[phase].append(duration_val)

                        if interval_name not in interval_durations_all:
                            interval_durations_all[interval_name] = array.array("d")
                            interval_durations_successful[interval_name] = array.array("d")
                            interval_total_counts[interval_name] = 0
                            interval_success_counts[interval_name] = 0

                        interval_total_counts[interval_name] += 1
                        interval_durations_all[interval_name].append(duration_val)
                        if is_success:
                            interval_success_counts[interval_name] += 1
                            interval_durations_successful[interval_name].append(duration_val)

        # Remove raw metrics file if not requested to keep
        if not keep_raw_metrics and raw_metrics_path.is_file():
            try:
                raw_metrics_path.unlink()
            except Exception as e:
                logger.debug("Failed to unlink raw metrics file: %s", e)

        # If requests.csv did not get written (empty raw metrics), ensure headers exist
        if not requests_csv_path.is_file():
            with open(requests_csv_path, "w", newline="", encoding="utf-8") as csv_f:
                csv.writer(csv_f).writerow(REQUESTS_CSV_COLUMNS)

        # Duration and throughput calculations
        scenario_duration = float(scenario.get("duration_seconds", 60.0))
        if first_req_wall is not None and last_req_wall is not None and last_req_wall >= first_req_wall:
            actual_duration = max(0.001, last_req_wall - first_req_wall)
        else:
            actual_duration = scenario_duration

        successful_count = len(successful_durations)
        total_throughput = float(total_requests / actual_duration) if actual_duration > 0 else 0.0
        successful_throughput = float(successful_count / actual_duration) if actual_duration > 0 else 0.0
        success_rate = float(successful_count / total_requests) if total_requests > 0 else 0.0

        overload_count = outcome_distribution.get("overload_503", 0)
        http_err_count = outcome_distribution.get("http_error", 0)
        transport_err_count = outcome_distribution.get("transport_error", 0)

        # Backend traffic distribution
        backend_traffic: Dict[str, Dict[str, Any]] = {}
        for b_name in ["backend-a", "backend-b", "backend-c", "backend-d", "backend-e"]:
            cnt = backend_distribution.get(b_name, 0)
            frac = float(cnt / successful_count) if successful_count > 0 else 0.0
            backend_traffic[b_name] = {
                "requests": cnt,
                "fraction": round(frac, 4),
            }

        # Timeline phase statistics
        timeline_cfg = scenario.get("timeline", {})
        phases_stats: Dict[str, Dict[str, Any]] = {}
        for p_name in ["warmup", "measurement", "cooldown"]:
            p_dur = float(timeline_cfg.get(f"{p_name}_seconds", 0.0))
            p_tot = phase_total_counts.get(p_name, 0)
            p_succ = phase_success_counts.get(p_name, 0)
            p_rate = float(p_succ / p_tot) if p_tot > 0 else 0.0
            p_thru = float(p_tot / p_dur) if p_dur > 0 else 0.0
            phases_stats[p_name] = {
                "duration_seconds": p_dur,
                "total_requests": p_tot,
                "successful_requests": p_succ,
                "success_rate": round(p_rate, 4),
                "throughput_rps": round(p_thru, 2),
                "successful_latency_ms": compute_latency_stats(phase_durations_successful[p_name]),
                "all_latency_ms": compute_latency_stats(phase_durations_all[p_name]),
            }

        # Experimental intervals statistics
        intervals_stats: Dict[str, Dict[str, Any]] = {}
        for inter in timeline.get_definitions():
            i_name = inter["name"]
            i_dur = inter["duration_seconds"]
            i_tot = interval_total_counts.get(i_name, 0)
            i_succ = interval_success_counts.get(i_name, 0)
            i_rate = float(i_succ / i_tot) if i_tot > 0 else 0.0
            i_thru = float(i_tot / i_dur) if i_dur > 0 else 0.0
            intervals_stats[i_name] = {
                "start_relative_seconds": inter["start_relative_seconds"],
                "end_relative_seconds": inter["end_relative_seconds"],
                "duration_seconds": i_dur,
                "total_requests": i_tot,
                "successful_requests": i_succ,
                "success_rate": round(i_rate, 4),
                "throughput_rps": round(i_thru, 2),
                "successful_latency_ms": compute_latency_stats(interval_durations_successful.get(i_name, [])),
                "all_latency_ms": compute_latency_stats(interval_durations_all.get(i_name, [])),
            }

        metrics_data: Dict[str, Any] = {
            "trial_metadata": {
                "scenario_id": scenario_id,
                "scenario_name": scenario.get("name", ""),
                "routing_policy": policy,
                "trial_index": trial_index,
                "valid": (dropped_iterations == 0 and total_requests > 0),
            },
            "workload_integrity": {
                "planned_duration_seconds": scenario_duration,
                "actual_duration_seconds": round(actual_duration, 4),
                "total_requests": total_requests,
                "dropped_iterations": dropped_iterations,
                "valid_offered_load": (dropped_iterations == 0),
                "total_throughput_rps": round(total_throughput, 2),
                "successful_throughput_rps": round(successful_throughput, 2),
            },
            "outcomes": {
                "total_requests": total_requests,
                "successful_requests": successful_count,
                "success_rate": round(success_rate, 4),
                "overload_503_count": overload_count,
                "overload_503_rate": round(float(overload_count / total_requests) if total_requests > 0 else 0.0, 4),
                "http_error_count": http_err_count,
                "http_error_rate": round(float(http_err_count / total_requests) if total_requests > 0 else 0.0, 4),
                "transport_error_count": transport_err_count,
                "transport_error_rate": round(float(transport_err_count / total_requests) if total_requests > 0 else 0.0, 4),
                "error_rate": round(float((total_requests - successful_count) / total_requests) if total_requests > 0 else 0.0, 4),
                "status_distribution": status_distribution,
            },
            "latency_ms": {
                "successful_requests": compute_latency_stats(successful_durations),
                "all_requests": compute_latency_stats(all_durations),
            },
            "backend_traffic_distribution": backend_traffic,
            "phases": phases_stats,
            "intervals": intervals_stats,
        }

        with open(metrics_json_path, "w", encoding="utf-8") as f:
            json.dump(metrics_data, f, indent=2)

        return requests_csv_path, metrics_json_path, metrics_data


class DataValidator:
    """
    Validates experimental artifacts for consistency, non-emptiness, valid timestamps,
    non-negative durations, workload integrity, and identifier alignment.
    """

    REQUIRED_ARTIFACTS: List[str] = [
        "metadata.json",
        "scenario.json",
        "events.json",
        "k6-summary.json",
        "k6-output.log",
        "requests.csv",
        "metrics.json",
    ]

    @classmethod
    def validate_trial(
        cls,
        run_dir: Path,
        scenario: Dict[str, Any],
        policy: str,
        trial_index: int,
    ) -> Tuple[bool, List[str]]:
        """
        Exhaustively validates trial directory artifacts. Returns (is_valid, error_list).
        """
        errors: List[str] = []
        scenario_id = scenario.get("id", "")

        # 1. Verify existence of all required artifact files
        for fname in cls.REQUIRED_ARTIFACTS:
            fpath = run_dir / fname
            if not fpath.is_file():
                errors.append(f"Missing required artifact: {fname}")

        if errors:
            return False, errors

        # 2. Validate metadata.json
        meta_path = run_dir / "metadata.json"
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            if meta.get("scenario_id") != scenario_id:
                errors.append(f"Mismatched metadata scenario_id: expected '{scenario_id}', got '{meta.get('scenario_id')}'")
            if meta.get("routing_policy") != policy:
                errors.append(f"Mismatched metadata routing_policy: expected '{policy}', got '{meta.get('routing_policy')}'")
            if meta.get("trial_index") != trial_index:
                errors.append(f"Mismatched metadata trial_index: expected {trial_index}, got {meta.get('trial_index')}")
        except Exception as e:
            errors.append(f"Malformed metadata.json: {e}")

        # 3. Validate events.json
        events_path = run_dir / "events.json"
        try:
            with open(events_path, "r", encoding="utf-8") as f:
                events_data = json.load(f)
            if not isinstance(events_data, list):
                errors.append("events.json must be a JSON array")
            else:
                planned_events = scenario.get("events", [])
                if len(events_data) < len(planned_events):
                    errors.append(f"Incomplete events.json: expected {len(planned_events)} records, got {len(events_data)}")
                for ev in events_data:
                    if not ev.get("success", False) or not ev.get("verified", False):
                        errors.append(f"Failed administrative event in events.json: index {ev.get('event_index')}")
        except Exception as e:
            errors.append(f"Malformed events.json: {e}")

        # 4. Validate k6-summary.json
        summary_path = run_dir / "k6-summary.json"
        summary_total = None
        try:
            with open(summary_path, "r", encoding="utf-8") as f:
                summary = json.load(f)
            integrity = summary.get("workload_integrity", {})
            summary_total = integrity.get("total_requests", 0)
            dropped = integrity.get("dropped_iterations", 0)
            if dropped > 0:
                errors.append(f"Workload integrity violated: {dropped} dropped iterations in k6-summary.json")
        except Exception as e:
            errors.append(f"Malformed k6-summary.json: {e}")

        # 5. Validate metrics.json
        metrics_path = run_dir / "metrics.json"
        metrics_total = None
        try:
            with open(metrics_path, "r", encoding="utf-8") as f:
                metrics = json.load(f)
            meta_sec = metrics.get("trial_metadata", {})
            if meta_sec.get("scenario_id") != scenario_id:
                errors.append(f"Mismatched metrics.json scenario_id: expected '{scenario_id}', got '{meta_sec.get('scenario_id')}'")
            if meta_sec.get("routing_policy") != policy:
                errors.append(f"Mismatched metrics.json routing_policy: expected '{policy}', got '{meta_sec.get('routing_policy')}'")

            wi = metrics.get("workload_integrity", {})
            metrics_total = wi.get("total_requests", 0)
            m_dropped = wi.get("dropped_iterations", 0)
            if m_dropped > 0:
                errors.append(f"Workload integrity violated: {m_dropped} dropped iterations in metrics.json")

            # Check latency non-negativity and finiteness
            for lat_cat in ["successful_requests", "all_requests"]:
                lat_obj = metrics.get("latency_ms", {}).get(lat_cat, {})
                for k in ["mean", "min", "p50", "p90", "p95", "p99", "max"]:
                    v = lat_obj.get(k, 0.0)
                    if not math.isfinite(v) or v < 0.0:
                        errors.append(f"Invalid {lat_cat} latency '{k}': {v}")
        except Exception as e:
            errors.append(f"Malformed metrics.json: {e}")

        # 6. Validate requests.csv
        csv_path = run_dir / "requests.csv"
        csv_row_count = 0
        try:
            with open(csv_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header != REQUESTS_CSV_COLUMNS:
                    errors.append(f"Invalid requests.csv header: expected {REQUESTS_CSV_COLUMNS}, got {header}")

                for row_idx, row in enumerate(reader):
                    csv_row_count += 1
                    if len(row) != len(REQUESTS_CSV_COLUMNS):
                        errors.append(f"Row {row_idx} in requests.csv has {len(row)} columns, expected {len(REQUESTS_CSV_COLUMNS)}")
                        break

                    # Duration check
                    try:
                        dur = float(row[3])
                        if not math.isfinite(dur) or dur < 0.0:
                            errors.append(f"Row {row_idx} in requests.csv has invalid duration: {dur}")
                            break
                    except ValueError:
                        errors.append(f"Row {row_idx} in requests.csv has unparseable duration: {row[3]}")
                        break

                    # Outcome check
                    outcome = row[5]
                    if outcome not in VALID_OUTCOMES:
                        errors.append(f"Row {row_idx} in requests.csv has unrecognized outcome: {outcome}")
                        break

                    # Identifier check
                    if row[12] != scenario_id:
                        errors.append(f"Row {row_idx} has mismatched scenario_id: expected {scenario_id}, got {row[12]}")
                        break
                    if row[13] != policy:
                        errors.append(f"Row {row_idx} has mismatched policy: expected {policy}, got {row[13]}")
                        break
        except Exception as e:
            errors.append(f"Failed to read requests.csv: {e}")

        # 7. Cross-artifact consistency checks
        if summary_total is not None and metrics_total is not None and summary_total != metrics_total:
            errors.append(f"Request count mismatch: k6-summary.json ({summary_total}) != metrics.json ({metrics_total})")

        if metrics_total is not None and csv_row_count != metrics_total:
            errors.append(f"Request count mismatch: requests.csv ({csv_row_count}) != metrics.json ({metrics_total})")

        is_valid = (len(errors) == 0)
        return is_valid, errors
