"""
Unit test suite for experimental data collection, stream processing, and validation.
Verifies request parsing, CSV/JSON export, latency percentiles, throughput,
timeline alignment, failure classification, and integrity validation.
"""

import csv
import json
import math
import os
from pathlib import Path
import shutil
import tempfile
import unittest

from experiments.orchestrator.data import (
    REQUESTS_CSV_COLUMNS,
    DataCollector,
    DataValidator,
    IntervalTimeline,
    compute_latency_stats,
    parse_iso_to_epoch,
)


class TestDataCollectionAndExport(unittest.TestCase):
    """Test suite for data collection, interval alignment, and validation."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="sbr_data_test_")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    # 1. ISO 8601 timestamp parsing to epoch seconds
    def test_parse_iso_to_epoch(self):
        # Nanosecond precision
        ts_nano = "2026-11-13T16:17:32.367387427Z"
        epoch_nano = parse_iso_to_epoch(ts_nano)
        self.assertGreater(epoch_nano, 1700000000.0)

        # Microsecond precision
        ts_micro = "2026-11-13T16:17:32.367387Z"
        epoch_micro = parse_iso_to_epoch(ts_micro)
        self.assertAlmostEqual(epoch_nano, epoch_micro, delta=0.001)

        # Standard seconds precision
        ts_sec = "2026-11-13T16:17:32Z"
        epoch_sec = parse_iso_to_epoch(ts_sec)
        self.assertEqual(int(epoch_sec), int(epoch_nano))

        # Invalid or empty string
        self.assertEqual(parse_iso_to_epoch(""), 0.0)
        self.assertEqual(parse_iso_to_epoch("not-a-timestamp"), 0.0)

    # 2. Latency percentile calculations
    def test_compute_latency_stats(self):
        # Empty array
        empty_stats = compute_latency_stats([])
        self.assertEqual(empty_stats["count"], 0)
        self.assertEqual(empty_stats["mean"], 0.0)

        # Single value
        single_stats = compute_latency_stats([50.0])
        self.assertEqual(single_stats["count"], 1)
        self.assertEqual(single_stats["p50"], 50.0)
        self.assertEqual(single_stats["min"], 50.0)
        self.assertEqual(single_stats["max"], 50.0)

        # Known sequence: 100 values from 1.0 to 100.0
        seq = [float(i) for i in range(1, 101)]
        stats = compute_latency_stats(seq)
        self.assertEqual(stats["count"], 100)
        self.assertEqual(stats["min"], 1.0)
        self.assertEqual(stats["max"], 100.0)
        self.assertEqual(stats["mean"], 50.5)
        self.assertAlmostEqual(stats["p50"], 50.5, delta=0.5)
        self.assertAlmostEqual(stats["p90"], 90.1, delta=1.0)
        self.assertAlmostEqual(stats["p95"], 95.05, delta=1.0)
        self.assertAlmostEqual(stats["p99"], 99.01, delta=1.0)

    # 3. Interval timeline construction and alignment
    def test_interval_timeline_resolution(self):
        scenario = {
            "id": "scenario_3_sudden_degradation",
            "duration_seconds": 90.0,
            "timeline": {"warmup_seconds": 10.0, "measurement_seconds": 75.0, "cooldown_seconds": 5.0},
        }

        # Scenario with 1 degradation event at t=30.0s
        events = [
            {
                "event_index": 0,
                "target": "backend-a",
                "action": "update",
                "actual_trigger_relative_seconds": 30.005,
            }
        ]

        timeline = IntervalTimeline(events=events, scenario=scenario)
        defs = timeline.get_definitions()
        self.assertEqual(len(defs), 2)
        self.assertEqual(defs[0]["name"], "baseline")
        self.assertEqual(defs[0]["start_relative_seconds"], 0.0)
        self.assertEqual(defs[0]["end_relative_seconds"], 30.005)

        self.assertEqual(defs[1]["name"], "degradation:backend-a")
        self.assertEqual(defs[1]["start_relative_seconds"], 30.005)
        self.assertEqual(defs[1]["end_relative_seconds"], 90.0)

        # Query intervals by elapsed timestamp
        self.assertEqual(timeline.get_interval_name(0.0), "baseline")
        self.assertEqual(timeline.get_interval_name(15.0), "baseline")
        self.assertEqual(timeline.get_interval_name(30.004), "baseline")
        self.assertEqual(timeline.get_interval_name(30.005), "degradation:backend-a")
        self.assertEqual(timeline.get_interval_name(60.0), "degradation:backend-a")
        self.assertEqual(timeline.get_interval_name(95.0), "degradation:backend-a")

        # Scenario without events
        no_event_timeline = IntervalTimeline(events=[], scenario=scenario)
        self.assertEqual(len(no_event_timeline.get_definitions()), 1)
        self.assertEqual(no_event_timeline.get_interval_name(10.0), "steady_state")

    # 4. Stream data collection and CSV export
    def test_stream_data_collection_and_csv_export(self):
        scenario = {
            "id": "scenario_test",
            "duration_seconds": 10.0,
            "timeline": {"warmup_seconds": 2.0, "measurement_seconds": 6.0, "cooldown_seconds": 2.0},
        }
        events = [
            {"event_index": 0, "target": "backend-b", "action": "update", "actual_trigger_relative_seconds": 4.0}
        ]
        timeline = IntervalTimeline(events=events, scenario=scenario)

        # Create mock raw k6 metrics JSON lines file
        raw_file = Path(self.test_dir) / "mock-raw-metrics.json"
        start_wall = 1700000000.0

        records = [
            # 1. Successful 200 on backend-a during warmup (baseline)
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2023-11-14T22:13:21.000000000Z",  # +1.0s elapsed
                    "value": 25.5,
                    "tags": {
                        "status": "200",
                        "outcome": "success",
                        "backend_id": "backend-a",
                        "phase": "warmup",
                    },
                },
            },
            # 2. Successful 200 on backend-b during measurement before degradation (baseline)
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2023-11-14T22:13:23.000000000Z",  # +3.0s elapsed
                    "value": 30.0,
                    "tags": {
                        "status": "200",
                        "outcome": "success",
                        "backend_id": "backend-b",
                        "phase": "measurement",
                    },
                },
            },
            # 3. Overload 503 during measurement after degradation
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2023-11-14T22:13:25.000000000Z",  # +5.0s elapsed
                    "value": 2.0,
                    "tags": {
                        "status": "503",
                        "outcome": "overload_503",
                        "backend_id": "",
                        "phase": "measurement",
                    },
                },
            },
            # 4. Transport error during cooldown
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2023-11-14T22:13:29.000000000Z",  # +9.0s elapsed
                    "value": 0.5,
                    "tags": {
                        "status": "0",
                        "outcome": "transport_error",
                        "backend_id": "",
                        "phase": "cooldown",
                    },
                },
            },
        ]

        with open(raw_file, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")

        run_dir = Path(self.test_dir) / "run_output"
        csv_path, metrics_path, metrics_data = DataCollector.collect_and_export(
            raw_metrics_path=raw_file,
            output_dir=run_dir,
            workload_start_wall=start_wall,
            timeline=timeline,
            scenario=scenario,
            policy="round-robin",
            trial_index=1,
            dropped_iterations=0,
            keep_raw_metrics=True,
        )

        self.assertTrue(csv_path.is_file())
        self.assertTrue(metrics_path.is_file())

        # Read back requests.csv
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))

        self.assertEqual(rows[0], REQUESTS_CSV_COLUMNS)
        self.assertEqual(len(rows) - 1, 4)

        # Row 0: success backend-a
        self.assertEqual(rows[1][4], "200")
        self.assertEqual(rows[1][5], "success")
        self.assertEqual(rows[1][6], "True")
        self.assertEqual(rows[1][7], "False")
        self.assertEqual(rows[1][8], "False")
        self.assertEqual(rows[1][9], "backend-a")
        self.assertEqual(rows[1][10], "warmup")
        self.assertEqual(rows[1][11], "baseline")

        # Row 2: 503 overload
        self.assertEqual(rows[3][4], "503")
        self.assertEqual(rows[3][5], "overload_503")
        self.assertEqual(rows[3][6], "False")
        self.assertEqual(rows[3][7], "True")
        self.assertEqual(rows[3][8], "False")
        self.assertEqual(rows[3][9], "")
        self.assertEqual(rows[3][10], "measurement")
        self.assertEqual(rows[3][11], "degradation:backend-b")

        # Row 3: transport error
        self.assertEqual(rows[4][4], "0")
        self.assertEqual(rows[4][5], "transport_error")
        self.assertEqual(rows[4][6], "False")
        self.assertEqual(rows[4][7], "False")
        self.assertEqual(rows[4][8], "True")
        self.assertEqual(rows[4][9], "")
        self.assertEqual(rows[4][10], "cooldown")

        # Verify metrics.json
        self.assertEqual(metrics_data["outcomes"]["total_requests"], 4)
        self.assertEqual(metrics_data["outcomes"]["successful_requests"], 2)
        self.assertEqual(metrics_data["outcomes"]["overload_503_count"], 1)
        self.assertEqual(metrics_data["outcomes"]["transport_error_count"], 1)
        self.assertEqual(metrics_data["outcomes"]["success_rate"], 0.5)

        # Check backend traffic distribution
        self.assertEqual(metrics_data["backend_traffic_distribution"]["backend-a"]["requests"], 1)
        self.assertEqual(metrics_data["backend_traffic_distribution"]["backend-b"]["requests"], 1)
        self.assertEqual(metrics_data["backend_traffic_distribution"]["backend-c"]["requests"], 0)

    # 5. Isolation of successful-request latency from all-request latency
    def test_separate_successful_vs_all_latency(self):
        scenario = {"id": "test", "duration_seconds": 10.0, "timeline": {}}
        timeline = IntervalTimeline(events=[], scenario=scenario)

        raw_file = Path(self.test_dir) / "fast_fail_metrics.json"
        # 10 successful requests at 100ms + 10 fast failures at 0.5ms
        points = []
        for i in range(10):
            points.append({
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": f"2023-11-14T22:13:2{i}.000000000Z",
                    "value": 100.0,
                    "tags": {"status": "200", "outcome": "success", "backend_id": "backend-a"},
                },
            })
            points.append({
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": f"2023-11-14T22:13:2{i}.500000000Z",
                    "value": 0.5,
                    "tags": {"status": "503", "outcome": "overload_503", "backend_id": ""},
                },
            })

        with open(raw_file, "w", encoding="utf-8") as f:
            for p in points:
                f.write(json.dumps(p) + "\n")

        run_dir = Path(self.test_dir) / "run_latency_test"
        _, _, metrics = DataCollector.collect_and_export(
            raw_metrics_path=raw_file,
            output_dir=run_dir,
            workload_start_wall=1700000000.0,
            timeline=timeline,
            scenario=scenario,
            policy="p2c",
            trial_index=1,
            dropped_iterations=0,
        )

        succ_lat = metrics["latency_ms"]["successful_requests"]
        all_lat = metrics["latency_ms"]["all_requests"]

        self.assertEqual(succ_lat["count"], 10)
        self.assertEqual(succ_lat["mean"], 100.0)
        self.assertEqual(succ_lat["min"], 100.0)
        self.assertEqual(succ_lat["p50"], 100.0)

        self.assertEqual(all_lat["count"], 20)
        self.assertEqual(all_lat["mean"], 50.25)
        self.assertEqual(all_lat["min"], 0.5)

    # 6. Data validator verifies valid trial
    def test_data_validator_valid_trial(self):
        run_dir = Path(self.test_dir) / "valid_run"
        run_dir.mkdir(parents=True, exist_ok=True)

        scenario = {
            "id": "scenario_1_homogeneous",
            "name": "Stable Homogeneous",
            "duration_seconds": 60.0,
            "events": [],
        }

        # Create all 7 valid artifacts
        with open(run_dir / "metadata.json", "w", encoding="utf-8") as f:
            json.dump({
                "scenario_id": "scenario_1_homogeneous",
                "routing_policy": "round-robin",
                "trial_index": 1,
            }, f)

        with open(run_dir / "scenario.json", "w", encoding="utf-8") as f:
            json.dump(scenario, f)

        with open(run_dir / "events.json", "w", encoding="utf-8") as f:
            json.dump([], f)

        with open(run_dir / "k6-summary.json", "w", encoding="utf-8") as f:
            json.dump({
                "workload_integrity": {"total_requests": 2, "dropped_iterations": 0},
                "outcomes": {"successful_200": 2},
            }, f)

        with open(run_dir / "k6-output.log", "w", encoding="utf-8") as f:
            f.write("k6 benchmark log\n")

        with open(run_dir / "requests.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(REQUESTS_CSV_COLUMNS)
            writer.writerow([0, "2023-11-14T22:13:21Z", 1.0, 25.0, 200, "success", True, False, False, "backend-a", "measurement", "steady_state", "scenario_1_homogeneous", "round-robin", 1])
            writer.writerow([1, "2023-11-14T22:13:22Z", 2.0, 26.0, 200, "success", True, False, False, "backend-b", "measurement", "steady_state", "scenario_1_homogeneous", "round-robin", 1])

        with open(run_dir / "metrics.json", "w", encoding="utf-8") as f:
            json.dump({
                "trial_metadata": {"scenario_id": "scenario_1_homogeneous", "routing_policy": "round-robin"},
                "workload_integrity": {"total_requests": 2, "dropped_iterations": 0},
                "latency_ms": {
                    "successful_requests": {"mean": 25.5, "min": 25.0, "p50": 25.5, "p90": 25.9, "p95": 25.95, "p99": 25.99, "max": 26.0},
                    "all_requests": {"mean": 25.5, "min": 25.0, "p50": 25.5, "p90": 25.9, "p95": 25.95, "p99": 25.99, "max": 26.0},
                },
            }, f)

        is_valid, errors = DataValidator.validate_trial(
            run_dir=run_dir,
            scenario=scenario,
            policy="round-robin",
            trial_index=1,
        )
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)

    # 7. Data validator detects missing artifact
    def test_data_validator_detects_missing_artifact(self):
        run_dir = Path(self.test_dir) / "missing_file_run"
        run_dir.mkdir(parents=True, exist_ok=True)

        scenario = {"id": "test", "duration_seconds": 10.0}
        # Only write metadata.json
        with open(run_dir / "metadata.json", "w", encoding="utf-8") as f:
            json.dump({"scenario_id": "test", "routing_policy": "round-robin", "trial_index": 1}, f)

        is_valid, errors = DataValidator.validate_trial(run_dir, scenario, "round-robin", 1)
        self.assertFalse(is_valid)
        self.assertTrue(any("Missing required artifact" in err for err in errors))

    # 8. Data validator detects request count mismatch
    def test_data_validator_detects_count_mismatch(self):
        run_dir = Path(self.test_dir) / "mismatch_run"
        run_dir.mkdir(parents=True, exist_ok=True)
        scenario = {"id": "test", "duration_seconds": 10.0, "events": []}

        for fname in ["metadata.json", "scenario.json", "events.json", "k6-output.log"]:
            with open(run_dir / fname, "w", encoding="utf-8") as f:
                if fname.endswith(".json"):
                    json.dump({"scenario_id": "test", "routing_policy": "round-robin", "trial_index": 1}, f)
                else:
                    f.write("log\n")

        # k6-summary says 5 requests
        with open(run_dir / "k6-summary.json", "w", encoding="utf-8") as f:
            json.dump({"workload_integrity": {"total_requests": 5, "dropped_iterations": 0}}, f)

        # metrics.json says 10 requests
        with open(run_dir / "metrics.json", "w", encoding="utf-8") as f:
            json.dump({
                "trial_metadata": {"scenario_id": "test", "routing_policy": "round-robin"},
                "workload_integrity": {"total_requests": 10, "dropped_iterations": 0},
                "latency_ms": {"successful_requests": {}, "all_requests": {}},
            }, f)

        # requests.csv has 2 rows
        with open(run_dir / "requests.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(REQUESTS_CSV_COLUMNS)
            writer.writerow([0, "2023-11-14T22:13:21Z", 1.0, 25.0, 200, "success", True, False, False, "backend-a", "measurement", "steady_state", "test", "round-robin", 1])
            writer.writerow([1, "2023-11-14T22:13:22Z", 2.0, 26.0, 200, "success", True, False, False, "backend-b", "measurement", "steady_state", "test", "round-robin", 1])

        is_valid, errors = DataValidator.validate_trial(run_dir, scenario, "round-robin", 1)
        self.assertFalse(is_valid)
        self.assertTrue(any("Request count mismatch" in err for err in errors))

    # 9. Data validator detects invalid / negative duration
    def test_data_validator_detects_invalid_duration(self):
        run_dir = Path(self.test_dir) / "invalid_duration_run"
        run_dir.mkdir(parents=True, exist_ok=True)
        scenario = {"id": "test", "duration_seconds": 10.0, "events": []}

        for fname in ["metadata.json", "scenario.json", "events.json", "k6-output.log"]:
            with open(run_dir / fname, "w", encoding="utf-8") as f:
                if fname.endswith(".json"):
                    json.dump({"scenario_id": "test", "routing_policy": "round-robin", "trial_index": 1}, f)
                else:
                    f.write("log\n")

        with open(run_dir / "k6-summary.json", "w", encoding="utf-8") as f:
            json.dump({"workload_integrity": {"total_requests": 1, "dropped_iterations": 0}}, f)

        with open(run_dir / "metrics.json", "w", encoding="utf-8") as f:
            json.dump({
                "trial_metadata": {"scenario_id": "test", "routing_policy": "round-robin"},
                "workload_integrity": {"total_requests": 1, "dropped_iterations": 0},
                "latency_ms": {
                    "successful_requests": {"mean": -10.0, "min": -10.0, "p50": -10.0, "p90": -10.0, "p95": -10.0, "p99": -10.0, "max": -10.0},
                    "all_requests": {"mean": -10.0, "min": -10.0, "p50": -10.0, "p90": -10.0, "p95": -10.0, "p99": -10.0, "max": -10.0},
                },
            }, f)

        with open(run_dir / "requests.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(REQUESTS_CSV_COLUMNS)
            writer.writerow([0, "2023-11-14T22:13:21Z", 1.0, -10.0, 200, "success", True, False, False, "backend-a", "measurement", "steady_state", "test", "round-robin", 1])

        is_valid, errors = DataValidator.validate_trial(run_dir, scenario, "round-robin", 1)
        self.assertFalse(is_valid)
        self.assertTrue(any("invalid duration" in err.lower() for err in errors))

    # 10. Data validator detects dropped iterations violation
    def test_data_validator_detects_dropped_iterations(self):
        run_dir = Path(self.test_dir) / "dropped_iters_run"
        run_dir.mkdir(parents=True, exist_ok=True)
        scenario = {"id": "test", "duration_seconds": 10.0, "events": []}

        for fname in ["metadata.json", "scenario.json", "events.json", "k6-output.log"]:
            with open(run_dir / fname, "w", encoding="utf-8") as f:
                if fname.endswith(".json"):
                    json.dump({"scenario_id": "test", "routing_policy": "round-robin", "trial_index": 1}, f)
                else:
                    f.write("log\n")

        with open(run_dir / "k6-summary.json", "w", encoding="utf-8") as f:
            json.dump({"workload_integrity": {"total_requests": 10, "dropped_iterations": 4}}, f)

        with open(run_dir / "metrics.json", "w", encoding="utf-8") as f:
            json.dump({
                "trial_metadata": {"scenario_id": "test", "routing_policy": "round-robin"},
                "workload_integrity": {"total_requests": 10, "dropped_iterations": 4},
                "latency_ms": {"successful_requests": {}, "all_requests": {}},
            }, f)

        with open(run_dir / "requests.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(REQUESTS_CSV_COLUMNS)

        is_valid, errors = DataValidator.validate_trial(run_dir, scenario, "round-robin", 1)
        self.assertFalse(is_valid)
        self.assertTrue(any("dropped iterations" in err.lower() for err in errors))

    # 11. Large file streaming memory safety
    def test_large_file_streaming_memory_safety(self):
        scenario = {"id": "large_test", "duration_seconds": 100.0, "timeline": {}}
        timeline = IntervalTimeline(events=[], scenario=scenario)

        large_file = Path(self.test_dir) / "large_raw_metrics.json"
        num_records = 5000

        with open(large_file, "w", encoding="utf-8") as f:
            for i in range(num_records):
                point = {
                    "metric": "sbr_request_observation",
                    "type": "Point",
                    "data": {
                        "time": "2023-11-14T22:13:21.000000000Z",
                        "value": 20.0 + (i % 50),
                        "tags": {
                            "status": "200",
                            "outcome": "success",
                            "backend_id": f"backend-{chr(97 + (i % 5))}",
                            "phase": "measurement",
                        },
                    },
                }
                f.write(json.dumps(point) + "\n")

        run_dir = Path(self.test_dir) / "large_run"
        csv_path, metrics_path, metrics = DataCollector.collect_and_export(
            raw_metrics_path=large_file,
            output_dir=run_dir,
            workload_start_wall=1700000000.0,
            timeline=timeline,
            scenario=scenario,
            policy="round-robin",
            trial_index=1,
            dropped_iterations=0,
            keep_raw_metrics=False,
        )

        self.assertTrue(csv_path.is_file())
        self.assertTrue(metrics_path.is_file())
        self.assertFalse(large_file.exists())  # Unlinked as requested
        self.assertEqual(metrics["outcomes"]["total_requests"], num_records)
        self.assertEqual(metrics["outcomes"]["successful_requests"], num_records)


if __name__ == "__main__":
    unittest.main()

