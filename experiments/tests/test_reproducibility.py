"""
Deterministic reproducibility and scientific-validity test suite for self-balancing-router.
Verifies trial isolation, random seed semantics, metadata completeness, policy equivalence,
timing interval alignment, 503 overload origin classification, and data integrity rules.
"""

import csv
import json
import math
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from experiments.orchestrator.config import (
    DEFAULT_ADMIN_PORTS,
    DEFAULT_PROXY_PORT,
    ExperimentConfig,
    PortConflictError,
    load_scenario,
    normalize_policy,
    verify_no_port_conflicts,
)
from experiments.orchestrator.data import (
    REQUESTS_CSV_COLUMNS,
    VALID_OUTCOMES,
    VALID_OVERLOAD_ORIGINS,
    DataCollector,
    DataValidator,
    IntervalTimeline,
)
from experiments.orchestrator.runner import (
    ExperimentRunner,
    get_docker_version,
    get_git_commit,
    get_k6_version,
    resolve_run_dir,
)
from experiments.orchestrator.sync import SyncCoordinator


class TestReproducibilityAndValidation(unittest.TestCase):
    """Test suite for experimental reproducibility, fair comparisons, and validation."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="sbr_repro_test_")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    # 1. Deterministic seed progression across repeated trials
    def test_seed_derivation_and_repetition_progression(self):
        scenario = {
            "id": "scenario_seed_test",
            "name": "Seed Progression Test",
            "duration_seconds": 10.0,
            "timeline": {"warmup_seconds": 2.0, "measurement_seconds": 6.0, "cooldown_seconds": 2.0},
            "workload": {"stages": [{"duration": "10s", "target": 10}]},
            "backends": {
                "backend-a": {"processing_delay": "10ms", "processing_jitter": "2ms"},
                "backend-b": {"processing_delay": "10ms", "processing_jitter": "2ms"},
                "backend-c": {"processing_delay": "10ms", "processing_jitter": "2ms"},
                "backend-d": {"processing_delay": "10ms", "processing_jitter": "2ms"},
                "backend-e": {"processing_delay": "10ms", "processing_jitter": "2ms"},
            },
            "events": [],
        }

        base_seed = 100
        cfg = ExperimentConfig(
            scenario=scenario,
            routing_policy="round-robin",
            repetitions=3,
            seed=base_seed,
            output_dir=Path(self.test_dir),
            dry_run=True,
        )
        runner = ExperimentRunner(cfg)

        res1 = runner.run_trial(trial_index=1)
        res2 = runner.run_trial(trial_index=2)
        res3 = runner.run_trial(trial_index=3)

        # Base seeds
        self.assertEqual(res1.metadata["random_seed"], 100)
        self.assertEqual(res2.metadata["random_seed"], 101)
        self.assertEqual(res3.metadata["random_seed"], 102)

        # Backend derived seeds
        for idx, res in enumerate([res1, res2, res3], start=1):
            expected_base = base_seed + idx - 1
            seeds = res.metadata["backend_seeds"]
            self.assertEqual(seeds["backend-a"], expected_base + 1)
            self.assertEqual(seeds["backend-b"], expected_base + 2)
            self.assertEqual(seeds["backend-c"], expected_base + 3)
            self.assertEqual(seeds["backend-d"], expected_base + 4)
            self.assertEqual(seeds["backend-e"], expected_base + 5)

    # 2. Metadata completeness and tool version capture
    def test_metadata_completeness_and_versions(self):
        scenario = {
            "id": "scenario_meta_test",
            "name": "Metadata Completeness Test",
            "duration_seconds": 15.0,
            "timeline": {"warmup_seconds": 3.0, "measurement_seconds": 9.0, "cooldown_seconds": 3.0},
            "workload": {"stages": [{"duration": "15s", "target": 5}]},
            "backends": {f"backend-{c}": {"processing_delay": "10ms", "processing_jitter": "1ms"} for c in "abcde"},
            "events": [],
        }

        cfg = ExperimentConfig(
            scenario=scenario,
            routing_policy="least-requests",
            repetitions=1,
            seed=42,
            output_dir=Path(self.test_dir),
            vus_preallocated=10,
            vus_max=25,
            dry_run=True,
        )
        runner = ExperimentRunner(cfg)
        result = runner.run_trial(trial_index=1)
        meta = result.metadata

        # Core identifiers
        self.assertEqual(meta["scenario_id"], "scenario_meta_test")
        self.assertEqual(meta["routing_policy"], "least-requests")
        self.assertEqual(meta["trial_index"], 1)
        self.assertEqual(meta["random_seed"], 42)

        # Versions section
        self.assertIn("python", meta["versions"])
        self.assertIn("docker", meta["versions"])
        self.assertIn("k6", meta["versions"])
        self.assertIn("k6_execution_method", meta["versions"])
        self.assertIn("os", meta["versions"])

        # Reproducibility section separating deterministic from nondeterministic factors
        self.assertIn("reproducibility", meta)
        det = meta["reproducibility"]["deterministic_elements"]
        nondet = meta["reproducibility"]["nondeterministic_elements"]
        self.assertTrue(any("Backend random seeds" in d for d in det))
        self.assertTrue(any("goroutine" in n or "scheduling" in n.lower() for n in nondet))

        # Workload configuration
        wl_cfg = meta["configuration"]["workload"]
        self.assertEqual(wl_cfg["vus_preallocated"], 10)
        self.assertEqual(wl_cfg["vus_max"], 25)

        # Backend configurations
        self.assertIn("backend-a", meta["backend_configurations"]["baseline"])
        self.assertEqual(len(meta["backend_configurations"]["seeds"]), 5)

    # 3. Fair policy comparisons: identical configurations across policies
    def test_fair_policy_comparisons_configuration_equivalence(self):
        scenario = {
            "id": "scenario_equiv_test",
            "name": "Policy Equivalence Test",
            "duration_seconds": 20.0,
            "timeline": {"warmup_seconds": 5.0, "measurement_seconds": 10.0, "cooldown_seconds": 5.0},
            "workload": {"stages": [{"duration": "20s", "target": 15}]},
            "backends": {
                "backend-a": {"processing_delay": "10ms", "processing_jitter": "1ms"},
                "backend-b": {"processing_delay": "20ms", "processing_jitter": "2ms"},
                "backend-c": {"processing_delay": "30ms", "processing_jitter": "3ms"},
                "backend-d": {"processing_delay": "40ms", "processing_jitter": "4ms"},
                "backend-e": {"processing_delay": "50ms", "processing_jitter": "5ms"},
            },
            "events": [
                {
                    "timestamp_seconds": 10.0,
                    "target": "backend-a",
                    "action": "update",
                    "parameters": {"processing_delay": "100ms", "processing_jitter": "10ms"},
                }
            ],
        }

        # Configure two different routing policies
        cfg_rr = ExperimentConfig(
            scenario=scenario,
            routing_policy="round-robin",
            repetitions=1,
            seed=500,
            output_dir=Path(self.test_dir),
            dry_run=True,
        )
        cfg_lr = ExperimentConfig(
            scenario=scenario,
            routing_policy="least-requests",
            repetitions=1,
            seed=500,
            output_dir=Path(self.test_dir),
            dry_run=True,
        )

        runner_rr = ExperimentRunner(cfg_rr)
        runner_lr = ExperimentRunner(cfg_lr)

        res_rr = runner_rr.run_trial(trial_index=1)
        res_lr = runner_lr.run_trial(trial_index=1)

        # Verify scenario definitions match exactly
        self.assertEqual(res_rr.metadata["scenario_definition"], res_lr.metadata["scenario_definition"])

        # Verify backend initial delay profiles match exactly
        self.assertEqual(
            res_rr.metadata["backend_configurations"]["baseline"],
            res_lr.metadata["backend_configurations"]["baseline"],
        )

        # Verify backend seeds match exactly for the same trial index
        self.assertEqual(
            res_rr.metadata["backend_configurations"]["seeds"],
            res_lr.metadata["backend_configurations"]["seeds"],
        )

        # Verify timeline boundaries match exactly
        self.assertEqual(
            res_rr.metadata["scenario_definition"]["timeline"],
            res_lr.metadata["scenario_definition"]["timeline"],
        )

        # Verify output directories are isolated per policy
        self.assertNotEqual(res_rr.run_dir, res_lr.run_dir)
        self.assertIn("round-robin", str(res_rr.run_dir))
        self.assertIn("least-requests", str(res_lr.run_dir))

    # 4. 503 Overload origin distinction (backend vs proxy vs unknown)
    def test_503_overload_origin_distinction(self):
        scenario = {
            "id": "scenario_overload_test",
            "duration_seconds": 20.0,
            "timeline": {"warmup_seconds": 5.0, "measurement_seconds": 10.0, "cooldown_seconds": 5.0},
        }
        timeline = IntervalTimeline(events=[], scenario=scenario)

        raw_file = Path(self.test_dir) / "raw_503_metrics.json"
        records = [
            # 1. Backend 503: body evidence shows backend overloaded
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2026-11-14T03:00:01.000Z",
                    "value": 1.5,
                    "tags": {
                        "status": "503",
                        "outcome": "overload_503",
                        "backend_id": "",
                        "overload_origin": "backend",
                        "phase": "measurement",
                    },
                },
            },
            # 2. Proxy 503: proxy routing unavailable
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2026-11-14T03:00:02.000Z",
                    "value": 0.8,
                    "tags": {
                        "status": "503",
                        "outcome": "overload_503",
                        "backend_id": "",
                        "overload_origin": "proxy",
                        "phase": "measurement",
                    },
                },
            },
            # 3. Unknown 503: unobservable origin
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2026-11-14T03:00:03.000Z",
                    "value": 1.2,
                    "tags": {
                        "status": "503",
                        "outcome": "overload_503",
                        "backend_id": "",
                        "overload_origin": "unknown",
                        "phase": "measurement",
                    },
                },
            },
            # 4. Normal 200
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2026-11-14T03:00:04.000Z",
                    "value": 25.0,
                    "tags": {
                        "status": "200",
                        "outcome": "success",
                        "backend_id": "backend-a",
                        "phase": "measurement",
                    },
                },
            },
        ]

        with open(raw_file, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

        run_dir = Path(self.test_dir) / "run_503"
        csv_path, metrics_path, metrics_data = DataCollector.collect_and_export(
            raw_metrics_path=raw_file,
            output_dir=run_dir,
            workload_start_wall=1700000000.0,
            timeline=timeline,
            scenario=scenario,
            policy="round-robin",
            trial_index=1,
            dropped_iterations=0,
        )

        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))

        # Check CSV overload_origin column
        self.assertEqual(rows[1][10], "backend")  # Row 0 (backend 503)
        self.assertEqual(rows[2][10], "proxy")    # Row 1 (proxy 503)
        self.assertEqual(rows[3][10], "unknown")  # Row 2 (unknown 503)
        self.assertEqual(rows[4][10], "")         # Row 3 (200 success)

        # Check metrics.json overload_503_breakdown
        breakdown = metrics_data["outcomes"]["overload_503_breakdown"]
        self.assertEqual(breakdown["backend"], 1)
        self.assertEqual(breakdown["proxy"], 1)
        self.assertEqual(breakdown["unknown"], 1)
        self.assertEqual(metrics_data["outcomes"]["overload_503_count"], 3)

    # 5. Timestamp semantics: completion time, derived dispatch, and boundary-spanning requests
    def test_timestamp_semantics_and_boundary_spanning(self):
        scenario = {
            "id": "scenario_boundary_test",
            "duration_seconds": 30.0,
            "timeline": {"warmup_seconds": 5.0, "measurement_seconds": 20.0, "cooldown_seconds": 5.0},
        }
        # Degradation event at 10.0s
        events = [{
            "event_index": 0,
            "actual_trigger_relative_seconds": 10.0,
            "planned_timestamp_seconds": 10.0,
            "action": "update",
            "target": "backend-b",
            "success": True,
            "verified": True,
        }]
        timeline = IntervalTimeline(events=events, scenario=scenario)
        start_wall = 1700000000.0

        raw_file = Path(self.test_dir) / "raw_boundary_metrics.json"
        records = [
            # Request 1: Dispatched at 9.0s, completed at 9.2s (fully inside baseline)
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2023-11-14T22:13:29.200Z",  # start_wall + 9.2s
                    "value": 200.0,  # 0.2s duration
                    "tags": {"status": "200", "outcome": "success", "backend_id": "backend-a", "phase": "measurement"},
                },
            },
            # Request 2: Dispatched at 9.8s, completed at 10.3s (SPANS BOUNDARY at 10.0s)
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2023-11-14T22:13:30.300Z",  # start_wall + 10.3s
                    "value": 500.0,  # 0.5s duration -> dispatch at 9.8s
                    "tags": {"status": "200", "outcome": "success", "backend_id": "backend-b", "phase": "measurement"},
                },
            },
            # Request 3: Dispatched at 10.5s, completed at 10.7s (fully inside degradation)
            {
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": "2023-11-14T22:13:30.700Z",  # start_wall + 10.7s
                    "value": 200.0,  # 0.2s duration
                    "tags": {"status": "200", "outcome": "success", "backend_id": "backend-c", "phase": "measurement"},
                },
            },
        ]

        with open(raw_file, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

        # Mock parse_iso_to_epoch to align exactly with start_wall + offset
        run_dir = Path(self.test_dir) / "run_boundary"
        with patch("experiments.orchestrator.data.parse_iso_to_epoch") as mock_parse:
            mock_parse.side_effect = [
                start_wall + 9.2,   # Req 1 completion
                start_wall + 10.3,  # Req 2 completion (dispatch = 9.8)
                start_wall + 10.7,  # Req 3 completion (dispatch = 10.5)
            ]
            csv_path, metrics_path, metrics_data = DataCollector.collect_and_export(
                raw_metrics_path=raw_file,
                output_dir=run_dir,
                workload_start_wall=start_wall,
                timeline=timeline,
                scenario=scenario,
                policy="p2c",
                trial_index=1,
            )

        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))

        # Req 1: dispatch at 9.0s (baseline), completion at 9.2s (baseline) -> spans_boundary = False
        self.assertEqual(rows[1][12], "baseline")
        self.assertEqual(rows[1][13], "False")

        # Req 2: dispatch at 9.8s (baseline), completion at 10.3s (degradation) -> spans_boundary = True!
        self.assertEqual(rows[2][12], "baseline")  # aligned to dispatch
        self.assertEqual(rows[2][13], "True")

        # Req 3: dispatch at 10.5s (degradation), completion at 10.7s (degradation) -> spans_boundary = False
        self.assertEqual(rows[3][12], "degradation:backend-b")
        self.assertEqual(rows[3][13], "False")

        # Check metrics.json timing semantics
        timing = metrics_data["workload_integrity"]["timing_semantics"]
        self.assertEqual(timing["request_timestamp_event"], "completion")
        self.assertEqual(timing["interval_alignment_reference"], "dispatch")
        self.assertEqual(timing["total_boundary_spanning_requests"], 1)

    # 6. Preserving failed trials and their failure reasons on disk
    def test_failed_trial_preservation_and_reporting(self):
        scenario = {
            "id": "scenario_fail_test",
            "name": "Failed Trial Preservation Test",
            "duration_seconds": 10.0,
            "timeline": {"warmup_seconds": 2.0, "measurement_seconds": 6.0, "cooldown_seconds": 2.0},
            "workload": {"stages": [{"duration": "10s", "target": 10}]},
            "backends": {f"backend-{c}": {"processing_delay": "10ms", "processing_jitter": "1ms"} for c in "abcde"},
            "events": [],
        }

        run_dir = Path(self.test_dir) / "failed_trial_run"
        run_dir.mkdir(parents=True, exist_ok=True)

        # Write all artifacts but inject a dropped iterations failure in summary and metrics
        with open(run_dir / "scenario.json", "w", encoding="utf-8") as f:
            json.dump(scenario, f)
        with open(run_dir / "events.json", "w", encoding="utf-8") as f:
            json.dump([], f)
        with open(run_dir / "k6-output.log", "w", encoding="utf-8") as f:
            f.write("simulation error\n")
        with open(run_dir / "k6-summary.json", "w", encoding="utf-8") as f:
            json.dump({
                "workload_integrity": {"total_requests": 50, "dropped_iterations": 12},
                "outcomes": {"successful_200": 38},
            }, f)
        with open(run_dir / "requests.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(REQUESTS_CSV_COLUMNS)
            for i in range(50):
                writer.writerow([
                    i, "2026-11-14T03:00:00Z", 1.0, 20.0, 200, "success",
                    True, False, False, "backend-a", "", "measurement", "steady_state", False,
                    "scenario_fail_test", "round-robin", 1,
                ])
        with open(run_dir / "metrics.json", "w", encoding="utf-8") as f:
            json.dump({
                "trial_metadata": {"scenario_id": "scenario_fail_test", "routing_policy": "round-robin"},
                "workload_integrity": {"total_requests": 50, "dropped_iterations": 12},
                "outcomes": {
                    "total_requests": 50,
                    "successful_requests": 50,
                    "overload_503_count": 0,
                    "overload_503_breakdown": {"backend": 0, "proxy": 0, "unknown": 0},
                    "status_distribution": {"200": 50},
                },
                "latency_ms": {
                    "successful_requests": {"count": 50, "mean": 20.0, "min": 20.0, "p50": 20.0, "p90": 20.0, "p95": 20.0, "p99": 20.0, "max": 20.0},
                    "all_requests": {"count": 50, "mean": 20.0, "min": 20.0, "p50": 20.0, "p90": 20.0, "p95": 20.0, "p99": 20.0, "max": 20.0},
                },
            }, f)
        with open(run_dir / "metadata.json", "w", encoding="utf-8") as f:
            json.dump({
                "scenario_id": "scenario_fail_test",
                "routing_policy": "round-robin",
                "trial_index": 1,
                "valid": False,
                "failure_reason": "Workload integrity violated: 12 dropped iterations",
            }, f)

        is_valid, errors = DataValidator.validate_trial(
            run_dir=run_dir,
            scenario=scenario,
            policy="round-robin",
            trial_index=1,
        )

        self.assertFalse(is_valid)
        self.assertTrue(any("dropped iterations" in err.lower() for err in errors))

        # Crucial check: verify all artifacts remain intact on disk for post-mortem analysis
        for art in DataValidator.REQUIRED_ARTIFACTS:
            self.assertTrue((run_dir / art).is_file(), f"Artifact {art} must be preserved on failure")

    # 7. Port conflict prevention with development stack
    def test_port_conflict_detection(self):
        # Mock is_port_in_use to return True for port 8080 (normal dev proxy)
        with patch("experiments.orchestrator.config.is_port_in_use") as mock_in_use:
            mock_in_use.side_effect = lambda port, host: port == 8080

            with self.assertRaises(PortConflictError) as ctx:
                verify_no_port_conflicts(proxy_port=8080, admin_ports=DEFAULT_ADMIN_PORTS)

            self.assertIn("proxy:8080", str(ctx.exception))

    # 8. Experiment history index and INDEX.md table generation
    def test_experiment_history_index_and_markdown(self):
        from experiments.orchestrator.history import ExperimentHistory

        results_dir = Path(self.test_dir) / "results_history"
        results_dir.mkdir(parents=True, exist_ok=True)
        history = ExperimentHistory(results_dir)

        # Record Trial 1 (PASS)
        run_dir_1 = results_dir / "scenario_1" / "round-robin" / "run-001"
        run_dir_1.mkdir(parents=True, exist_ok=True)
        meta1 = {
            "scenario_id": "scenario_1",
            "scenario_name": "Homogeneous Backends",
            "routing_policy": "round-robin",
            "trial_index": 1,
            "random_seed": 42,
            "git_commit": "abcdef123456",
            "valid": True,
            "failure_reason": None,
            "start_time": "2026-11-14T03:00:00Z",
            "duration_seconds": 10.0,
            "note": "Baseline benchmark",
            "hypothesis": "Equal load distribution",
        }
        metrics1 = {
            "outcomes": {"total_requests": 100, "successful_requests": 100, "success_rate": 1.0},
            "latency_ms": {"successful_requests": {"count": 100, "mean": 15.5, "p50": 14.8, "p99": 22.1}},
        }
        history.record_trial(meta1, run_dir_1, metrics1, note=meta1["note"], hypothesis=meta1["hypothesis"])

        # Record Trial 2 (FAIL)
        run_dir_2 = results_dir / "scenario_1" / "round-robin" / "run-002"
        run_dir_2.mkdir(parents=True, exist_ok=True)
        meta2 = {
            "scenario_id": "scenario_1",
            "scenario_name": "Homogeneous Backends",
            "routing_policy": "round-robin",
            "trial_index": 2,
            "random_seed": 43,
            "git_commit": "abcdef123456",
            "valid": False,
            "failure_reason": "Dropped iterations detected: 4",
            "start_time": "2026-11-14T03:05:00Z",
            "duration_seconds": 10.0,
        }
        metrics2 = {
            "outcomes": {"total_requests": 96, "successful_requests": 96, "success_rate": 1.0},
            "latency_ms": {"successful_requests": {"count": 96, "mean": 18.2, "p50": 17.5, "p99": 35.0}},
        }
        history.record_trial(meta2, run_dir_2, metrics2)

        # Verify index.json contents
        index_data = history.load_index()
        self.assertEqual(index_data["total_trials"], 2)
        self.assertEqual(index_data["valid_trials"], 1)
        self.assertEqual(index_data["invalid_trials"], 1)

        # Verify INDEX.md content
        md_text = history.index_md_path.read_text(encoding="utf-8")
        self.assertIn("# Experiment History Index", md_text)
        self.assertIn("**PASS**", md_text)
        self.assertIn("*FAIL*", md_text)
        self.assertIn("[run-001](scenario_1/round-robin/run-001/)", md_text)
        self.assertIn("[run-002](scenario_1/round-robin/run-002/)", md_text)
        self.assertIn("Dropped iterations detected: 4", md_text)

    # 9. Research journal creation and session logging
    def test_experiment_journal_entry(self):
        from experiments.orchestrator.history import ExperimentHistory

        results_dir = Path(self.test_dir) / "journal_test"
        results_dir.mkdir(parents=True, exist_ok=True)
        history = ExperimentHistory(results_dir)

        run_dir = results_dir / "sc_eval" / "p2c" / "run-001"
        run_dir.mkdir(parents=True, exist_ok=True)
        meta = {
            "scenario_id": "sc_eval",
            "routing_policy": "p2c",
            "trial_index": 1,
            "random_seed": 77,
            "git_commit": "commit777",
            "valid": True,
            "start_time": "2026-11-14T04:00:00Z",
            "note": "Evaluated p2c under sudden degradation",
            "hypothesis": "P2C will avoid degraded backend without arm inertia",
        }
        metrics = {
            "outcomes": {"total_requests": 200, "successful_requests": 198, "success_rate": 0.99},
            "latency_ms": {"successful_requests": {"count": 198, "mean": 22.4, "p50": 20.1, "p99": 45.0}},
        }

        history.record_trial(meta, run_dir, metrics, note=meta["note"], hypothesis=meta["hypothesis"])

        # Verify JOURNAL.md created and updated
        self.assertTrue(history.journal_md_path.is_file())
        journal_text = history.journal_md_path.read_text(encoding="utf-8")
        self.assertIn("# Experimentation Research Journal", journal_text)
        self.assertIn("P2C will avoid degraded backend without arm inertia", journal_text)
        self.assertIn("Evaluated p2c under sudden degradation", journal_text)
        self.assertIn("200 total requests, 198 successful", journal_text)

    # 10. Raw measurement preservation with gzip compression
    def test_raw_metrics_compression_and_decompression(self):
        import gzip

        scenario = {
            "id": "scenario_gzip_test",
            "duration_seconds": 10.0,
            "timeline": {"warmup_seconds": 2.0, "measurement_seconds": 6.0, "cooldown_seconds": 2.0},
        }
        timeline = IntervalTimeline(events=[], scenario=scenario)

        raw_file = Path(self.test_dir) / "raw_gzip_metrics.json"
        raw_lines = [
            json.dumps({
                "metric": "sbr_request_observation",
                "type": "Point",
                "data": {
                    "time": f"2026-11-14T03:00:{i:02d}.000Z",
                    "value": 15.0 + float(i),
                    "tags": {"status": "200", "outcome": "success", "backend_id": "backend-a", "phase": "measurement"},
                },
            })
            for i in range(10)
        ]
        raw_file.write_text("\n".join(raw_lines) + "\n", encoding="utf-8")

        run_dir = Path(self.test_dir) / "run_gzip"
        DataCollector.collect_and_export(
            raw_metrics_path=raw_file,
            output_dir=run_dir,
            workload_start_wall=1700000000.0,
            timeline=timeline,
            scenario=scenario,
            policy="round-robin",
            trial_index=1,
            raw_metrics_mode="gz",
        )

        # Verify uncompressed file was removed and compressed file exists
        self.assertFalse(raw_file.exists())
        gz_file = run_dir / "raw-metrics.json.gz"
        self.assertTrue(gz_file.is_file())
        self.assertGreater(gz_file.stat().st_size, 0)

        # Decompress and verify all lines recovered intact
        with gzip.open(gz_file, "rt", encoding="utf-8") as f:
            recovered_lines = [line.strip() for line in f if line.strip()]

        self.assertEqual(len(recovered_lines), 10)
        first_point = json.loads(recovered_lines[0])
        self.assertEqual(first_point["metric"], "sbr_request_observation")
        self.assertEqual(first_point["data"]["value"], 15.0)

    # 11. Scan and rebuild history across discovered trials
    def test_scan_and_rebuild_history(self):
        from experiments.orchestrator.history import ExperimentHistory

        results_dir = Path(self.test_dir) / "scan_results"
        results_dir.mkdir(parents=True, exist_ok=True)

        # Create two mock trial directories
        for p, run in [("round-robin", "run-001"), ("least-requests", "run-001")]:
            tdir = results_dir / "sc_scan" / p / run
            tdir.mkdir(parents=True, exist_ok=True)
            with open(tdir / "metadata.json", "w", encoding="utf-8") as f:
                json.dump({
                    "scenario_id": "sc_scan",
                    "routing_policy": p,
                    "trial_index": 1,
                    "valid": True,
                    "start_time": "2026-11-14T05:00:00Z",
                }, f)
            with open(tdir / "metrics.json", "w", encoding="utf-8") as f:
                json.dump({
                    "outcomes": {"total_requests": 50, "successful_requests": 50, "success_rate": 1.0},
                    "latency_ms": {"successful_requests": {"count": 50, "mean": 12.0, "p50": 11.5, "p99": 20.0}},
                }, f)

        history = ExperimentHistory(results_dir)
        count = history.scan_and_rebuild()
        self.assertEqual(count, 2)

        idx = history.load_index()
        self.assertEqual(idx["total_trials"], 2)
        self.assertTrue((results_dir / "INDEX.md").is_file())


if __name__ == "__main__":
    unittest.main()

