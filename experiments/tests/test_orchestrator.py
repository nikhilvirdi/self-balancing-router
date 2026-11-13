"""
Unit test suite for the self-balancing-router Python experiment orchestrator.
Validates scenario loading, policy configuration, command building, directory allocation,
event scheduling, administrative requests, failed event handling, drift recording,
metadata serialization, invalid-run classification, subprocess handling, cleanup,
trial isolation, and credential redaction without requiring Docker or k6.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from experiments.orchestrator.admin import AdminClient, EventExecutionRecord
from experiments.orchestrator.cli import parse_admin_ports
from experiments.orchestrator.config import (
    DEFAULT_ADMIN_PORTS,
    DEFAULT_PROXY_PORT,
    SUPPORTED_POLICIES,
    ExperimentConfig,
    PortConflictError,
    ValidationError,
    is_port_in_use,
    load_scenario,
    normalize_policy,
    resolve_repo_root,
    validate_scenario,
    verify_no_port_conflicts,
)
from experiments.orchestrator.docker import DockerLifecycle
from experiments.orchestrator.k6 import K6ExecutionResult, K6Runner
from experiments.orchestrator.runner import (
    ExperimentRunner,
    TrialResult,
    get_git_commit,
    resolve_run_dir,
)
from experiments.orchestrator.sync import SyncCoordinator


class MockAdminHTTPHandler(BaseHTTPRequestHandler):
    """Mock handler simulating backend administrative endpoints."""

    workload_state = {
        "processing_delay": "20ms",
        "processing_jitter": "5ms",
        "baseline_processing_delay": "20ms",
        "baseline_processing_jitter": "5ms",
    }
    fail_put = False
    fail_reset = False
    received_token = None

    def do_GET(self):
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            MockAdminHTTPHandler.received_token = auth[7:]

        if self.path.startswith("/admin/v1/workload"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(self.workload_state).encode("utf-8"))
        elif self.path.startswith("/readyz"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok\n")
        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            MockAdminHTTPHandler.received_token = auth[7:]

        if self.fail_put:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'{"error":"simulated failure"}\n')
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8")
        payload = json.loads(body)

        MockAdminHTTPHandler.workload_state["processing_delay"] = payload.get("processing_delay", "20ms")
        MockAdminHTTPHandler.workload_state["processing_jitter"] = payload.get("processing_jitter", "5ms")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(MockAdminHTTPHandler.workload_state).encode("utf-8"))

    def do_POST(self):
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            MockAdminHTTPHandler.received_token = auth[7:]

        if self.fail_reset:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'{"error":"reset failure"}\n')
            return

        MockAdminHTTPHandler.workload_state["processing_delay"] = MockAdminHTTPHandler.workload_state["baseline_processing_delay"]
        MockAdminHTTPHandler.workload_state["processing_jitter"] = MockAdminHTTPHandler.workload_state["baseline_processing_jitter"]

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(MockAdminHTTPHandler.workload_state).encode("utf-8"))

    def log_message(self, format, *args):
        pass


class TestExperimentOrchestrator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), MockAdminHTTPHandler)
        cls.mock_port = cls.server.server_port
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        MockAdminHTTPHandler.fail_put = False
        MockAdminHTTPHandler.fail_reset = False
        MockAdminHTTPHandler.received_token = None
        MockAdminHTTPHandler.workload_state = {
            "processing_delay": "20ms",
            "processing_jitter": "5ms",
            "baseline_processing_delay": "20ms",
            "baseline_processing_jitter": "5ms",
        }
        self.test_dir = tempfile.mkdtemp(prefix="sbr_test_")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    # 1. Scenario loading and validation
    def test_scenario_loading_and_validation(self):
        # Load by ID
        s1 = load_scenario("scenario_1_homogeneous")
        self.assertEqual(s1["id"], "scenario_1_homogeneous")
        validate_scenario(s1)

        # Load by index
        s_idx = load_scenario("2")
        self.assertEqual(s_idx["id"], "scenario_2_heterogeneous")

        # Load by filename
        s_file = load_scenario("03_sudden_degradation.json")
        self.assertEqual(s_file["id"], "scenario_3_sudden_degradation")

        # Unknown scenario raises ValidationError
        with self.assertRaises(ValidationError):
            load_scenario("nonexistent_scenario_xyz")

        # Invalid scenario domain rules
        invalid_sc = dict(s1)
        invalid_sc["duration_seconds"] = -10.0
        with self.assertRaises(ValidationError):
            validate_scenario(invalid_sc)

    # 2. Policy configuration
    def test_policy_configuration(self):
        for p in SUPPORTED_POLICIES:
            self.assertEqual(normalize_policy(p), p)

        # Test underscore aliases
        self.assertEqual(normalize_policy("p2c_peak_ewma"), "p2c-peak-ewma")
        self.assertEqual(normalize_policy("round_robin"), "round-robin")
        self.assertEqual(normalize_policy("least_requests"), "least-requests")
        self.assertEqual(normalize_policy("first_eligible"), "first-eligible")

        with self.assertRaises(ValidationError):
            normalize_policy("unsupported-policy-name")

    # 3. Command construction
    def test_command_construction(self):
        runner = K6Runner(repo_root=resolve_repo_root(), exec_mode="auto")
        summary_path = Path(self.test_dir) / "k6-summary.json"

        # Local command construction
        cmd_local, _ = runner.build_command(
            scenario_id="scenario_1_homogeneous",
            proxy_port=8080,
            policy="round-robin",
            summary_output_path=summary_path,
            sync_url="http://127.0.0.1:9999/start",
            method="local",
        )
        self.assertEqual(cmd_local[0], "k6")
        self.assertIn("-e", cmd_local)
        self.assertIn("SBR_PROXY_URL=http://127.0.0.1:8080", cmd_local)
        self.assertIn("SBR_PROXY_POLICY=round-robin", cmd_local)
        self.assertIn("SBR_SYNC_URL=http://127.0.0.1:9999/start", cmd_local)

        # Docker command construction
        cmd_docker, _ = runner.build_command(
            scenario_id="scenario_1_homogeneous",
            proxy_port=8080,
            policy="round-robin",
            summary_output_path=summary_path,
            sync_url="http://host.docker.internal:9999/start",
            method="docker",
        )
        self.assertEqual(cmd_docker[0], "docker")
        self.assertIn("run", cmd_docker)
        self.assertIn("SBR_PROXY_URL=http://host.docker.internal:8080", cmd_docker)
        self.assertIn("SBR_SYNC_URL=http://host.docker.internal:9999/start", cmd_docker)

    # 4. Experiment directory creation
    def test_directory_creation(self):
        base = Path(self.test_dir)
        d1 = resolve_run_dir(base, "scenario_test", "round-robin")
        d2 = resolve_run_dir(base, "scenario_test", "round-robin")
        d3 = resolve_run_dir(base, "scenario_test", "round-robin")

        self.assertEqual(d1.name, "run-001")
        self.assertEqual(d2.name, "run-002")
        self.assertEqual(d3.name, "run-003")
        self.assertTrue(d1.is_dir())
        self.assertTrue(d2.is_dir())
        self.assertTrue(d3.is_dir())

    # 5. Event ordering and scheduling
    def test_event_ordering_and_scheduling(self):
        events = [
            {"timestamp_seconds": 80.0, "target": "backend-a", "action": "reset"},
            {"timestamp_seconds": 40.0, "target": "backend-a", "action": "update", "processing_delay": "100ms", "processing_jitter": "10ms"},
            {"timestamp_seconds": 10.0, "target": "backend-b", "action": "update", "processing_delay": "50ms", "processing_jitter": "5ms"},
        ]
        sorted_events = sorted(events, key=lambda ev: float(ev["timestamp_seconds"]))
        self.assertEqual(sorted_events[0]["timestamp_seconds"], 10.0)
        self.assertEqual(sorted_events[1]["timestamp_seconds"], 40.0)
        self.assertEqual(sorted_events[2]["timestamp_seconds"], 80.0)

    # 6. Administrative request construction
    def test_administrative_requests(self):
        ports = {"backend-a": self.mock_port}
        token = "secret-token-xyz-123"
        admin = AdminClient(admin_ports=ports, admin_token=token)

        # GET
        data = admin.get_workload("backend-a")
        self.assertEqual(data["processing_delay"], "20ms")
        self.assertEqual(MockAdminHTTPHandler.received_token, token)

        # PUT update
        up_data, lat = admin.update_workload("backend-a", "150ms", "25ms")
        self.assertEqual(up_data["processing_delay"], "150ms")
        self.assertGreaterEqual(lat, 0.0)

        # POST reset
        res_data, lat_res = admin.reset_workload("backend-a")
        self.assertEqual(res_data["processing_delay"], "20ms")
        self.assertGreaterEqual(lat_res, 0.0)

    # 7. Failed event handling
    def test_failed_event_handling(self):
        ports = {"backend-a": self.mock_port}
        admin = AdminClient(admin_ports=ports, admin_token="test-tok")

        MockAdminHTTPHandler.fail_put = True
        ev = {
            "timestamp_seconds": 10.0,
            "target": "backend-a",
            "action": "update",
            "processing_delay": "300ms",
            "processing_jitter": "30ms",
        }
        rec = admin.execute_event(ev, 0, workload_start_monotonic=time.monotonic() - 10.0)
        self.assertFalse(rec.success)
        self.assertFalse(rec.verified)
        self.assertIsNotNone(rec.error)
        self.assertIn("backend-a returned status 500", rec.error)

    # 8. Timing drift recording
    def test_timing_drift_recording(self):
        ports = {"backend-a": self.mock_port}
        admin = AdminClient(admin_ports=ports, admin_token="test-tok")

        # Simulate execution at 10.05s when planned was 10.0s -> drift +0.05s
        t0 = time.monotonic() - 10.05
        ev = {"timestamp_seconds": 10.0, "target": "backend-a", "action": "reset"}
        rec = admin.execute_event(ev, 0, workload_start_monotonic=t0)

        self.assertTrue(rec.success)
        self.assertAlmostEqual(rec.planned_timestamp_seconds, 10.0, places=2)
        self.assertGreater(rec.actual_trigger_relative_seconds, 10.0)
        self.assertAlmostEqual(rec.scheduling_drift_seconds, 0.05, delta=0.02)
        self.assertGreaterEqual(rec.request_latency_seconds, 0.0)

    # 9. Metadata serialization
    def test_metadata_serialization(self):
        scenario = load_scenario("scenario_1_homogeneous")
        run_dir = Path(self.test_dir) / "run-001"
        run_dir.mkdir(parents=True)

        meta = {
            "scenario_id": scenario["id"],
            "routing_policy": "round-robin",
            "trial_index": 1,
            "random_seed": 42,
            "git_commit": "abc1234",
            "valid": True,
            "failure_reason": None,
            "timing_summary": {
                "max_scheduling_drift_seconds": 0.005,
                "max_request_latency_seconds": 0.012,
            },
        }

        meta_path = run_dir / "metadata.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

        with open(meta_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        self.assertEqual(loaded["scenario_id"], scenario["id"])
        self.assertEqual(loaded["routing_policy"], "round-robin")
        self.assertTrue(loaded["valid"])
        self.assertEqual(loaded["timing_summary"]["max_scheduling_drift_seconds"], 0.005)

    # 10. Invalid-run classification
    def test_invalid_run_classification(self):
        runner = K6Runner(repo_root=resolve_repo_root())
        summary_path = Path(self.test_dir) / "summary.json"
        log_path = Path(self.test_dir) / "output.log"

        # Case A: Dropped iterations
        dropped_data = {
            "workload_integrity": {"dropped_iterations": 15, "total_requests": 500},
            "outcomes": {"successful_200": 485, "overload_503": 0},
        }

        with patch("subprocess.Popen") as mock_popen:
            def simulate_k6_run(*args, **kwargs):
                with open(summary_path, "w", encoding="utf-8") as f:
                    json.dump(dropped_data, f)
                return 0

            mock_proc = MagicMock()
            mock_proc.wait.side_effect = simulate_k6_run
            mock_proc.stdout = []
            mock_popen.return_value = mock_proc

            res = runner.run_workload(["k6"], {}, summary_path, log_path)
            self.assertFalse(res.valid)
            self.assertIn("Dropped iterations", res.validation_reason)

        # Case B: Non-zero exit code
        with patch("subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_proc.wait.return_value = 1
            mock_proc.stdout = []
            mock_popen.return_value = mock_proc

            res = runner.run_workload(["k6"], {}, summary_path, log_path)
            self.assertFalse(res.valid)
            self.assertIn("non-zero code 1", res.validation_reason)

    # 11. Subprocess failures and interruption
    def test_subprocess_failures_and_interruption(self):
        runner = K6Runner(repo_root=resolve_repo_root())
        summary_path = Path(self.test_dir) / "summary.json"
        log_path = Path(self.test_dir) / "output.log"

        with patch("subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_proc.wait.side_effect = KeyboardInterrupt
            mock_proc.stdout = []
            mock_popen.return_value = mock_proc

            with self.assertRaises(KeyboardInterrupt):
                runner.run_workload(["k6"], {}, summary_path, log_path)
            mock_proc.kill.assert_called_once()

    # 12. Cleanup behavior
    def test_cleanup_behavior(self):
        sync = SyncCoordinator()
        sync.start_server()
        self.assertGreater(sync.port, 0)
        sync.close()
        self.assertIsNone(sync._server)

        # Verify docker stop invocation
        docker = DockerLifecycle("test-project", env={"TEST": "1"})
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            docker.stop()
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            self.assertIn("down", cmd)
            self.assertIn("-p", cmd)
            self.assertIn("test-project", cmd)

    # 13. Repeated-trial isolation
    def test_repeated_trial_isolation(self):
        scenario = load_scenario("scenario_1_homogeneous")
        cfg = ExperimentConfig(
            scenario=scenario,
            routing_policy="round-robin",
            repetitions=3,
            seed=100,
            output_dir=Path(self.test_dir),
            dry_run=True,
        )
        runner = ExperimentRunner(cfg)
        results = runner.run_all()

        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].metadata["random_seed"], 100)
        self.assertEqual(results[1].metadata["random_seed"], 101)
        self.assertEqual(results[2].metadata["random_seed"], 102)

        self.assertEqual(results[0].run_dir.name, "run-001")
        self.assertEqual(results[1].run_dir.name, "run-002")
        self.assertEqual(results[2].run_dir.name, "run-003")

    # 14. Credential redaction
    def test_credential_redaction(self):
        secret = "super-secret-production-token-999"
        admin = AdminClient(admin_ports={"backend-a": 19091}, admin_token=secret)

        leaked_msg = f"Error connecting to server with token {secret} on port 19091"
        sanitized = admin.redact(leaked_msg)
        self.assertNotIn(secret, sanitized)
        self.assertIn("[REDACTED]", sanitized)

        # Check port parsing
        parsed = parse_admin_ports("19001,19002,19003,19004,19005")
        self.assertEqual(parsed["backend-a"], 19001)
        self.assertEqual(parsed["backend-e"], 19005)

    # 15. CLI argument parsing and VU override command generation
    def test_cli_parsing_and_vu_overrides(self):
        from experiments.orchestrator.cli import build_parser
        parser = build_parser()
        args = parser.parse_args([
            "--scenario", "scenario_1_homogeneous",
            "--policy", "least-requests",
            "--vus-preallocated", "25",
            "--vus-max", "75",
        ])
        self.assertEqual(args.policy, "least-requests")
        self.assertEqual(args.vus_preallocated, 25)
        self.assertEqual(args.vus_max, 75)

        runner = K6Runner(repo_root=Path(self.test_dir))
        cmd, _ = runner.build_command(
            scenario_id="scenario_1_homogeneous",
            proxy_port=8080,
            policy="least-requests",
            summary_output_path=Path(self.test_dir) / "summary.json",
            sync_url="http://127.0.0.1:9999/start",
            preallocated_vus=25,
            max_vus=75,
            method="docker",
        )
        cmd_str = " ".join(cmd)
        self.assertIn("SBR_VUS_PREALLOCATED=25", cmd_str)
        self.assertIn("SBR_VUS_MAX=75", cmd_str)


if __name__ == "__main__":
    unittest.main()
