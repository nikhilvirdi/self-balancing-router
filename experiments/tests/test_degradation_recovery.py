import json
from pathlib import Path
import unittest

from experiments.degradation_recovery import BACKENDS, PHASES, phase_at, sustained_crossing, verify_traces
from experiments.orchestrator.config import load_scenario


class DegradationRecoveryTest(unittest.TestCase):
    def test_scenario_uses_one_constant_stage_and_preserves_baseline(self):
        path = Path(__file__).resolve().parents[1] / "scenarios" / "08_degradation_recovery.json"
        scenario = load_scenario("8")
        self.assertEqual(scenario, json.loads(path.read_text(encoding="utf-8")))
        self.assertEqual(scenario["backends"], load_scenario("2")["backends"])
        self.assertEqual(scenario["workload"], {
            "pattern": "constant",
            "stages": [{"start_seconds": 0.0, "duration_seconds": 160.0, "target_rps": 80.0}],
        })
        self.assertEqual([(e["timestamp_seconds"], e["action"]) for e in scenario["events"]], [(40, "update"), (100, "reset")])
        self.assertEqual(scenario["events"][0]["processing_jitter"], scenario["backends"]["backend-a"]["processing_jitter"])

    def test_boundaries_and_sustained_crossing(self):
        self.assertEqual([phase_at(at, [40.01, 100.04]) for at in (40, 40.01, 100.04)], list(PHASES))
        bins = [
            {"starts": 400, "from_change_s": offset * 5, "a_share": share}
            for offset, share in enumerate((0.2, 0.8, 0.2, 0.3))
        ]
        self.assertEqual(sustained_crossing(bins, lambda share: share <= 0.3), 20)
        self.assertIsNone(sustained_crossing(bins[:-1], lambda share: share <= 0.3))

    def test_trace_counts_stay_exact_with_small_clock_inversions(self):
        backend = BACKENDS[0]
        traces = {"request": {
            "backend_selected": (60.0, {"backend_id": backend, "in_flight": [{"BackendID": b} for b in BACKENDS]}),
            "request_completed": (60.020, {"backend_id": backend, "status": 200, "duration_ns": 23_000_000}),
            "queue_admitted": (59.997, {"backend_id": backend}),
            "work_completed": (60.018, {"backend_id": backend, "duration_ns": 21_000_000, "queue_wait_ns": 0}),
        }}
        proxy = {'sbr_proxy_requests_total{status="200"}': 1}
        for b in BACKENDS:
            proxy[f'sbr_proxy_routed_requests_total{{backend="{b}"}}'] = int(b == backend)
        metrics = {b: {'sbr_backend_rejections_total{reason="queue_full"}': 0} for b in BACKENDS}
        errors, clock = verify_traces(traces, proxy, metrics)
        self.assertEqual(errors, [])
        self.assertEqual(clock["wall_clock_inversions"], 1)
        self.assertAlmostEqual(clock["max_wall_clock_inversion_ms"], 3)

        traces["request"]["queue_admitted"] = (59.980, {"backend_id": backend})
        errors, _ = verify_traces(traces, proxy, metrics)
        self.assertIn("Event clocks disagree beyond the 10 ms alignment tolerance", errors)
