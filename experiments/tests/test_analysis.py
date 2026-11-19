from importlib.util import find_spec
import gzip
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from experiments.analysis.data import (
    PILOT_MANIFEST, POLICIES, RECOVERY, RECOVERY_MANIFEST, SCENARIOS, STABLE, SUDDEN,
    Request, Trial, TrialSpec, classify, discover, load_trial, raw_errors, recovery_results, validate_summaries,
)
from experiments.analysis.statistics import aggregate, measure_trial, median_interval, paired_effects


def write_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")


class AnalysisDataTest(unittest.TestCase):
    def test_selection_uses_completed_manifests_and_skips_failed_attempts_and_controls(self):
        self.assertNotIn("sliding-window-ucb-pending", POLICIES)
        with TemporaryDirectory() as temp:
            root = Path(temp)
            schedule = [(block, scenario, policy) for block in (1, 2, 3) for scenario in (STABLE, SUDDEN) for policy in POLICIES]
            pilot = [{"kind": "plan", "base_seed": 40, "git_commit": "pilot", "schedule": schedule,
                      "original_invalid": f"{SUDDEN}/round-robin/run-031"}]
            for block, scenario, policy in schedule:
                pilot.append({"kind": "trial", "scenario": scenario, "policy": policy, "rep": block,
                              "valid": True, "requests": 6200, "path": f"{scenario}/{policy}/run-{block:03}"})
            pilot += [{"kind": "trial", "valid": False, "path": f"{SUDDEN}/round-robin/run-031"}, {"kind": "complete"}]
            write_jsonl(root / PILOT_MANIFEST, pilot)
            control = f"{RECOVERY}/round-robin/run-031"
            (root / control).mkdir(parents=True)
            recovery = [{"kind": "protocol", "git_commit": "recovery", "seeds": [60, 61, 62], "controls": [{"path": control}]}]
            for block in (1, 2, 3):
                for pos, policy in enumerate(POLICIES, 1):
                    recovery.append({"kind": "result", "policy": policy, "rep": block, "position": pos,
                                     "seed": 59 + block, "project": "isolated", "valid": True,
                                     "path": f"{RECOVERY}/{policy}/run-{block:03}", "audit": {"requests": 12800}})
            original = recovery[1]
            original["valid"] = False
            recovery.extend([{**original, "kind": "revalidation", "valid": True}, {"kind": "complete"}])
            write_jsonl(root / RECOVERY_MANIFEST, recovery)
            specs = discover(root)
            self.assertEqual(len(specs), 72)
            self.assertIn(original["path"], {s.path for s in specs})
            self.assertNotIn(control, {s.path for s in specs})
            self.assertNotIn(pilot[0]["original_invalid"], {s.path for s in specs})
            write_jsonl(root / RECOVERY_MANIFEST, recovery[:-2] + recovery[-1:])
            with self.assertRaisesRegex(ValueError, "Incomplete"):
                discover(root)

    def test_revalidation_cannot_substitute_a_trial(self):
        original = dict(kind="result", rep=1, position=1, policy="p2c", seed=60, project="run", path="original")
        with self.assertRaisesRegex(ValueError, "identity"):
            recovery_results([original, {**original, "kind": "revalidation", "path": "other"}])

    def test_missing_malformed_and_inconsistent_artifacts(self):
        spec = TrialSpec(STABLE, "p2c", 1, 40, f"{STABLE}/p2c/run-001", "abc", 1)
        with TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaisesRegex(ValueError, "Missing trial artifact"):
                load_trial(root, spec)
            path = root / spec.path
            path.mkdir(parents=True)
            for name in ("metadata.json", "scenario.json", "metrics.json", "k6-summary.json", "events.json",
                         "k6-output.log", "requests.csv", "raw-metrics.json.gz", "compose.override.yaml"):
                (path / name).write_text("{", encoding="utf-8")
            with self.assertRaises(json.JSONDecodeError):
                load_trial(root, spec)
        with self.assertRaisesRegex(ValueError, "request counts"):
            validate_summaries([Request("200", "success", "backend-a", 1.0, 2.0)],
                               {"outcomes": {"status_distribution": {"503": 1}}}, {}, spec, 1)

    def test_raw_timeout_tag_and_csv_reconciliation(self):
        fields = {"backend_id": "", "overload_origin": ""}
        row = dict(timestamp_iso="2026-01-01T00:00:05Z", status_code="0", duration_ms="0.0", **fields)
        point = {"type": "Point", "metric": "sbr_request_observation", "data": {
            "time": row["timestamp_iso"], "value": 0, "tags": {**fields, "status": "0", "error": "request timeout", "phase": "warmup"},
        }}
        with TemporaryDirectory() as temp:
            path = Path(temp) / "raw-metrics.json.gz"
            with gzip.open(path, "wt", encoding="utf-8") as stream:
                stream.write(json.dumps(point) + "\n")
            self.assertEqual(raw_errors(path, [row]), {("timeout", "warmup"): 1})
            with self.assertRaisesRegex(ValueError, "differ"):
                raw_errors(path, [{**row, "duration_ms": "1"}])

    def test_analysis_package_importable(self):
        import experiments.analysis
        self.assertIsNotNone(experiments.analysis.__doc__)


class AnalysisStatisticsTest(unittest.TestCase):
    def test_success_percentiles_and_boundaries_keep_untimed_failures_in_full_trial(self):
        spec = TrialSpec(SUDDEN, "p2c", 1, 40, "trial", "abc", 4)
        reqs = [
            Request("200", "success", "backend-a", 1000, 10.5),   # starts before the event
            Request("200", "success", "backend-b", 3000, 13),    # starts on the boundary
            Request("0", classify("0", ""), "", 0, 12),          # cannot recover dispatch
            Request("503", classify("503", "backend"), "", 1, 14),
        ]
        trial = Trial(spec, reqs, 45, {"degradation": 10}, {}, 0, {}, {}, [])
        rows = {(r["scope"], r["offset_seconds"]): r["metrics"] for r in measure_trial(trial)}
        full = rows["whole", None]
        self.assertEqual(full["p50_ms"], 2000)
        self.assertEqual(full["p95_ms"], 2900)
        self.assertEqual(full["p99_ms"], 2980)
        self.assertEqual(full["success_pct"], 50)
        self.assertEqual(full["unassigned_dispatches"], 1)
        self.assertEqual(full["boundary_spans"], 1)
        self.assertEqual(rows["normal", None]["requests"], 1)
        self.assertEqual(rows["degraded", None]["requests"], 2)
        self.assertEqual(rows["event_degradation", 0]["backend_503_count"], 1)
        self.assertEqual(rows["event_degradation", -5]["successful"], 1)
        self.assertNotIn("backend-a_assignment_pct", full)

    def test_matched_effects_use_repetitions_and_exclude_unsupported_intervals(self):
        rows = [
            {"scenario": STABLE, "policy": policy, "scope": "whole", "offset_seconds": None,
             "block": block, "seed": block + 39, "metrics": {"p99_ms": duration}}
            for policy, measurements in (("p2c", [5, 20, 30]), ("round-robin", [10, 21, 34]))
            for block, duration in enumerate(measurements, 1)
        ]
        effect, = paired_effects(rows)
        self.assertEqual((effect["median"], effect["min"], effect["max"]), (-4, -5, -1))
        self.assertEqual([p["seed"] for p in effect["pairs"]], [40, 41, 42])
        self.assertIsNone(effect["median_ci95"])
        self.assertEqual(median_interval([1, 2, 3, 4, 5, 6]), {"lower": 1, "upper": 6, "coverage": .96875})
        self.assertEqual(aggregate(rows)[0]["metrics"]["p99_ms"]["n"], 3)
        with self.assertRaisesRegex(ValueError, "Unmatched"):
            paired_effects(rows[:-1])


@unittest.skipUnless(find_spec("matplotlib"), "install analysis plotting requirements")
class AnalysisPlotTest(unittest.TestCase):
    def test_svg_and_png_are_reproducible(self):
        from experiments.analysis.plots import matched_variability, matplotlib, save_figure

        effect = dict(scenario=SCENARIOS[0], policy="p2c", scope="whole", metric="p99_ms", min=-4, max=-2, median=-3,
                      pairs=[{"delta": -4}, {"delta": -2}, {"delta": -3}])
        with TemporaryDirectory() as temp, matplotlib.rc_context({"svg.hashsalt": "deterministic-analysis"}):
            base = Path(temp)
            for name in ("a", "b"):
                save_figure(matched_variability([effect]), base / name)
            for suffix, signature in (("svg", b"<?xml"), ("png", b"\x89PNG")):
                first = (base / f"a.{suffix}").read_bytes()
                self.assertTrue(first.startswith(signature))
                self.assertEqual(first, (base / f"b.{suffix}").read_bytes())
