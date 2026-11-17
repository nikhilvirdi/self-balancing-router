import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock

from experiments.orchestrator.observability import collect_events, collect_metrics, compose_observability, export_events


def record(message, service, **fields):
    return json.dumps(dict(msg=f"sbr diagnostic {message}", diagnostic_schema=1,
                           diagnostic_service=service, time="2026-01-01T00:00:00Z", **fields)).encode() + b"\n"


class ObservabilityExportTest(unittest.TestCase):
    def test_bandit_trace_compose_option_is_opt_in_and_proxy_only(self):
        base = compose_observability(["backend-a"], 1)
        detailed = compose_observability(["backend-a"], 1, bandit_trace=True)
        self.assertEqual(detailed[1:], base)
        self.assertEqual(detailed[0], '      SBR_PROXY_BANDIT_TRACE: "true"')

    def test_complete_export_preserves_request_ids_and_filters_application_logs(self):
        request_id = "test-request"
        logs = io.BytesIO(
            b'{"msg":"normal application message","data":"not exported"}\n'
            + record("event", "proxy", event={"kind": "backend_selected", "request_id": request_id})
            + record("event", "backend-a", event={"kind": "queue_admitted", "request_id": request_id})
            + record("summary", "proxy", written=1, dropped=0, sample_every=10)
            + record("summary", "backend-a", written=1, dropped=0, sample_every=10)
        )
        output = io.StringIO()
        counts, summaries, errors = export_events(logs, output, ["proxy", "backend-a"], 10)
        self.assertEqual(errors, [])
        self.assertEqual(counts, {"proxy": 1, "backend-a": 1})
        self.assertEqual(summaries.keys(), counts.keys())
        self.assertEqual([json.loads(line)["event"]["request_id"] for line in output.getvalue().splitlines()], [request_id] * 2)
        self.assertNotIn("not exported", output.getvalue())

    def test_reports_dropped_malformed_and_missing_records(self):
        logs = io.BytesIO(b'{"msg":"sbr diagnostic event"\n' + record("summary", "proxy", written=2, dropped=1, sample_every=1))
        counts, summaries, errors = export_events(logs, io.StringIO(), ["proxy", "backend-a"], 1)
        self.assertEqual(counts, {})
        self.assertEqual(summaries["proxy"]["dropped"], 1)
        self.assertCountEqual(errors, ["malformed diagnostic record", "missing or dropped events for proxy", "missing diagnostic summary for backend-a"])

    def test_metrics_failure_and_existing_artifact_are_preserved(self):
        docker = Mock()

        def capture(args, output):
            if args == ["stop"]:
                raise subprocess.TimeoutExpired("compose stop", 40)
            if args[0] == "logs":
                output.write(record("summary", "proxy", written=0, dropped=0, sample_every=1))
            else:
                output.write(b"0.0.0.0:9464\n")

        docker.capture.side_effect = capture
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp)
            existing = run_dir / "diagnostic-events.jsonl"
            existing.write_text("existing\n", encoding="utf-8")
            metadata = {"snapshots": [], "errors": []}
            collect_metrics(docker, run_dir, ["proxy"], "before", metadata)
            collect_events(docker, run_dir, ["proxy"], 1, metadata)
            self.assertEqual(metadata["snapshots"], [])
            self.assertEqual(existing.read_text(encoding="utf-8"), "existing\n")
            self.assertEqual(metadata["errors"], ["before metrics for proxy: ValueError",
                                                  "diagnostic shutdown: TimeoutExpired",
                                                  "diagnostic export: FileExistsError"])
