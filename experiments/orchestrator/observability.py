"""Collect optional snapshots and diagnostic records from an isolated trial."""

from collections import Counter
import datetime
import json
import subprocess
import tempfile
import urllib.request

SCHEMA_VERSION = 1
METRICS_PORT = 9464
MAX_METRICS_BYTES = 4 * 1024 * 1024


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def compose_observability(backends, every):
    lines = []
    for name in ("proxy", *backends):
        if name != "proxy":
            lines.extend([f"  {name}:", "    environment:"])
        lines.extend([
            f'      SBR_METRICS_ADDR: ":{METRICS_PORT}"',
            f'      SBR_DIAGNOSTIC_SAMPLE_EVERY: "{every}"',
            "    ports:",
            f'      - "127.0.0.1::{METRICS_PORT}"',
        ])
    return lines


def collect_metrics(docker, run_dir, services, phase, metadata):
    for name in services:
        try:
            with tempfile.TemporaryFile() as output:
                docker.capture(["port", name, str(METRICS_PORT)], output)
                output.seek(0)
                host, port = output.read(256).decode("ascii").strip().rsplit(":", 1)
            if host != "127.0.0.1" or not port.isdecimal():
                raise ValueError("unexpected metrics address")
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(f"http://127.0.0.1:{port}/metrics", timeout=3) as response:
                data = response.read(MAX_METRICS_BYTES + 1)
            prefix = f"# TYPE sbr_{'proxy' if name == 'proxy' else 'backend'}_".encode()
            if len(data) > MAX_METRICS_BYTES or not data.startswith(prefix):
                raise ValueError("unexpected metrics response")
            artifact = f"observability-{phase}-{name}.prom"
            with (run_dir / artifact).open("xb") as output:
                output.write(data)
            metadata["snapshots"].append({"service": name, "phase": phase, "collected_at": utc_now(), "artifact": artifact})
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            metadata["errors"].append(f"{phase} metrics for {name}: {type(exc).__name__}")


def export_events(logs, output, services, every):
    counts = Counter()
    summaries = {}
    errors = []
    for line in logs:
        if b"sbr diagnostic" not in line:
            continue
        try:
            record = json.loads(line)
            if record.get("msg") not in {"sbr diagnostic event", "sbr diagnostic summary"}:
                continue
            service = record["diagnostic_service"]
            if service not in services or record["diagnostic_schema"] != SCHEMA_VERSION:
                raise ValueError("unknown diagnostic source or schema")
            if not isinstance(record.get("time"), str) or not record["time"]:
                raise ValueError("missing diagnostic timestamp")
            if record["msg"] == "sbr diagnostic event":
                event = record["event"]
                if not event.get("request_id") or not event.get("kind"):
                    raise ValueError("incomplete diagnostic event")
                counts[service] += 1
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
            elif record["msg"] == "sbr diagnostic summary":
                if service in summaries or record["sample_every"] != every:
                    raise ValueError("unexpected diagnostic summary")
                for field in ("written", "dropped"):
                    if type(record[field]) is not int or record[field] < 0:
                        raise ValueError("invalid diagnostic counter")
                summaries[service] = {key: record[key] for key in ("written", "dropped", "time")}
        except (KeyError, TypeError, ValueError, AttributeError):
            if "malformed diagnostic record" not in errors:
                errors.append("malformed diagnostic record")
    for service in services:
        summary = summaries.get(service)
        if summary is None:
            errors.append(f"missing diagnostic summary for {service}")
        elif summary["written"] != counts[service] or summary["dropped"]:
            errors.append(f"missing or dropped events for {service}")
    return dict(counts), summaries, errors


def collect_events(docker, run_dir, services, every, metadata):
    try:
        # Stopped containers retain stdout. Capture after the bounded Go drain.
        docker.capture(["stop"], subprocess.DEVNULL)
    except (OSError, subprocess.SubprocessError) as exc:
        metadata["errors"].append(f"diagnostic shutdown: {type(exc).__name__}")
    try:
        with tempfile.TemporaryFile() as logs:
            docker.capture(["logs", "--no-color", "--no-log-prefix", *services], logs)
            logs.seek(0)
            artifact = "diagnostic-events.jsonl"
            with (run_dir / artifact).open("x", encoding="utf-8", newline="\n") as output:
                counts, summaries, errors = export_events(logs, output, services, every)
        metadata.update(event_artifact=artifact, exported_events=counts, event_summaries=summaries,
                        dropped_events=sum(s["dropped"] for s in summaries.values()) if len(summaries) == len(services) else None)
        metadata["errors"].extend(errors)
    except (OSError, subprocess.SubprocessError) as exc:
        metadata["errors"].append(f"diagnostic export: {type(exc).__name__}")
