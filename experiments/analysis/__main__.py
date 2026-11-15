"""Run the read-only archived campaign analysis."""

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import platform
import subprocess
import sys
import uuid

from .data import (
    PILOT_MANIFEST, PILOT_REPORT, RECOVERY_MANIFEST, RECOVERY_REPORT, RECOVERY_TABLE,
    discover, load_trial, read_jsonl, require, verify_reference_reports,
)
from .statistics import BIN_SECONDS, EVENT_WINDOW, REFERENCE_POLICY, aggregate, measure_trial, paired_effects

ROOT = Path(__file__).resolve().parents[1]


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def generate(root):
    import matplotlib
    import numpy

    from .plots import render_all
    from .report import append_index, write_json, write_statistics, write_summary

    specs = discover(root)
    rows, inputs, protocols = [], [], {}
    source_files = {name: sha256(root / name) for name in
                    (PILOT_MANIFEST, PILOT_REPORT, RECOVERY_MANIFEST, RECOVERY_TABLE, RECOVERY_REPORT)}
    for i, spec in enumerate(specs, start=1):
        try:
            trial = load_trial(root, spec)
            prior = protocols.setdefault(spec.scenario, trial.protocol)
            require(trial.protocol == prior, "Protocol settings differ within a matched scenario")
            rows.extend(measure_trial(trial))
            inputs.append({**asdict(spec), "events_seconds": trial.events, "quality": trial.quality,
                           "artifacts_sha256": {p.relative_to(root).as_posix(): sha256(p) for p in trial.artifacts}})
        except (OSError, ArithmeticError, ValueError, KeyError, IndexError, TypeError) as exc:
            raise ValueError(f"{spec.path}: {exc}") from exc
        if i % 24 == 0:
            print(f"Audited {spec.scenario} ({i}/{len(specs)} trials)", flush=True)
    references = verify_reference_reports(root, rows)
    summaries = aggregate(rows)
    effects = paired_effects(rows)
    offer = {name: sum(s["target_rps"] * s["duration_seconds"] for s in p["scenario"]["workload"]["stages"])
             / p["scenario"]["duration_seconds"] for name, p in protocols.items()}
    recovery = read_jsonl(root / RECOVERY_MANIFEST)[0]
    instrumentation_delta = max(abs(v["p95_enabled_ms"] - v["p95_disabled_ms"])
                                for v in recovery["instrumentation_control"].values())
    now = datetime.now(timezone.utc)
    repo = ROOT.parent
    try:
        revision = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        revision = "unversioned"
    sources = [*Path(__file__).parent.glob("*.py"), Path(__file__).parent / "requirements.txt",
               ROOT / "degradation_recovery.py", ROOT / "orchestrator" / "data.py", ROOT / "orchestrator" / "sync.py",
               ROOT / "orchestrator" / "config.py"]
    metadata = {
        "generated_utc": now.isoformat(), "source_git_revision": revision,
        "source_file_sha256": {p.relative_to(repo).as_posix(): sha256(p) for p in sorted(sources)},
        "platform": platform.platform(), "dependency_versions": {
            "python": platform.python_version(), "matplotlib": matplotlib.__version__, "numpy": numpy.__version__,
        },
        "parameters": {"window_seconds": BIN_SECONDS, "event_offset_range_seconds": EVENT_WINDOW,
                       "contrast_reference": REFERENCE_POLICY, "throughput_denominator": "scheduled window seconds",
                       "success_status": 200, "confidence_level": .95, "nominal_requests_per_second": offer},
        "protocols": protocols, "source_archive_sha256": source_files, "trials": inputs, "reference_counts": references,
        "instrumentation_control_max_p95_delta_ms": instrumentation_delta,
        "generation_command": "python -B -m experiments.analysis --results-root <archive>",
    }
    base = root / "analysis"
    output = base / "matched-campaigns" / f"{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    output.mkdir(parents=True, exist_ok=False)
    figures = output / "figures"
    figures.mkdir()
    write_statistics(output / "statistics.csv", rows, summaries)
    write_json(output / "comparisons.json", {"aggregates": summaries, "matched_effects": effects})
    write_json(output / "analysis-metadata.json", metadata)
    render_all(summaries, effects, offer, figures)
    write_summary(output, root, rows, summaries, effects, metadata)
    append_index(base / "INDEX.md", output, now.isoformat(), len(specs))
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-root", type=Path, default=ROOT / "results",
                        help="Directory containing the existing campaign manifests and trial artifacts")
    args = parser.parse_args()
    try:
        print(generate(args.results_root.resolve()))
    except ModuleNotFoundError as exc:
        print(f"{exc}. Install experiments/analysis/requirements.txt in a virtual environment.", file=sys.stderr)
        return 1
    except (OSError, ArithmeticError, ValueError, KeyError, IndexError, TypeError, subprocess.CalledProcessError) as exc:
        print(f"Analysis failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
