"""
Persistent experiment history index and research journal management.
Maintains automatically updated index.json, INDEX.md, and JOURNAL.md
under experiments/results/ for IDE accessibility and auditability.
"""

import datetime
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("sbr.orchestrator.history")

INDEX_JSON_NAME = "index.json"
INDEX_MD_NAME = "INDEX.md"
JOURNAL_MD_NAME = "JOURNAL.md"


class ExperimentHistory:
    """
    Manages the global experiment index and Markdown research journal across all trials.
    Never deletes or overwrites existing experiment records.
    """

    def __init__(self, results_root: Path):
        self.results_root = results_root
        self.index_json_path = results_root / INDEX_JSON_NAME
        self.index_md_path = results_root / INDEX_MD_NAME
        self.journal_md_path = results_root / JOURNAL_MD_NAME

    def _ensure_results_dir(self) -> None:
        self.results_root.mkdir(parents=True, exist_ok=True)

    def load_index(self) -> Dict[str, Any]:
        """Loads index.json or returns an empty catalog structure."""
        if self.index_json_path.is_file():
            try:
                with open(self.index_json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict) and "trials" in data:
                        return data
            except Exception as e:
                logger.warning("Failed to parse %s: %s; re-initializing index.", self.index_json_path, e)

        return {
            "version": "1.0",
            "last_updated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "total_trials": 0,
            "valid_trials": 0,
            "invalid_trials": 0,
            "trials": [],
        }

    def save_index(self, index_data: Dict[str, Any]) -> None:
        """Persists index.json and re-renders INDEX.md."""
        self._ensure_results_dir()
        index_data["last_updated"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        trials = index_data.get("trials", [])
        index_data["total_trials"] = len(trials)
        index_data["valid_trials"] = sum(1 for t in trials if t.get("valid") is True)
        index_data["invalid_trials"] = len(trials) - index_data["valid_trials"]

        # Write index.json
        with open(self.index_json_path, "w", encoding="utf-8") as f:
            json.dump(index_data, f, indent=2)

        # Render INDEX.md
        md_content = self.render_index_markdown(index_data)
        with open(self.index_md_path, "w", encoding="utf-8") as f:
            f.write(md_content)

    def render_index_markdown(self, index_data: Dict[str, Any]) -> str:
        """Renders an IDE-accessible Markdown table of all trials."""
        total = index_data.get("total_trials", 0)
        valid_cnt = index_data.get("valid_trials", 0)
        invalid_cnt = index_data.get("invalid_trials", 0)
        updated = index_data.get("last_updated", "N/A")

        lines = [
            "# Experiment History Index",
            "",
            "Automatically maintained catalog of all benchmark trials and evaluation rounds.",
            "",
            f"- **Total Trials:** {total}",
            f"- **Valid Trials:** {valid_cnt} (`PASS`)",
            f"- **Invalid / Failed Trials:** {invalid_cnt} (`FAIL`)",
            f"- **Last Updated:** `{updated}`",
            "",
            "| Date / Time (UTC) | Scenario | Policy | Run | Status | Reqs | Success % | Mean (ms) | P99 (ms) | Directory | Notes / Reason |",
            "|---|---|---|---|:---:|---:|---:|---:|---:|---|---|",
        ]

        trials = index_data.get("trials", [])
        # Display latest first
        for t in reversed(trials):
            ts = t.get("timestamp", "")
            ts_display = ts[:19].replace("T", " ") if len(ts) >= 19 else ts
            sc_id = t.get("scenario_id", "")
            policy = t.get("routing_policy", "")
            run_name = t.get("run_name", "")
            is_valid = t.get("valid", False)
            status_badge = "**PASS**" if is_valid else "*FAIL*"
            reqs = t.get("total_requests", 0)
            succ_pct = f"{t.get('success_rate', 0.0) * 100:.1f}%" if reqs > 0 else "0.0%"

            mean_lat = t.get("mean_latency_ms")
            mean_str = f"{mean_lat:.2f}" if mean_lat is not None else "-"

            p99_lat = t.get("p99_latency_ms")
            p99_str = f"{p99_lat:.2f}" if p99_lat is not None else "-"

            rel_dir = t.get("relative_dir", "").replace("\\", "/")
            dir_link = f"[{run_name}]({rel_dir}/)" if rel_dir else run_name

            note_or_err = t.get("note") or t.get("failure_reason") or "-"
            # Truncate for table display
            if len(note_or_err) > 60:
                note_or_err = note_or_err[:57] + "..."
            note_or_err = note_or_err.replace("|", "/")

            lines.append(
                f"| {ts_display} | {sc_id} | {policy} | {run_name} | {status_badge} | {reqs} | {succ_pct} | {mean_str} | {p99_str} | {dir_link} | {note_or_err} |"
            )

        lines.append("")
        return "\n".join(lines)

    def record_trial(
        self,
        metadata: Dict[str, Any],
        run_dir: Path,
        metrics_data: Optional[Dict[str, Any]] = None,
        note: Optional[str] = None,
        hypothesis: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Appends or updates a trial record in index.json and INDEX.md.
        Never removes previously recorded trials.
        """
        try:
            rel_dir = run_dir.resolve().relative_to(self.results_root.resolve()).as_posix()
        except ValueError:
            rel_dir = run_dir.name

        trial_id = f"{metadata.get('scenario_id')}/{metadata.get('routing_policy')}/{run_dir.name}"

        # Extract summary metrics if available
        total_reqs = 0
        succ_reqs = 0
        succ_rate = 0.0
        mean_lat = None
        p50_lat = None
        p99_lat = None

        if metrics_data and "outcomes" in metrics_data:
            outcomes = metrics_data["outcomes"]
            total_reqs = outcomes.get("total_requests", 0)
            succ_reqs = outcomes.get("successful_requests", 0)
            succ_rate = outcomes.get("success_rate", 0.0)

            lat = metrics_data.get("latency_ms", {}).get("successful_requests", {})
            if lat and lat.get("count", 0) > 0:
                mean_lat = lat.get("mean")
                p50_lat = lat.get("p50")
                p99_lat = lat.get("p99")

        entry: Dict[str, Any] = {
            "trial_id": trial_id,
            "timestamp": metadata.get("start_time") or datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "scenario_id": metadata.get("scenario_id", ""),
            "scenario_name": metadata.get("scenario_name", ""),
            "routing_policy": metadata.get("routing_policy", ""),
            "trial_index": metadata.get("trial_index", 1),
            "run_name": run_dir.name,
            "random_seed": metadata.get("random_seed"),
            "git_commit": metadata.get("git_commit"),
            "valid": metadata.get("valid", False),
            "failure_reason": metadata.get("failure_reason"),
            "duration_seconds": metadata.get("duration_seconds", 0.0),
            "total_requests": total_reqs,
            "successful_requests": succ_reqs,
            "success_rate": round(succ_rate, 4),
            "mean_latency_ms": round(mean_lat, 2) if mean_lat is not None else None,
            "p50_latency_ms": round(p50_lat, 2) if p50_lat is not None else None,
            "p99_latency_ms": round(p99_lat, 2) if p99_lat is not None else None,
            "relative_dir": rel_dir,
            "note": note or metadata.get("note"),
            "hypothesis": hypothesis or metadata.get("hypothesis"),
        }

        index_data = self.load_index()
        trials = index_data.get("trials", [])

        # Idempotently update if matching trial_id exists; otherwise append
        updated = False
        for i, existing in enumerate(trials):
            if existing.get("trial_id") == trial_id or existing.get("relative_dir") == rel_dir:
                trials[i] = entry
                updated = True
                break

        if not updated:
            trials.append(entry)

        index_data["trials"] = trials
        self.save_index(index_data)

        # Update Markdown research journal
        self.append_journal_entry(entry, metadata)

        return entry

    def ensure_journal_initialized(self) -> None:
        """Initializes JOURNAL.md with research structure if not already present."""
        if not self.journal_md_path.is_file():
            self._ensure_results_dir()
            header = [
                "# Experimentation Research Journal",
                "",
                "This journal documents research hypotheses, configuration changes, empirical observations,",
                "and comparative conclusions across benchmark evaluations in `self-balancing-router`.",
                "",
                "## Research Objectives",
                "- Evaluate adaptive HTTP load-balancing policies against heterogeneous, non-stationary backend performance.",
                "- Measure empirical regret, queue-overflow avoidance, and latency adaptation across stationary and degraded stages.",
                "- Compare traditional heuristics (`round-robin`, `least-requests`, `p2c`, `p2c-peak-ewma`) against Multi-Armed Bandit policies.",
                "",
                "## Research Journal Entry Template",
                "Copy this template to record exploratory investigations, manual analyses, or multi-trial campaign findings:",
                "",
                "```markdown",
                "### [YYYY-MM-DD HH:MM] <Investigation Title>",
                "- **Evaluator:** <Researcher Name / Session>",
                "- **Git Commit:** `<sha>`",
                "- **Scenario:** `<scenario_id>`",
                "- **Policies Evaluated:** `[round-robin, least-requests, p2c, p2c-peak-ewma, ucb1, sliding-window-ucb, discounted-ucb]`",
                "- **Hypothesis:**",
                "  - Expected system behavior and theoretical justification.",
                "- **Parameter Configurations / Changes:**",
                "  - Workload parameters: `--seed <S> --vus-preallocated <N>`",
                "  - Policy parameters: `--mab-latency-scale <scale> --mab-sw-window <W> --mab-discount-factor <gamma>`",
                "- **Empirical Observations:**",
                "  - Latency trends, arm selection distributions, and queue overload events.",
                "- **Key Findings & Conclusions:**",
                "  - Evaluation against hypothesis and actionable conclusions.",
                "```",
                "",
                "## How to Record Manual Journal Entries",
                "When executing benchmarks, use `--hypothesis \"...\"` and `--note \"...\"` or append markdown entries below.",
                "",
                "---",
                "",
                "## Logged Evaluation Sessions",
                "",
            ]
            with open(self.journal_md_path, "w", encoding="utf-8") as f:
                f.write("\n".join(header))

    def append_journal_entry(self, entry: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Appends a structured markdown record to JOURNAL.md."""
        self.ensure_journal_initialized()
        ts = entry.get("timestamp", "")
        sc_id = entry.get("scenario_id", "")
        policy = entry.get("routing_policy", "")
        run_name = entry.get("run_name", "")
        rel_dir = entry.get("relative_dir", "").replace("\\", "/")
        is_valid = entry.get("valid", False)
        status_tag = "VALID (PASS)" if is_valid else f"INVALID (FAIL: {entry.get('failure_reason')})"

        lines = [
            f"### [{ts[:19].replace('T', ' ')}] `{sc_id}` | Policy: `{policy}` | `{run_name}`",
            f"- **Result Directory:** [`{rel_dir}`]({rel_dir}/)",
            f"- **Status:** {status_tag}",
            f"- **Git Commit:** `{metadata.get('git_commit', 'unknown')}` | **Base Seed:** `{metadata.get('random_seed')}`",
        ]

        if entry.get("hypothesis"):
            lines.append(f"- **Hypothesis:** {entry['hypothesis']}")

        pol_cfg = metadata.get("configuration", {}).get("policy_configuration")
        if pol_cfg:
            cfg_str = ", ".join(f"`{k}={v}`" for k, v in sorted(pol_cfg.items()))
            lines.append(f"- **Policy Configuration:** {cfg_str}")

        if entry.get("note"):
            lines.append(f"- **Notes / Observations:** {entry['note']}")

        reqs = entry.get("total_requests", 0)
        succ = entry.get("successful_requests", 0)
        mean_l = entry.get("mean_latency_ms")
        p99_l = entry.get("p99_latency_ms")

        mean_str = f"{mean_l:.2f}ms" if mean_l is not None else "N/A"
        p99_str = f"{p99_l:.2f}ms" if p99_l is not None else "N/A"

        lines.append(
            f"- **Summary:** {reqs} total requests, {succ} successful ({entry.get('success_rate', 0.0)*100:.1f}%), Mean Latency: {mean_str}, P99: {p99_str}"
        )
        lines.append("")

        with open(self.journal_md_path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    def scan_and_rebuild(self) -> int:
        """
        Discovers all existing trial directories under results_root, reads metadata.json and
        metrics.json, and completely rebuilds index.json and INDEX.md.
        """
        self._ensure_results_dir()
        discovered: List[Dict[str, Any]] = []

        # Find all metadata.json files in subdirectories
        for meta_file in sorted(self.results_root.glob("*/*/*/metadata.json")):
            run_dir = meta_file.parent
            try:
                with open(meta_file, "r", encoding="utf-8") as f:
                    meta = json.load(f)

                metrics_data = None
                metrics_file = run_dir / "metrics.json"
                if metrics_file.is_file():
                    try:
                        with open(metrics_file, "r", encoding="utf-8") as mf:
                            metrics_data = json.load(mf)
                    except Exception:
                        pass

                try:
                    rel_dir = run_dir.resolve().relative_to(self.results_root.resolve()).as_posix()
                except ValueError:
                    rel_dir = run_dir.name

                trial_id = f"{meta.get('scenario_id')}/{meta.get('routing_policy')}/{run_dir.name}"

                total_reqs = 0
                succ_reqs = 0
                succ_rate = 0.0
                mean_lat = None
                p50_lat = None
                p99_lat = None

                if metrics_data and "outcomes" in metrics_data:
                    outcomes = metrics_data["outcomes"]
                    total_reqs = outcomes.get("total_requests", 0)
                    succ_reqs = outcomes.get("successful_requests", 0)
                    succ_rate = outcomes.get("success_rate", 0.0)

                    lat = metrics_data.get("latency_ms", {}).get("successful_requests", {})
                    if lat and lat.get("count", 0) > 0:
                        mean_lat = lat.get("mean")
                        p50_lat = lat.get("p50")
                        p99_lat = lat.get("p99")

                entry: Dict[str, Any] = {
                    "trial_id": trial_id,
                    "timestamp": meta.get("start_time") or datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "scenario_id": meta.get("scenario_id", ""),
                    "scenario_name": meta.get("scenario_name", ""),
                    "routing_policy": meta.get("routing_policy", ""),
                    "trial_index": meta.get("trial_index", 1),
                    "run_name": run_dir.name,
                    "random_seed": meta.get("random_seed"),
                    "git_commit": meta.get("git_commit"),
                    "valid": meta.get("valid", False),
                    "failure_reason": meta.get("failure_reason"),
                    "duration_seconds": meta.get("duration_seconds", 0.0),
                    "total_requests": total_reqs,
                    "successful_requests": succ_reqs,
                    "success_rate": round(succ_rate, 4),
                    "mean_latency_ms": round(mean_lat, 2) if mean_lat is not None else None,
                    "p50_latency_ms": round(p50_lat, 2) if p50_lat is not None else None,
                    "p99_latency_ms": round(p99_lat, 2) if p99_lat is not None else None,
                    "relative_dir": rel_dir,
                    "note": meta.get("note"),
                    "hypothesis": meta.get("hypothesis"),
                }
                discovered.append(entry)
            except Exception as e:
                logger.warning("Error inspecting trial %s: %s", run_dir, e)

        # Sort chronologically by timestamp
        discovered.sort(key=lambda x: x.get("timestamp", ""))

        index_data = {
            "version": "1.0",
            "last_updated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "total_trials": len(discovered),
            "valid_trials": sum(1 for d in discovered if d.get("valid") is True),
            "invalid_trials": len(discovered) - sum(1 for d in discovered if d.get("valid") is True),
            "trials": discovered,
        }

        self.save_index(index_data)
        logger.info("Rebuilt experiment index across %d discovered trials.", len(discovered))
        return len(discovered)
