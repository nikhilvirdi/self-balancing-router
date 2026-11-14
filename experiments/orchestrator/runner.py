"""
Experiment trial runner and metadata persister.
"""

from dataclasses import asdict, dataclass
import datetime
import json
import logging
import os
from pathlib import Path
import platform
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional

from .admin import AdminClient, EventExecutionRecord
from .config import ExperimentConfig, resolve_repo_root
from .data import DataCollector, DataValidator, IntervalTimeline
from .docker import DockerLifecycle
from .k6 import K6ExecutionResult, K6Runner
from .sync import SyncCoordinator

logger = logging.getLogger("sbr.orchestrator.runner")


def get_git_commit(repo_root: Path) -> Optional[str]:
    """Retrieves current git commit hash if git is available."""
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
            timeout=2.0,
        )
        if res.returncode == 0:
            return res.stdout.strip()
    except Exception:
        pass
    return None


def get_docker_version() -> Optional[str]:
    """Retrieves Docker version string."""
    try:
        res = subprocess.run(
            ["docker", "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
            timeout=2.0,
        )
        if res.returncode == 0:
            return res.stdout.strip()
    except Exception:
        pass
    return None


def resolve_run_dir(base_dir: Path, scenario_id: str, policy: str) -> Path:
    """Finds the next sequential non-existing run directory: run-001, run-002, etc."""
    target_parent = base_dir / scenario_id / policy
    target_parent.mkdir(parents=True, exist_ok=True)

    idx = 1
    while True:
        run_name = f"run-{idx:03d}"
        candidate = target_parent / run_name
        if not candidate.exists():
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate
        idx += 1


@dataclass
class TrialResult:
    """Complete summary of a single experimental repetition."""

    trial_index: int
    scenario_id: str
    policy: str
    run_dir: Path
    valid: bool
    failure_reason: Optional[str]
    metadata: Dict[str, Any]
    events: List[EventExecutionRecord]
    k6_result: Optional[K6ExecutionResult]


class ExperimentRunner:
    """Coordinates and executes isolated, reproducible experiment trials."""

    def __init__(self, config: ExperimentConfig):
        self.config = config
        self.repo_root = resolve_repo_root()
        self.k6_runner = K6Runner(repo_root=self.repo_root, exec_mode=config.k6_exec)

    def run_trial(self, trial_index: int) -> TrialResult:
        """Executes a single end-to-end experimental trial."""
        scenario = self.config.scenario
        scenario_id = scenario.get("id", "unknown_scenario")
        policy = self.config.routing_policy
        trial_seed = self.config.seed + trial_index - 1

        run_dir = resolve_run_dir(self.config.output_dir, scenario_id, policy)
        logger.info(
            "=== Starting Trial %d/%d for '%s' [%s] in %s ===",
            trial_index,
            self.config.repetitions,
            scenario_id,
            policy,
            run_dir,
        )

        trial_start_wall = time.time()
        start_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        git_sha = get_git_commit(self.repo_root)
        docker_ver = get_docker_version()
        k6_method = self.k6_runner.resolve_execution_method()

        metadata: Dict[str, Any] = {
            "scenario_id": scenario_id,
            "scenario_name": scenario.get("name", ""),
            "routing_policy": policy,
            "trial_index": trial_index,
            "random_seed": trial_seed,
            "git_commit": git_sha,
            "versions": {
                "python": platform.python_version(),
                "docker": docker_ver,
                "k6_execution_method": k6_method,
            },
            "start_time": start_iso,
            "valid": False,
            "failure_reason": None,
            "scenario_definition": scenario,
            "configuration": {
                "proxy_port": self.config.proxy_port,
                "admin_ports": self.config.admin_ports,
                "compose_project": self.config.compose_project,
                "http_timeout": self.config.http_timeout,
                "readiness_timeout": self.config.readiness_timeout,
            },
        }

        # Dry run short-circuit
        if self.config.dry_run:
            logger.info("Dry run mode: validating without starting infrastructure.")
            metadata["valid"] = True
            metadata["dry_run"] = True
            metadata["end_time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            metadata["duration_seconds"] = 0.0

            trial_scenario_file = run_dir / "scenario.json"
            with open(trial_scenario_file, "w", encoding="utf-8") as f:
                json.dump(scenario, f, indent=2)

            events_file = run_dir / "events.json"
            with open(events_file, "w", encoding="utf-8") as f:
                json.dump([], f, indent=2)

            with open(run_dir / "k6-summary.json", "w", encoding="utf-8") as f:
                json.dump({
                    "metadata": {"scenario_id": scenario_id, "routing_policy": policy},
                    "workload_integrity": {"total_requests": 0, "dropped_iterations": 0},
                    "outcomes": {"successful_200": 0},
                }, f, indent=2)

            with open(run_dir / "k6-output.log", "w", encoding="utf-8") as f:
                f.write("[DRY RUN] No k6 output generated\n")

            timeline = IntervalTimeline(events=[], scenario=scenario)
            DataCollector.collect_and_export(
                raw_metrics_path=run_dir / "raw-metrics.json",
                output_dir=run_dir,
                workload_start_wall=trial_start_wall,
                timeline=timeline,
                scenario=scenario,
                policy=policy,
                trial_index=trial_index,
                dropped_iterations=0,
                dry_run=True,
            )

            meta_file = run_dir / "metadata.json"
            with open(meta_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)

            return TrialResult(
                trial_index=trial_index,
                scenario_id=scenario_id,
                policy=policy,
                run_dir=run_dir,
                valid=True,
                failure_reason=None,
                metadata=metadata,
                events=[],
                k6_result=None,
            )

        admin_client = AdminClient(
            admin_ports=self.config.admin_ports,
            admin_token=self.config.admin_token,
            timeout=self.config.http_timeout,
        )

        # Derive backend delay seeds
        env = {
            "SBR_PROXY_POLICY": policy,
            "SBR_PROXY_PORT": str(self.config.proxy_port),
            "SBR_BACKEND_ADMIN_TOKEN": self.config.admin_token,
            "SBR_BACKEND_A_ADMIN_PORT": str(self.config.admin_ports["backend-a"]),
            "SBR_BACKEND_B_ADMIN_PORT": str(self.config.admin_ports["backend-b"]),
            "SBR_BACKEND_C_ADMIN_PORT": str(self.config.admin_ports["backend-c"]),
            "SBR_BACKEND_D_ADMIN_PORT": str(self.config.admin_ports["backend-d"]),
            "SBR_BACKEND_E_ADMIN_PORT": str(self.config.admin_ports["backend-e"]),
            "SBR_BACKEND_A_DELAY_SEED": str(trial_seed + 1),
            "SBR_BACKEND_B_DELAY_SEED": str(trial_seed + 2),
            "SBR_BACKEND_C_DELAY_SEED": str(trial_seed + 3),
            "SBR_BACKEND_D_DELAY_SEED": str(trial_seed + 4),
            "SBR_BACKEND_E_DELAY_SEED": str(trial_seed + 5),
        }

        docker_lifecycle = DockerLifecycle(
            compose_project=self.config.compose_project,
            repo_root=self.repo_root,
            env=env,
        )

        sync_coord = SyncCoordinator()
        events_recorded: List[EventExecutionRecord] = []
        k6_result: Optional[K6ExecutionResult] = None
        failure_reasons: List[str] = []
        backends_cfg = scenario.get("backends", {})
        scheduled_events = list(scenario.get("events", []))
        scheduled_events.sort(key=lambda ev: float(ev.get("timestamp_seconds", 0.0)))

        try:
            # 1. Start Docker deployment
            docker_lifecycle.start()

            # 2. Wait until healthy
            docker_lifecycle.wait_until_healthy(
                proxy_port=self.config.proxy_port,
                admin_ports=self.config.admin_ports,
                admin_token=self.config.admin_token,
                timeout=self.config.readiness_timeout,
            )

            # 3. Apply baseline configuration
            backends_cfg = scenario.get("backends", {})
            admin_client.apply_baseline(backends_cfg)

            # 4. Start synchronization server
            sync_coord.start_server()
            is_docker_k6 = (k6_method == "docker")
            sync_url = sync_coord.get_sync_url(is_docker=is_docker_k6)

            # 5. Build and launch k6 workload
            summary_path = run_dir / "k6-summary.json"
            log_path = run_dir / "k6-output.log"
            raw_metrics_path = run_dir / "raw-metrics.json"
            trial_scenario_file = run_dir / "scenario.json"
            with open(trial_scenario_file, "w", encoding="utf-8") as f:
                json.dump(scenario, f, indent=2)

            k6_cmd, k6_env = self.k6_runner.build_command(
                scenario_id=scenario_id,
                proxy_port=self.config.proxy_port,
                policy=policy,
                summary_output_path=summary_path,
                sync_url=sync_url,
                scenario_file=trial_scenario_file,
                raw_metrics_output_path=raw_metrics_path,
                http_timeout=f"{int(self.config.http_timeout)}s",
                preallocated_vus=self.config.vus_preallocated,
                max_vus=self.config.vus_max,
                method=k6_method,
            )

            launch_monotonic = time.monotonic()
            k6_proc = subprocess.Popen(
                k6_cmd,
                cwd=str(self.repo_root),
                env=k6_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            k6_output_lines: List[str] = []

            def read_k6_stream():
                if k6_proc.stdout is None:
                    return
                for raw_line in k6_proc.stdout:
                    line = raw_line.rstrip("\r\n")
                    k6_output_lines.append(line)
                    sync_coord.on_stdout_line(line)

            stream_thread = threading.Thread(target=read_k6_stream, daemon=True)
            stream_thread.start()

            # 6. Wait for workload start signal
            workload_start_mono = sync_coord.wait_for_start(
                timeout=20.0,
                fallback_monotonic=launch_monotonic,
            )

            # 7. Execute scheduled administrative events
            for idx, ev in enumerate(scheduled_events):
                target_offset = float(ev.get("timestamp_seconds", 0.0))
                intended_mono = workload_start_mono + target_offset

                # Sleep until intended event time
                while True:
                    remaining = intended_mono - time.monotonic()
                    if remaining <= 0:
                        break
                    # Check if k6 process died prematurely
                    if k6_proc.poll() is not None:
                        failure_reasons.append("k6 process terminated before scheduled event")
                        break
                    time.sleep(min(remaining, 0.05))

                if k6_proc.poll() is not None:
                    break

                # Execute event
                rec = admin_client.execute_event(ev, idx, workload_start_mono)
                events_recorded.append(rec)
                if not rec.success or not rec.verified:
                    err_detail = rec.error or f"status={rec.status_code}"
                    failure_reasons.append(
                        f"Administrative event {idx} ({rec.action} on {rec.target}) failed: {err_detail}"
                    )

            # 8. Wait for k6 completion
            # Allow scenario duration + 30s buffer
            scenario_duration = float(scenario.get("duration_seconds", 60.0))
            k6_timeout = max(30.0, scenario_duration + 30.0)

            try:
                retcode = k6_proc.wait(timeout=k6_timeout)
            except subprocess.TimeoutExpired:
                k6_proc.kill()
                k6_proc.wait()
                retcode = -1
                failure_reasons.append(f"k6 process timed out after {k6_timeout}s")

            stream_thread.join(timeout=2.0)
            k6_stdout = "\n".join(k6_output_lines)
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(k6_stdout)

            # Parse and validate summary
            summary_data: Optional[Dict] = None
            if summary_path.is_file():
                try:
                    with open(summary_path, "r", encoding="utf-8") as f:
                        summary_data = json.load(f)
                    dropped = summary_data.get("workload_integrity", {}).get("dropped_iterations", 0)
                    total_reqs = summary_data.get("workload_integrity", {}).get("total_requests", 0)
                    if dropped > 0:
                        failure_reasons.append(f"Dropped iterations detected in k6 summary: {dropped}")
                    if total_reqs == 0:
                        failure_reasons.append("Zero requests recorded in k6 summary")
                except Exception as e:
                    failure_reasons.append(f"Failed to read k6 summary JSON: {e}")
            else:
                failure_reasons.append("k6 summary JSON file was not generated")

            if retcode != 0:
                failure_reasons.append(f"k6 process exited with return code {retcode}")

            k6_result = K6ExecutionResult(
                returncode=retcode,
                started_at_wall=trial_start_wall,
                finished_at_wall=time.time(),
                duration_seconds=time.time() - trial_start_wall,
                stdout=k6_stdout,
                summary_data=summary_data,
                summary_path=summary_path,
                log_path=log_path,
                valid=(len(failure_reasons) == 0),
                validation_reason="; ".join(failure_reasons) or "OK",
            )

        except Exception as e:
            failure_reasons.append(admin_client.redact(f"Trial execution error: {e}"))
            logger.exception("Error during trial execution")

        finally:
            sync_coord.close()

            # Attempt clean baseline restore before teardown
            try:
                if backends_cfg:
                    admin_client.apply_baseline(backends_cfg)
            except Exception:
                pass

            # Always tear down Docker containers
            docker_lifecycle.stop()

        # Finalize metadata
        trial_end_wall = time.time()
        metadata["end_time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        metadata["duration_seconds"] = trial_end_wall - trial_start_wall

        # Timing analysis
        drifts = [rec.scheduling_drift_seconds for rec in events_recorded]
        latencies = [rec.request_latency_seconds for rec in events_recorded]

        metadata["timing_summary"] = {
            "sync_method": sync_coord.sync_method,
            "events_scheduled": len(scheduled_events),
            "events_executed": len(events_recorded),
            "max_scheduling_drift_seconds": max(drifts) if drifts else 0.0,
            "mean_scheduling_drift_seconds": (sum(drifts) / len(drifts)) if drifts else 0.0,
            "max_request_latency_seconds": max(latencies) if latencies else 0.0,
            "mean_request_latency_seconds": (sum(latencies) / len(latencies)) if latencies else 0.0,
        }

        metadata["valid"] = (len(failure_reasons) == 0)
        metadata["failure_reason"] = "; ".join(failure_reasons) if failure_reasons else None

        # Write events.json
        events_file = run_dir / "events.json"
        with open(events_file, "w", encoding="utf-8") as f:
            json.dump([rec.to_dict() for rec in events_recorded], f, indent=2)

        # Process and export structured data (requests.csv and metrics.json)
        timeline = IntervalTimeline(events=[rec.to_dict() for rec in events_recorded], scenario=scenario)
        start_wall = sync_coord.workload_start_wall or trial_start_wall
        dropped_iters = summary_data.get("workload_integrity", {}).get("dropped_iterations", 0) if summary_data else 0

        raw_metrics_path = run_dir / "raw-metrics.json"
        DataCollector.collect_and_export(
            raw_metrics_path=raw_metrics_path,
            output_dir=run_dir,
            workload_start_wall=start_wall,
            timeline=timeline,
            scenario=scenario,
            policy=policy,
            trial_index=trial_index,
            dropped_iterations=dropped_iters,
        )

        # Write metadata.json (initial before validation)
        metadata["valid"] = (len(failure_reasons) == 0)
        metadata["failure_reason"] = "; ".join(failure_reasons) if failure_reasons else None
        meta_file = run_dir / "metadata.json"
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        # Validate trial artifacts on disk
        is_data_valid, data_errors = DataValidator.validate_trial(
            run_dir=run_dir,
            scenario=scenario,
            policy=policy,
            trial_index=trial_index,
        )
        if not is_data_valid:
            for err in data_errors:
                if err not in failure_reasons:
                    failure_reasons.append(err)

            metadata["valid"] = False
            metadata["failure_reason"] = "; ".join(failure_reasons) if failure_reasons else None
            with open(meta_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)

        status_str = "VALID" if metadata["valid"] else "INVALID"
        logger.info(
            "=== Trial %d completed: [%s] (drift max=%.3fs, latency max=%.3fs) ===",
            trial_index,
            status_str,
            metadata["timing_summary"]["max_scheduling_drift_seconds"],
            metadata["timing_summary"]["max_request_latency_seconds"],
        )

        return TrialResult(
            trial_index=trial_index,
            scenario_id=scenario_id,
            policy=policy,
            run_dir=run_dir,
            valid=metadata["valid"],
            failure_reason=metadata["failure_reason"],
            metadata=metadata,
            events=events_recorded,
            k6_result=k6_result,
        )

    def run_all(self) -> List[TrialResult]:
        """Sequentially executes all configured repetitions for this experiment."""
        results: List[TrialResult] = []
        for i in range(1, self.config.repetitions + 1):
            res = self.run_trial(trial_index=i)
            results.append(res)
        return results
