"""
Subprocess executor and result collector for Grafana k6 workload generator.
"""

from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("sbr.orchestrator.k6")


@dataclass
class K6ExecutionResult:
    """Summary of a completed k6 generator execution."""

    returncode: int
    started_at_wall: float
    finished_at_wall: float
    duration_seconds: float
    stdout: str
    summary_data: Optional[Dict]
    summary_path: Path
    log_path: Path
    valid: bool
    validation_reason: str


class K6Runner:
    """Executes the k6 benchmark workload generator via local binary or Docker."""

    def __init__(
        self,
        repo_root: Optional[Path] = None,
        exec_mode: str = "auto",
        docker_image: str = "grafana/k6:latest",
    ):
        self.repo_root = repo_root or Path(__file__).resolve().parent.parent.parent
        self.exec_mode = exec_mode
        self.docker_image = docker_image

    def resolve_execution_method(self) -> str:
        """Determines whether to execute via local k6 or Docker container."""
        if self.exec_mode == "local":
            if not shutil.which("k6"):
                raise RuntimeError("Local 'k6' binary requested but not found in PATH")
            return "local"
        if self.exec_mode == "docker":
            return "docker"
        # 'auto'
        if shutil.which("k6"):
            return "local"
        return "docker"

    def build_command(
        self,
        scenario_id: str,
        proxy_port: int,
        policy: str,
        summary_output_path: Path,
        sync_url: str,
        scenario_file: Optional[Path] = None,
        raw_metrics_output_path: Optional[Path] = None,
        http_timeout: str = "10s",
        preallocated_vus: int = 0,
        max_vus: int = 0,
        method: Optional[str] = None,
    ) -> Tuple[List[str], Dict[str, str]]:
        """Constructs the k6 execution command line and environment."""
        resolved_method = method or self.resolve_execution_method()
        workload_rel = Path("experiments") / "k6" / "workload.js"

        if resolved_method == "local":
            proxy_url = f"http://127.0.0.1:{proxy_port}"
            cmd = [
                "k6",
                "run",
                "-e", f"SBR_SCENARIO={scenario_id}",
                "-e", f"SBR_PROXY_URL={proxy_url}",
                "-e", f"SBR_PROXY_POLICY={policy}",
                "-e", f"SBR_HTTP_TIMEOUT={http_timeout}",
                "-e", f"SBR_SUMMARY_OUTPUT={summary_output_path.resolve()}",
                "-e", f"SBR_SYNC_URL={sync_url}",
            ]
            if scenario_file is not None:
                cmd.extend(["-e", f"SBR_SCENARIO_FILE={scenario_file.resolve()}"])
            if preallocated_vus > 0:
                cmd.extend(["-e", f"SBR_VUS_PREALLOCATED={preallocated_vus}"])
            if max_vus > 0:
                cmd.extend(["-e", f"SBR_VUS_MAX={max_vus}"])
            if raw_metrics_output_path is not None:
                cmd.extend(["-o", f"json={raw_metrics_output_path.resolve()}"])
            cmd.append(str(workload_rel))
            return cmd, os.environ.copy()
        else:
            # Docker execution
            # Host loopback is reached via host.docker.internal
            proxy_url = f"http://host.docker.internal:{proxy_port}"
            # Path inside container workspace
            try:
                rel_summary = summary_output_path.resolve().relative_to(self.repo_root.resolve())
                container_summary = f"/workspace/{rel_summary.as_posix()}"
            except ValueError:
                container_summary = f"/workspace/experiments/results/{scenario_id}/{policy}/k6-summary.json"

            cmd = [
                "docker", "run", "--rm",
                "-v", f"{self.repo_root.resolve()}:/workspace",
                "-w", "/workspace",
                "-e", f"SBR_SCENARIO={scenario_id}",
                "-e", f"SBR_PROXY_URL={proxy_url}",
                "-e", f"SBR_PROXY_POLICY={policy}",
                "-e", f"SBR_HTTP_TIMEOUT={http_timeout}",
                "-e", f"SBR_SUMMARY_OUTPUT={container_summary}",
                "-e", f"SBR_SYNC_URL={sync_url}",
            ]
            if scenario_file is not None:
                try:
                    rel_sc = scenario_file.resolve().relative_to(self.repo_root.resolve())
                    container_sc = f"/workspace/{rel_sc.as_posix()}"
                except ValueError:
                    container_sc = str(scenario_file.resolve())
                cmd.extend(["-e", f"SBR_SCENARIO_FILE={container_sc}"])
            if preallocated_vus > 0:
                cmd.extend(["-e", f"SBR_VUS_PREALLOCATED={preallocated_vus}"])
            if max_vus > 0:
                cmd.extend(["-e", f"SBR_VUS_MAX={max_vus}"])

            cmd.extend([self.docker_image, "run"])
            if raw_metrics_output_path is not None:
                try:
                    rel_raw = raw_metrics_output_path.resolve().relative_to(self.repo_root.resolve())
                    container_raw = f"/workspace/{rel_raw.as_posix()}"
                except ValueError:
                    container_raw = f"/workspace/experiments/results/{scenario_id}/{policy}/raw-metrics.json"
                cmd.extend(["-o", f"json={container_raw}"])
            cmd.append(f"/workspace/{workload_rel.as_posix()}")
            return cmd, os.environ.copy()

    def run_workload(
        self,
        cmd: List[str],
        env: Dict[str, str],
        summary_path: Path,
        log_path: Path,
        on_line: Optional[Callable[[str], None]] = None,
        timeout: Optional[float] = None,
    ) -> K6ExecutionResult:
        """Executes the k6 process, streams logs, and collects results."""
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        if summary_path.exists():
            summary_path.unlink()

        started_at = time.time()
        output_lines: List[str] = []

        logger.info("Executing k6 command: %s", " ".join(cmd[:6]) + " ...")
        proc = subprocess.Popen(
            cmd,
            cwd=str(self.repo_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        def read_stream():
            if proc.stdout is None:
                return
            for raw_line in proc.stdout:
                line = raw_line.rstrip("\r\n")
                output_lines.append(line)
                if on_line:
                    try:
                        on_line(line)
                    except Exception:
                        pass

        reader_thread = threading.Thread(target=read_stream, daemon=True)
        reader_thread.start()

        try:
            returncode = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            returncode = -1
            output_lines.append(f"\n[ORCHESTRATOR ERROR] k6 process timed out after {timeout}s")
        except KeyboardInterrupt:
            proc.kill()
            proc.wait()
            returncode = -2
            output_lines.append("\n[ORCHESTRATOR ERROR] k6 process interrupted by user")
            raise

        reader_thread.join(timeout=2.0)
        finished_at = time.time()
        duration = finished_at - started_at

        full_log = "\n".join(output_lines)
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(full_log)

        # Parse and validate summary output
        summary_data: Optional[Dict] = None
        valid = (returncode == 0)
        reasons: List[str] = []
        if returncode != 0:
            reasons.append(f"k6 exited with non-zero code {returncode}")

        if summary_path.is_file():
            try:
                with open(summary_path, "r", encoding="utf-8") as f:
                    summary_data = json.load(f)

                # Workload integrity check
                integrity = summary_data.get("workload_integrity", {})
                dropped = integrity.get("dropped_iterations", 0)
                total_reqs = integrity.get("total_requests", 0)

                if dropped > 0:
                    valid = False
                    reasons.append(f"Dropped iterations detected: {dropped} iterations dropped due to VU starvation")
                elif total_reqs == 0:
                    valid = False
                    reasons.append("Zero requests recorded by generator")
            except Exception as e:
                valid = False
                reasons.append(f"Failed to parse k6 summary JSON: {e}")
        else:
            valid = False
            reasons.append(f"Expected summary JSON file was not generated at {summary_path}")

        reason = "; ".join(reasons) or "OK"

        return K6ExecutionResult(
            returncode=returncode,
            started_at_wall=started_at,
            finished_at_wall=finished_at,
            duration_seconds=duration,
            stdout=full_log,
            summary_data=summary_data,
            summary_path=summary_path,
            log_path=log_path,
            valid=valid,
            validation_reason=reason,
        )
