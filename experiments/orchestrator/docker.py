"""
Docker Compose lifecycle and health polling for isolated experiment deployments.
"""

import json
import logging
import os
from pathlib import Path
import subprocess
import time
from typing import Dict, List, Optional
import urllib.error
import urllib.request

logger = logging.getLogger("sbr.orchestrator.docker")


class DockerLifecycle:
    """Manages an isolated Docker Compose project for an experiment."""

    def __init__(
        self,
        compose_project: str,
        compose_files: Optional[List[Path]] = None,
        env: Optional[Dict[str, str]] = None,
        repo_root: Optional[Path] = None,
    ):
        self.project = compose_project
        self.repo_root = repo_root or Path(__file__).resolve().parent.parent.parent
        self.compose_files = compose_files or [
            self.repo_root / "deployments" / "compose.yaml",
            self.repo_root / "deployments" / "compose.experiments.yaml",
        ]
        self.env = os.environ.copy()
        if env:
            self.env.update(env)

    def _base_cmd(self) -> List[str]:
        cmd = ["docker", "compose", "-p", self.project]
        for cf in self.compose_files:
            if cf.is_file():
                cmd.extend(["-f", str(cf)])
        return cmd

    def is_docker_available(self) -> bool:
        """Verifies Docker CLI and daemon are operational."""
        try:
            res = subprocess.run(
                ["docker", "info"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=5.0,
            )
            return res.returncode == 0
        except Exception:
            return False

    def start(self, build: bool = False, timeout: float = 60.0) -> None:
        """Starts the isolated Compose project containers."""
        cmd = self._base_cmd() + ["up", "-d"]
        if build:
            cmd.append("--build")

        logger.info("Starting Docker Compose project '%s'...", self.project)
        proc = subprocess.run(
            cmd,
            cwd=str(self.repo_root),
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=timeout,
        )
        if proc.returncode != 0:
            err = proc.stderr.strip() or proc.stdout.strip()
            raise RuntimeError(f"Failed to start Docker Compose project '{self.project}': {err}")

    def ensure_clean_state(self, timeout: float = 30.0) -> None:
        """
        Ensures an isolated, clean starting state by removing any lingering containers,
        volumes, or networks from previously aborted trials with this compose project name.
        """
        logger.info("Verifying clean Docker environment for project '%s'...", self.project)
        self.stop(timeout=timeout)

    def stop(self, timeout: float = 30.0) -> None:
        """Stops and tears down containers and networks owned by this Compose project."""
        cmd = self._base_cmd() + ["down", "-v", "--remove-orphans"]
        logger.info("Tearing down Docker Compose project '%s'...", self.project)
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(self.repo_root),
                env=self.env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=timeout,
            )
            if proc.returncode != 0:
                logger.warning("Docker compose down returned code %d: %s", proc.returncode, proc.stderr.strip())
        except Exception as e:
            logger.warning("Error during Docker Compose teardown: %s", e)

    def wait_until_healthy(
        self,
        proxy_port: int,
        admin_ports: Dict[str, int],
        admin_token: str,
        timeout: float = 30.0,
        poll_interval: float = 0.5,
        host: str = "127.0.0.1",
    ) -> None:
        """
        Polls until the proxy /readyz endpoint and all 5 backend admin endpoints return 200 OK.
        """
        deadline = time.monotonic() + timeout
        proxy_url = f"http://{host}:{proxy_port}/readyz"

        unready_targets: Dict[str, str] = {}

        while time.monotonic() < deadline:
            unready_targets.clear()

            # 1. Check proxy readiness
            try:
                req = urllib.request.Request(proxy_url, headers={"User-Agent": "sbr-health-poll/1.0"})
                with urllib.request.urlopen(req, timeout=1.0) as resp:
                    if resp.status != 200:
                        unready_targets["proxy"] = f"status={resp.status}"
            except Exception as e:
                unready_targets["proxy"] = f"error={type(e).__name__}"

            # 2. Check each backend admin endpoint
            admin_headers = {
                "Authorization": f"Bearer {admin_token}",
                "User-Agent": "sbr-health-poll/1.0",
            }
            for b_id, port in admin_ports.items():
                admin_url = f"http://{host}:{port}/admin/v1/workload"
                try:
                    req = urllib.request.Request(admin_url, headers=admin_headers)
                    with urllib.request.urlopen(req, timeout=1.0) as resp:
                        if resp.status != 200:
                            unready_targets[b_id] = f"status={resp.status}"
                except Exception as e:
                    unready_targets[b_id] = f"error={type(e).__name__}"

            if not unready_targets:
                logger.info("All services healthy (proxy and 5 backend administrative endpoints ready).")
                return

            time.sleep(poll_interval)

        raise RuntimeError(
            f"Readiness check timed out after {timeout}s. Unready targets: {sorted(list(unready_targets.keys()))} ({unready_targets})"
        )

