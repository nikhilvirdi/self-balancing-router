"""
Administrative client and event executor for backend runtime configuration.
"""

from dataclasses import asdict, dataclass
import json
import time
from typing import Any, Dict, Optional
import urllib.error
import urllib.request


@dataclass
class EventExecutionRecord:
    """Record of an executed administrative event during an experiment."""

    event_index: int
    target: str
    action: str
    planned_timestamp_seconds: float
    actual_trigger_relative_seconds: float
    scheduling_drift_seconds: float
    request_latency_seconds: float
    status_code: int
    success: bool
    verified: bool
    error: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d


class AdminClient:
    """HTTP client for backend administrative API."""

    def __init__(
        self,
        admin_ports: Dict[str, int],
        admin_token: str,
        timeout: float = 5.0,
        host: str = "127.0.0.1",
    ):
        self.admin_ports = dict(admin_ports)
        self.admin_token = admin_token
        self.timeout = timeout
        self.host = host

    def redact(self, text: str) -> str:
        """Redacts administrative token from any text or URL."""
        if not self.admin_token:
            return text
        return text.replace(self.admin_token, "[REDACTED]")

    def _url_for(self, backend_id: str, path: str) -> str:
        if backend_id not in self.admin_ports:
            raise ValueError(f"Unknown backend ID: '{backend_id}'. Known backends: {sorted(list(self.admin_ports.keys()))}")
        port = self.admin_ports[backend_id]
        clean_path = path if path.startswith("/") else f"/{path}"
        return f"http://{self.host}:{port}{clean_path}"

    def _request(
        self,
        backend_id: str,
        path: str,
        method: str = "GET",
        payload: Optional[Dict[str, Any]] = None,
    ) -> tuple[int, Dict[str, Any], float]:
        """
        Executes an HTTP request against a backend administrative endpoint.
        Returns (status_code, response_json, elapsed_seconds).
        """
        url = self._url_for(backend_id, path)
        headers = {
            "Authorization": f"Bearer {self.admin_token}",
            "Accept": "application/json",
            "User-Agent": "sbr-experiment-orchestrator/1.0",
        }
        data_bytes = None
        if payload is not None:
            data_bytes = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
        start_time = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                elapsed = time.monotonic() - start_time
                body = resp.read().decode("utf-8")
                parsed = json.loads(body) if body else {}
                return resp.status, parsed, elapsed
        except urllib.error.HTTPError as e:
            elapsed = time.monotonic() - start_time
            body = e.read().decode("utf-8")
            try:
                parsed = json.loads(body)
            except Exception:
                parsed = {"raw_error": self.redact(body)}
            return e.code, parsed, elapsed
        except Exception as e:
            elapsed = time.monotonic() - start_time
            safe_err = self.redact(str(e))
            raise RuntimeError(f"Admin request to {backend_id} ({path}) failed: {safe_err}") from None

    def get_workload(self, backend_id: str) -> Dict[str, Any]:
        status, data, _ = self._request(backend_id, "/admin/v1/workload", method="GET")
        if status != 200:
            raise RuntimeError(f"GET /admin/v1/workload on {backend_id} returned status {status}: {data}")
        return data

    def update_workload(
        self,
        backend_id: str,
        processing_delay: str,
        processing_jitter: str,
    ) -> tuple[Dict[str, Any], float]:
        payload = {
            "processing_delay": processing_delay,
            "processing_jitter": processing_jitter,
        }
        status, data, elapsed = self._request(backend_id, "/admin/v1/workload", method="PUT", payload=payload)
        if status != 200:
            raise RuntimeError(f"PUT /admin/v1/workload on {backend_id} returned status {status}: {data}")
        return data, elapsed

    def reset_workload(self, backend_id: str) -> tuple[Dict[str, Any], float]:
        status, data, elapsed = self._request(backend_id, "/admin/v1/workload/reset", method="POST")
        if status != 200:
            raise RuntimeError(f"POST /admin/v1/workload/reset on {backend_id} returned status {status}: {data}")
        return data, elapsed

    def apply_baseline(self, backends_cfg: Dict[str, Dict[str, Any]]) -> None:
        """Applies scenario baseline delay and jitter to all configured backends and verifies state."""
        for backend_id, cfg in backends_cfg.items():
            delay = cfg.get("processing_delay", "20ms")
            jitter = cfg.get("processing_jitter", "5ms")
            self.update_workload(backend_id, delay, jitter)
            # Verify applied state
            current = self.get_workload(backend_id)
            if current.get("processing_delay") != delay or current.get("processing_jitter") != jitter:
                raise RuntimeError(
                    f"Baseline verification failed for {backend_id}: expected delay={delay}, jitter={jitter}; "
                    f"got delay={current.get('processing_delay')}, jitter={current.get('processing_jitter')}"
                )

    def execute_event(
        self,
        event: Dict[str, Any],
        event_index: int,
        workload_start_monotonic: float,
    ) -> EventExecutionRecord:
        """Executes a single scheduled administrative event and returns its detailed record."""
        target = event.get("target", "")
        action = event.get("action", "")
        planned_ts = float(event.get("timestamp_seconds", 0.0))

        now_mono = time.monotonic()
        actual_rel_ts = now_mono - workload_start_monotonic
        drift = actual_rel_ts - planned_ts

        params: Dict[str, Any] = {}
        error: Optional[str] = None
        status_code = 0
        latency = 0.0
        success = False
        verified = False

        try:
            if action == "update":
                delay = event.get("processing_delay", "")
                jitter = event.get("processing_jitter", "")
                params = {"processing_delay": delay, "processing_jitter": jitter}
                data, latency = self.update_workload(target, delay, jitter)
                status_code = 200
                success = True

                # Verification check
                current = self.get_workload(target)
                if current.get("processing_delay") == delay and current.get("processing_jitter") == jitter:
                    verified = True
                else:
                    error = (
                        f"State mismatch: expected delay={delay}, jitter={jitter}; "
                        f"got {current.get('processing_delay')}, {current.get('processing_jitter')}"
                    )
            elif action == "reset":
                params = {}
                data, latency = self.reset_workload(target)
                status_code = 200
                success = True

                # Verification check
                current = self.get_workload(target)
                baseline_delay = current.get("baseline_processing_delay")
                if current.get("processing_delay") == baseline_delay:
                    verified = True
                else:
                    error = f"Reset mismatch: current delay={current.get('processing_delay')} != baseline={baseline_delay}"
            else:
                error = f"Unknown event action '{action}'"
        except Exception as e:
            error = self.redact(str(e))
            success = False
            verified = False

        return EventExecutionRecord(
            event_index=event_index,
            target=target,
            action=action,
            planned_timestamp_seconds=planned_ts,
            actual_trigger_relative_seconds=actual_rel_ts,
            scheduling_drift_seconds=drift,
            request_latency_seconds=latency,
            status_code=status_code,
            success=success,
            verified=verified,
            error=error,
            parameters=params,
        )

