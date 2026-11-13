"""
Workload execution synchronization between k6 generator and orchestrator.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import re
import socketserver
import threading
import time
from typing import Optional

logger = logging.getLogger("sbr.orchestrator.sync")


class _SyncHTTPRequestHandler(BaseHTTPRequestHandler):
    coordinator: Optional["SyncCoordinator"] = None

    def do_GET(self):
        if self.path.startswith("/start"):
            if self.coordinator is not None:
                self.coordinator.record_start(method="webhook")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}\n')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress noisy standard HTTP access logs
        pass


class SyncCoordinator:
    """Coordinates workload execution start time using webhook and stdout signals."""

    def __init__(self, host: str = "127.0.0.1"):
        self.host = host
        self._server: Optional[HTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None
        self.port: int = 0
        self.started_event = threading.Event()

        self.workload_start_monotonic: Optional[float] = None
        self.workload_start_wall: Optional[float] = None
        self.sync_method: str = "none"
        self._lock = threading.Lock()

    def start_server(self) -> int:
        """Starts the ephemeral synchronization HTTP server."""
        handler_cls = _SyncHTTPRequestHandler
        handler_cls.coordinator = self

        self._server = HTTPServer((self.host, 0), handler_cls)
        self.port = self._server.server_port

        self._server_thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._server_thread.start()
        return self.port

    def get_sync_url(self, is_docker: bool = False) -> str:
        """Returns the webhook sync URL for the k6 worker."""
        if is_docker:
            return f"http://host.docker.internal:{self.port}/start"
        return f"http://127.0.0.1:{self.port}/start"

    def record_start(self, method: str = "webhook", detected_at_monotonic: Optional[float] = None) -> None:
        """Idempotently records the workload start timestamp."""
        with self._lock:
            if not self.started_event.is_set():
                now_mono = detected_at_monotonic if detected_at_monotonic is not None else time.monotonic()
                self.workload_start_monotonic = now_mono
                self.workload_start_wall = time.time()
                self.sync_method = method
                self.started_event.set()

    def on_stdout_line(self, line: str) -> None:
        """Parses k6 stdout lines for the machine-readable start token."""
        if "[SBR_WORKLOAD_START]" in line:
            self.record_start(method="stdout")

    def wait_for_start(self, timeout: float = 20.0, fallback_monotonic: Optional[float] = None) -> float:
        """
        Waits for the workload start signal. Falls back gracefully to process launch time if timed out.
        """
        signaled = self.started_event.wait(timeout)
        with self._lock:
            if not signaled:
                now_mono = fallback_monotonic if fallback_monotonic is not None else time.monotonic()
                self.workload_start_monotonic = now_mono
                self.workload_start_wall = time.time()
                self.sync_method = "fallback"
                logger.warning(
                    "Workload synchronization timed out after %.1fs; falling back to process start time",
                    timeout,
                )
            return self.workload_start_monotonic or time.monotonic()

    def close(self) -> None:
        """Shuts down the sync server."""
        if self._server:
            try:
                self._server.shutdown()
                self._server.server_close()
            except Exception:
                pass
            self._server = None

