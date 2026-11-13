#!/usr/bin/env python3
"""
Integration verification script for self-balancing-router five-backend simulation.
Validates proxy readiness, five-backend distribution, heterogeneous profiles,
administrative authentication, runtime degradation, recovery, and listener isolation.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request


def make_request(url, method="GET", headers=None, data=None, timeout=5.0):
    req_headers = headers or {}
    encoded_data = None
    if data is not None:
        if isinstance(data, (dict, list)):
            encoded_data = json.dumps(data).encode("utf-8")
            if "Content-Type" not in req_headers:
                req_headers["Content-Type"] = "application/json"
        elif isinstance(data, str):
            encoded_data = data.encode("utf-8")
        else:
            encoded_data = data

    req = urllib.request.Request(url, data=encoded_data, headers=req_headers, method=method)
    try:
        start_time = time.monotonic()
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            elapsed = time.monotonic() - start_time
            body = resp.read().decode("utf-8")
            return resp.status, resp.headers, body, elapsed
    except urllib.error.HTTPError as e:
        elapsed = time.monotonic() - start_time
        body = e.read().decode("utf-8")
        return e.code, e.headers, body, elapsed


def run_checks(proxy_url, admin_token, admin_ports, timeout):
    results = []
    degraded_ports = []

    def record(name, passed, detail=""):
        status_str = "PASS" if passed else "FAIL"
        print(f"[{status_str}] {name}" + (f" - {detail}" if detail else ""))
        results.append((name, passed, detail))

    try:
        # 1. Proxy Readiness
        print("\n--- 1. Proxy Readiness ---")
        live_status, _, live_body, _ = make_request(f"{proxy_url}/livez", timeout=timeout)
        ready_status, _, ready_body, _ = make_request(f"{proxy_url}/readyz", timeout=timeout)
        record(
            "Proxy /livez check",
            live_status == 200 and "ok" in live_body,
            f"status={live_status}",
        )
        record(
            "Proxy /readyz check",
            ready_status == 200 and "ok" in ready_body,
            f"status={ready_status}",
        )

        # 2. Five-Backend Identity & Round Robin Routing
        print("\n--- 2. Five-Backend Round Robin Routing ---")
        seen_backends = set()
        routing_sequence = []
        for i in range(10):
            status, _, body, _ = make_request(f"{proxy_url}/api/verify", timeout=timeout)
            if status == 200:
                try:
                    payload = json.loads(body)
                    b_id = payload.get("backend_id")
                    if b_id:
                        seen_backends.add(b_id)
                        routing_sequence.append(b_id)
                except Exception as e:
                    print(f"Failed to parse response: {e}")

        expected_backends = {"backend-a", "backend-b", "backend-c", "backend-d", "backend-e"}
        record(
            "All five backends present in rotation",
            expected_backends.issubset(seen_backends),
            f"seen={sorted(list(seen_backends))}",
        )
        print(f"Sample routing sequence: {' -> '.join(routing_sequence[:5])}")

        # 3. Administrative Workload Profiles
        print("\n--- 3. Administrative Workload Profiles ---")
        admin_auth_headers = {"Authorization": f"Bearer {admin_token}"}
        profiles = {}
        for port in admin_ports:
            status, _, body, _ = make_request(
                f"http://127.0.0.1:{port}/admin/v1/workload",
                headers=admin_auth_headers,
                timeout=timeout,
            )
            if status == 200:
                try:
                    cfg = json.loads(body)
                    profiles[port] = cfg
                    print(
                        f"Port {port}: delay={cfg.get('processing_delay')} "
                        f"jitter={cfg.get('processing_jitter')} "
                        f"baseline_delay={cfg.get('baseline_processing_delay')}"
                    )
                except Exception as e:
                    record(f"Parse profile port {port}", False, str(e))
            else:
                record(f"Query profile port {port}", False, f"status={status}")

        record(
            "All administrative endpoints reachable",
            len(profiles) == len(admin_ports),
            f"reachable={len(profiles)}/{len(admin_ports)}",
        )

        # Verify heterogeneous delays
        delays = [p.get("processing_delay") for p in profiles.values()]
        record(
            "Heterogeneous delay profiles configured",
            len(set(delays)) >= 4,
            f"distinct_delays={sorted(list(set(delays)))}",
        )

        # 4. Administrative Security & Authentication
        print("\n--- 4. Administrative Security Controls ---")
        first_admin_port = admin_ports[0]
        # Missing auth
        no_auth_status, _, _, _ = make_request(
            f"http://127.0.0.1:{first_admin_port}/admin/v1/workload",
            timeout=timeout,
        )
        record(
            "Missing authorization rejected (401)",
            no_auth_status == 401,
            f"status={no_auth_status}",
        )

        # Wrong token
        wrong_auth_status, _, _, _ = make_request(
            f"http://127.0.0.1:{first_admin_port}/admin/v1/workload",
            headers={"Authorization": "Bearer wrong-token-12345678"},
            timeout=timeout,
        )
        record(
            "Invalid authorization token rejected (401)",
            wrong_auth_status == 401,
            f"status={wrong_auth_status}",
        )

        # Query param auth rejected
        query_auth_status, _, _, _ = make_request(
            f"http://127.0.0.1:{first_admin_port}/admin/v1/workload?token={admin_token}",
            timeout=timeout,
        )
        record(
            "Query parameter authentication rejected (401)",
            query_auth_status == 401,
            f"status={query_auth_status}",
        )

        # Listener isolation on public proxy port
        proxy_admin_status, _, proxy_admin_body, _ = make_request(
            f"{proxy_url}/admin/v1/workload",
            headers=admin_auth_headers,
            timeout=timeout,
        )
        record(
            "Public proxy listener rejects /admin route (404)",
            proxy_admin_status == 404,
            f"status={proxy_admin_status}",
        )

        # 5. Runtime Degradation
        print("\n--- 5. Runtime Workload Degradation ---")
        target_port = admin_ports[0]
        degrade_payload = {"processing_delay": "300ms", "processing_jitter": "30ms"}
        put_status, _, put_body, _ = make_request(
            f"http://127.0.0.1:{target_port}/admin/v1/workload",
            method="PUT",
            headers=admin_auth_headers,
            data=degrade_payload,
            timeout=timeout,
        )
        degraded_ports.append(target_port)
        record(
            "PUT /admin/v1/workload accepts update (200)",
            put_status == 200,
            f"status={put_status}",
        )

        # Verify updated state
        get_status, _, get_body, _ = make_request(
            f"http://127.0.0.1:{target_port}/admin/v1/workload",
            headers=admin_auth_headers,
            timeout=timeout,
        )
        if get_status == 200:
            up_cfg = json.loads(get_body)
            record(
                "Runtime delay updated to 300ms",
                up_cfg.get("processing_delay") == "300ms",
                f"delay={up_cfg.get('processing_delay')}",
            )
            record(
                "Baseline delay remains immutable",
                up_cfg.get("baseline_processing_delay") == "20ms",
                f"baseline={up_cfg.get('baseline_processing_delay')}",
            )
        else:
            record("Verify updated workload", False, f"status={get_status}")

        # 6. Controlled Application Traffic during Degradation
        print("\n--- 6. Application Traffic Under Degradation ---")
        traffic_ok = True
        for _ in range(5):
            st, _, _, _ = make_request(f"{proxy_url}/api/work", timeout=timeout)
            if st != 200:
                traffic_ok = False
        record(
            "Application traffic succeeds during backend degradation",
            traffic_ok,
        )

        # 7. Runtime Recovery
        print("\n--- 7. Runtime Workload Recovery ---")
        reset_status, _, reset_body, _ = make_request(
            f"http://127.0.0.1:{target_port}/admin/v1/workload/reset",
            method="POST",
            headers=admin_auth_headers,
            data=b"",
            timeout=timeout,
        )
        record(
            "POST /admin/v1/workload/reset succeeds (200)",
            reset_status == 200,
            f"status={reset_status}",
        )
        degraded_ports.remove(target_port)

        if reset_status == 200:
            res_cfg = json.loads(reset_body)
            record(
                "Workload restored to baseline (20ms)",
                res_cfg.get("processing_delay") == "20ms",
                f"delay={res_cfg.get('processing_delay')}",
            )

    finally:
        # Cleanup any remaining degraded backends
        for p in degraded_ports:
            try:
                make_request(
                    f"http://127.0.0.1:{p}/admin/v1/workload/reset",
                    method="POST",
                    headers={"Authorization": f"Bearer {admin_token}"},
                    data=b"",
                    timeout=2.0,
                )
                print(f"Cleaned up degradation on port {p}")
            except Exception:
                pass

    print("\n================ Verification Summary ================")
    all_passed = all(p for _, p, _ in results)
    total = len(results)
    passed_count = sum(1 for _, p, _ in results if p)
    print(f"Total Checks: {total} | Passed: {passed_count} | Failed: {total - passed_count}")
    return all_passed


def main():
    parser = argparse.ArgumentParser(description="Verify self-balancing-router simulation environment")
    parser.add_argument(
        "--proxy-url",
        default=os.environ.get("SBR_PROXY_URL", "http://127.0.0.1:18080"),
        help="URL of the HTTP reverse proxy (default: http://127.0.0.1:18080 or SBR_PROXY_URL)",
    )
    parser.add_argument(
        "--admin-token",
        default=os.environ.get("SBR_BACKEND_ADMIN_TOKEN", "test-admin-secret-token-123456"),
        help="Administrative Bearer token (default: SBR_BACKEND_ADMIN_TOKEN)",
    )
    parser.add_argument(
        "--admin-ports",
        default=os.environ.get("SBR_ADMIN_PORTS", "19091,19092,19093,19094,19095"),
        help="Comma-separated backend admin ports on host (default: 19091,19092,19093,19094,19095)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP request timeout in seconds (default: 5.0)",
    )
    args = parser.parse_args()

    ports = [int(p.strip()) for p in args.admin_ports.split(",") if p.strip()]

    print(f"Running simulation verification:")
    print(f"  Proxy URL:    {args.proxy_url}")
    print(f"  Admin Ports:  {ports}")
    print(f"  Auth Token:   [CONFIGURED]")

    success = run_checks(args.proxy_url, args.admin_token, ports, args.timeout)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()

