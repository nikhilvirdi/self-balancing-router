/**
 * Scenario-driven HTTP workload generator for self-balancing-router.
 * Consumes standardized benchmark scenario catalog definitions without hardcoding
 * arrival rates, stage durations, or backend configurations.
 *
 * Environment variables:
 *   SBR_SCENARIO         - Scenario identifier, filename, or index (default: "scenario_1_homogeneous").
 *   SBR_PROXY_URL        - Reverse proxy base URL (default: "http://127.0.0.1:8080").
 *   SBR_REQUEST_PATH     - Application request path (default: "/api/workload").
 *   SBR_PROXY_POLICY     - Optional routing policy tag for metrics (e.g. "round-robin", "p2c").
 *   SBR_HTTP_TIMEOUT     - HTTP request timeout (default: "10s").
 *   SBR_VUS_PREALLOCATED - Explicit override for preallocated virtual users.
 *   SBR_VUS_MAX          - Explicit override for maximum virtual users.
 *   SBR_SUMMARY_OUTPUT   - Filepath for machine-readable JSON summary export.
 */

import http from 'k6/http';
import exec from 'k6/execution';
import { Counter, Rate, Trend } from 'k6/metrics';
import {
  resolveScenario,
  buildK6Options,
  constructTargetURL,
  getTimelinePhase,
  classifyResponse,
  formatSummaryReport,
} from './config.js';

// 1. Initialization Context: load scenario from catalog or explicit scenario file
let selectedScenario;
if (__ENV.SBR_SCENARIO_FILE) {
  const fileContent = open(__ENV.SBR_SCENARIO_FILE).replace(/^\uFEFF/, '');
  const parsed = JSON.parse(fileContent);
  if (parsed.scenarios && Array.isArray(parsed.scenarios)) {
    selectedScenario = resolveScenario(parsed, __ENV.SBR_SCENARIO);
  } else {
    selectedScenario = parsed;
  }
} else {
  const rawCatalog = open('../scenarios.json').replace(/^\uFEFF/, '');
  const catalog = JSON.parse(rawCatalog);
  selectedScenario = resolveScenario(catalog, __ENV.SBR_SCENARIO);
}
export const options = buildK6Options(selectedScenario, __ENV);

// 3. Resolve target endpoint
const targetURL = constructTargetURL(__ENV.SBR_PROXY_URL, __ENV.SBR_REQUEST_PATH);
const httpTimeout = __ENV.SBR_HTTP_TIMEOUT || '10s';

// 4. Custom metrics to distinguish overload from normal completion
const successfulRequests = new Counter('sbr_successful_requests');
const overloadRequests503 = new Counter('sbr_overload_requests_503');
const otherHttpErrors = new Counter('sbr_other_http_errors');
const transportErrors = new Counter('sbr_transport_errors');

const successRate = new Rate('sbr_success_rate');
const overloadRate503 = new Rate('sbr_overload_rate_503');

// Isolated latency trends: prevents fast 503 drops from lowering average latency
const successfulReqDuration = new Trend('sbr_successful_duration', true);
const measurementDuration = new Trend('sbr_measurement_duration', true);
const measurementSuccessfulDuration = new Trend('sbr_measurement_successful_duration', true);

let syncSignalSent = false;

/**
 * Worker iteration function executing open-loop requests according to scenario arrival rate.
 */
export default function () {
  if (!syncSignalSent) {
    syncSignalSent = true;
    if (exec.scenario.iterationInTest === 0) {
      const syncPayload = {
        event: 'sbr_workload_started',
        timestamp_ms: Date.now(),
        iteration: 0,
      };
      console.log(`[SBR_WORKLOAD_START] ${JSON.stringify(syncPayload)}`);
    }
  }

  const elapsedSeconds = exec.instance.currentTestRunDuration / 1000.0;
  const phase = getTimelinePhase(elapsedSeconds, selectedScenario.timeline);

  const tags = {
    scenario: selectedScenario.id,
    phase: phase,
    policy: __ENV.SBR_PROXY_POLICY || 'unspecified',
  };

  const params = {
    headers: {
      'Accept': 'application/json',
      'User-Agent': 'k6-workload-generator/1.0',
      'X-Scenario-ID': selectedScenario.id,
      'X-Workload-Phase': phase,
    },
    timeout: httpTimeout,
    tags: tags,
  };

  const res = http.get(targetURL, params);
  const classification = classifyResponse(res.status);

  switch (classification) {
    case 'success':
      successfulRequests.add(1, tags);
      successRate.add(1, tags);
      successfulReqDuration.add(res.timings.duration, tags);
      if (phase === 'measurement') {
        measurementSuccessfulDuration.add(res.timings.duration, tags);
      }
      break;
    case 'overload_503':
      overloadRequests503.add(1, tags);
      overloadRate503.add(1, tags);
      successRate.add(0, tags);
      break;
    case 'http_error':
      otherHttpErrors.add(1, tags);
      successRate.add(0, tags);
      break;
    default:
      transportErrors.add(1, tags);
      successRate.add(0, tags);
      break;
  }

  if (phase === 'measurement') {
    measurementDuration.add(res.timings.duration, tags);
  }
}

/**
 * Aggregates results and exports machine-readable benchmark metrics.
 *
 * @param {Object} data - k6 raw test summary metrics.
 * @returns {Object} Output mapping for stdout and optional JSON export.
 */
export function handleSummary(data) {
  const report = formatSummaryReport(data, selectedScenario, {
    policy: __ENV.SBR_PROXY_POLICY,
    proxy_url: targetURL,
  });

  const dropped = report.workload_integrity.dropped_iterations;
  let textOut = '\n' + '='.repeat(70) + '\n';
  textOut += `  SELF-BALANCING-ROUTER BENCHMARK WORKLOAD REPORT\n`;
  textOut += '='.repeat(70) + '\n';
  textOut += `  Scenario:    ${selectedScenario.id} (${selectedScenario.name})\n`;
  textOut += `  Duration:    ${selectedScenario.duration_seconds}s (Warmup: ${selectedScenario.timeline.warmup_seconds}s, Meas: ${selectedScenario.timeline.measurement_seconds}s, Cool: ${selectedScenario.timeline.cooldown_seconds}s)\n`;
  textOut += `  Target URL:  ${targetURL}\n`;
  textOut += `  Policy:      ${report.metadata.routing_policy}\n`;
  textOut += '-'.repeat(70) + '\n';
  textOut += `  Total Reqs:  ${report.workload_integrity.total_requests}\n`;
  textOut += `  Successful:  ${report.outcomes.successful_200}\n`;
  textOut += `  HTTP 503s:   ${report.outcomes.overload_503}\n`;
  textOut += `  HTTP Errors: ${report.outcomes.other_http_errors}\n`;
  textOut += `  Transport:   ${report.outcomes.transport_errors}\n`;
  textOut += `  Error Rate:  ${report.outcomes.error_rate}\n`;
  textOut += `  Dropped Iterations: ${dropped}\n`;

  if (dropped > 0) {
    textOut += '\n  [WARNING] Dropped iterations detected! The generator experienced VU starvation.\n';
    textOut += '            Increase SBR_VUS_MAX or SBR_VUS_PREALLOCATED to sustain offered load.\n';
  }

  if (report.latency_ms.successful_only) {
    const lat = report.latency_ms.successful_only;
    textOut += '-'.repeat(70) + '\n';
    textOut += `  Successful Latency (ms):\n`;
    textOut += `    p50: ${lat.med !== undefined ? lat.med.toFixed(2) : 'N/A'} | p90: ${lat.p90 !== undefined ? lat.p90.toFixed(2) : 'N/A'} | p95: ${lat.p95 !== undefined ? lat.p95.toFixed(2) : 'N/A'} | p99: ${lat.p99 !== undefined ? lat.p99.toFixed(2) : 'N/A'}\n`;
    textOut += `    min: ${lat.min !== undefined ? lat.min.toFixed(2) : 'N/A'} | avg: ${lat.avg !== undefined ? lat.avg.toFixed(2) : 'N/A'} | max: ${lat.max !== undefined ? lat.max.toFixed(2) : 'N/A'}\n`;
  }

  if (report.latency_ms.measurement_phase) {
    const mLat = report.latency_ms.measurement_phase;
    textOut += `  Measurement Phase Latency (ms):\n`;
    textOut += `    p50: ${mLat.med !== undefined ? mLat.med.toFixed(2) : 'N/A'} | p90: ${mLat.p90 !== undefined ? mLat.p90.toFixed(2) : 'N/A'} | p95: ${mLat.p95 !== undefined ? mLat.p95.toFixed(2) : 'N/A'} | p99: ${mLat.p99 !== undefined ? mLat.p99.toFixed(2) : 'N/A'}\n`;
  }

  textOut += '='.repeat(70) + '\n\n';

  const outputs = {
    stdout: textOut,
  };

  if (__ENV.SBR_SUMMARY_OUTPUT) {
    outputs[__ENV.SBR_SUMMARY_OUTPUT] = JSON.stringify(report, null, 2);
  }

  return outputs;
}
