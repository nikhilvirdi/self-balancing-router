/**
 * Scenario translation and configuration builder for Grafana k6 workload generation.
 * Consumes standardized self-balancing-router scenario specifications and generates
 * open-loop k6 scenario options, virtual user sizing, request parameters, and metric classifications.
 */

/**
 * Resolves a scenario definition from the scenario catalog.
 * Supports scenario id, case-insensitive matching, filename conventions, and numeric indices.
 *
 * @param {Object} catalog - Parsed scenarios.json catalog object.
 * @param {string} [scenarioKey] - Scenario identifier, filename, or index.
 * @returns {Object} Matching scenario object.
 * @throws {Error} If catalog is invalid or scenario identifier is unknown.
 */
export function resolveScenario(catalog, scenarioKey) {
  if (!catalog || !Array.isArray(catalog.scenarios) || catalog.scenarios.length === 0) {
    throw new Error('Invalid scenario catalog: missing or empty "scenarios" array');
  }

  const defaultKey = 'scenario_1_homogeneous';
  const rawKey = (scenarioKey !== undefined && scenarioKey !== null) ? String(scenarioKey).trim() : '';
  const key = rawKey === '' ? defaultKey : rawKey;

  // 1. Exact match by id
  let found = catalog.scenarios.find(s => s.id === key);
  if (found) return found;

  // 2. Case-insensitive match by id or name
  const lowerKey = key.toLowerCase();
  found = catalog.scenarios.find(s =>
    s.id.toLowerCase() === lowerKey || s.name.toLowerCase() === lowerKey
  );
  if (found) return found;

  // 3. Filename matching (e.g. "01_stable_homogeneous.json" -> "scenario_1_homogeneous")
  const stripped = lowerKey.replace(/\.json$/, '');
  const filePrefixMatch = stripped.match(/^0?([1-7])_(.*)$/);
  if (filePrefixMatch) {
    const num = parseInt(filePrefixMatch[1], 10);
    const suffix = filePrefixMatch[2];
    found = catalog.scenarios.find(s => s.id === `scenario_${num}_${suffix}`);
    if (found) return found;
  }

  // 4. Numeric index (1 to N)
  const idx = parseInt(key, 10);
  if (!isNaN(idx) && idx >= 1 && idx <= catalog.scenarios.length) {
    return catalog.scenarios[idx - 1];
  }

  // 5. Partial prefix match (e.g. "scenario_1")
  if (!isNaN(idx)) {
    found = catalog.scenarios.find(s => s.id.startsWith(`scenario_${idx}_`));
    if (found) return found;
  }

  const validIDs = catalog.scenarios.map(s => s.id).join(', ');
  throw new Error(`Unknown scenario identifier: "${rawKey}". Available scenarios: ${validIDs}`);
}

/**
 * Calculates virtual user (VU) preallocation and maximum bounds for an arrival-rate stage.
 *
 * @param {number} targetRPS - Target requests per second for the stage.
 * @param {Object} [envOverrides] - Environment overrides for VU settings.
 * @returns {{ preAllocatedVUs: number, maxVUs: number }}
 */
export function calculateVUs(targetRPS, envOverrides = {}) {
  if (typeof targetRPS !== 'number' || targetRPS <= 0 || isNaN(targetRPS)) {
    throw new Error(`targetRPS must be a positive number, got: ${targetRPS}`);
  }

  // Allow explicit overrides
  let preAllocOverride = null;
  let maxOverride = null;

  if (envOverrides.SBR_VUS_PREALLOCATED !== undefined && envOverrides.SBR_VUS_PREALLOCATED !== '') {
    preAllocOverride = parseInt(envOverrides.SBR_VUS_PREALLOCATED, 10);
    if (isNaN(preAllocOverride) || preAllocOverride <= 0) {
      throw new Error(`Invalid SBR_VUS_PREALLOCATED: must be a positive integer, got "${envOverrides.SBR_VUS_PREALLOCATED}"`);
    }
  }

  if (envOverrides.SBR_VUS_MAX !== undefined && envOverrides.SBR_VUS_MAX !== '') {
    maxOverride = parseInt(envOverrides.SBR_VUS_MAX, 10);
    if (isNaN(maxOverride) || maxOverride <= 0) {
      throw new Error(`Invalid SBR_VUS_MAX: must be a positive integer, got "${envOverrides.SBR_VUS_MAX}"`);
    }
  }

  if (preAllocOverride !== null && maxOverride !== null && preAllocOverride > maxOverride) {
    throw new Error(`SBR_VUS_PREALLOCATED (${preAllocOverride}) cannot exceed SBR_VUS_MAX (${maxOverride})`);
  }

  // Dynamic baseline sizing:
  // Little's Law: Concurrency = RPS * Latency
  // preAllocated: sized for ~500ms baseline response time
  // maxVUs: sized for ~2500ms tail latency to absorb queueing surges without dropping iterations
  const preAllocRatio = 0.5;
  const maxRatio = 2.5;

  const defaultPreAlloc = Math.max(5, Math.ceil(targetRPS * preAllocRatio));
  const defaultMax = Math.max(20, Math.ceil(targetRPS * maxRatio));

  const preAllocatedVUs = preAllocOverride !== null ? preAllocOverride : defaultPreAlloc;
  let maxVUs = maxOverride !== null ? maxOverride : defaultMax;

  if (maxVUs < preAllocatedVUs) {
    maxVUs = preAllocatedVUs;
  }

  return { preAllocatedVUs, maxVUs };
}

/**
 * Validates and constructs the target request URL.
 *
 * @param {string} [proxyUrl] - Proxy base URL.
 * @param {string} [requestPath] - Target application request path.
 * @returns {string} Fully qualified request URL.
 */
export function constructTargetURL(proxyUrl = 'http://127.0.0.1:8080', requestPath = '/api/workload') {
  const trimmedUrl = String(proxyUrl).trim().replace(/\/+$/, '');
  if (!/^https?:\/\//i.test(trimmedUrl)) {
    throw new Error(`Invalid SBR_PROXY_URL: must start with http:// or https://, got "${proxyUrl}"`);
  }

  const trimmedPath = String(requestPath).trim();
  const normalizedPath = trimmedPath.startsWith('/') ? trimmedPath : `/${trimmedPath}`;
  return `${trimmedUrl}${normalizedPath}`;
}

/**
 * Builds standard Grafana k6 options matching the exact stages of the scenario definition.
 *
 * @param {Object} scenario - Validated scenario object.
 * @param {Object} [envOverrides] - Environment configuration overrides.
 * @returns {Object} Grafana k6 options object.
 */
export function buildK6Options(scenario, envOverrides = {}) {
  if (!scenario || typeof scenario !== 'object') {
    throw new Error('scenario must be an object');
  }
  if (!scenario.duration_seconds || scenario.duration_seconds <= 0) {
    throw new Error(`scenario duration_seconds must be positive, got: ${scenario.duration_seconds}`);
  }
  if (!scenario.workload || !Array.isArray(scenario.workload.stages) || scenario.workload.stages.length === 0) {
    throw new Error('scenario workload must define at least one stage');
  }

  const gracefulStop = envOverrides.SBR_GRACEFUL_STOP || '5s';
  const pattern = scenario.workload.pattern || 'constant';
  const k6Scenarios = {};

  if (pattern === 'ramping') {
    // For continuous gradual ramps, use ramping-arrival-rate executor
    const startRate = scenario.workload.stages[0].target_rps;
    const maxRPS = Math.max(...scenario.workload.stages.map(st => st.target_rps));
    const vuConfig = calculateVUs(maxRPS, envOverrides);

    k6Scenarios['ramping_workload'] = {
      executor: 'ramping-arrival-rate',
      startRate: startRate,
      timeUnit: '1s',
      stages: scenario.workload.stages.map(st => ({
        target: st.target_rps,
        duration: `${st.duration_seconds}s`,
      })),
      preAllocatedVUs: vuConfig.preAllocatedVUs,
      maxVUs: vuConfig.maxVUs,
      gracefulStop: gracefulStop,
    };
  } else {
    // For constant, stepped, bursty, and cyclic patterns:
    // Map each discrete stage to an independent constant-arrival-rate executor with exact startTime.
    // This preserves exact step-change transitions without accidental interpolation.
    scenario.workload.stages.forEach((stage, idx) => {
      const vuConfig = calculateVUs(stage.target_rps, envOverrides);
      k6Scenarios[`stage_${idx}`] = {
        executor: 'constant-arrival-rate',
        rate: stage.target_rps,
        timeUnit: '1s',
        duration: `${stage.duration_seconds}s`,
        startTime: `${stage.start_seconds}s`,
        gracefulStop: gracefulStop,
        preAllocatedVUs: vuConfig.preAllocatedVUs,
        maxVUs: vuConfig.maxVUs,
        tags: {
          scenario_id: scenario.id,
          stage_index: `${idx}`,
          target_rps: `${stage.target_rps}`,
        },
      };
    });
  }

  return {
    scenarios: k6Scenarios,
    discardResponseBodies: false,
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  };
}

/**
 * Determines current timeline phase (warmup, measurement, cooldown) from elapsed test run duration.
 *
 * @param {number} elapsedSeconds - Elapsed time from test start in seconds.
 * @param {Object} timeline - Scenario timeline object.
 * @returns {'warmup' | 'measurement' | 'cooldown'} Current phase identifier.
 */
export function getTimelinePhase(elapsedSeconds, timeline) {
  if (!timeline) return 'measurement';
  const warmup = timeline.warmup_seconds || 0;
  const measurement = timeline.measurement_seconds || 0;

  if (elapsedSeconds < warmup) {
    return 'warmup';
  }
  if (elapsedSeconds < (warmup + measurement)) {
    return 'measurement';
  }
  return 'cooldown';
}

/**
 * Classifies an HTTP response outcome into metric categories.
 *
 * @param {number} status - HTTP status code (0 for transport errors).
 * @returns {'success' | 'overload_503' | 'http_error' | 'transport_error'}
 */
export function classifyResponse(status) {
  if (status === 200) {
    return 'success';
  }
  if (status === 503) {
    return 'overload_503';
  }
  if (status > 0) {
    return 'http_error';
  }
  return 'transport_error';
}

/**
 * Safely extracts backend identity from an HTTP response if available.
 *
 * @param {number} status - HTTP response status code.
 * @param {string|Object} [body] - Response body string or parsed JSON object.
 * @returns {string} Backend ID string, or empty string if unobservable.
 */
export function extractBackendID(status, body) {
  if (status !== 200 || !body) {
    return '';
  }
  try {
    const parsed = (typeof body === 'string') ? JSON.parse(body) : body;
    if (parsed && typeof parsed.backend_id === 'string') {
      return parsed.backend_id;
    }
  } catch (_) {}
  return '';
}

/**
 * Formats a comprehensive machine-readable summary report from k6 execution data.
 *
 * @param {Object} data - Raw k6 summary data from handleSummary.
 * @param {Object} scenario - Executed scenario configuration.
 * @param {Object} [metadata] - Additional run metadata (policy, proxy URL, etc.).
 * @returns {Object} Structured benchmark report.
 */
export function formatSummaryReport(data, scenario, metadata = {}) {
  const getMetric = (name) => (data.metrics && data.metrics[name]) ? data.metrics[name].values : null;

  const httpReqs = getMetric('http_reqs');
  const duration = getMetric('http_req_duration');
  const successDuration = getMetric('sbr_successful_duration');
  const measurementDuration = getMetric('sbr_measurement_duration');
  const droppedIters = getMetric('dropped_iterations');

  const totalReqs = httpReqs ? (httpReqs.count || 0) : 0;
  const droppedCount = droppedIters ? (droppedIters.count || 0) : 0;

  const successReqs = getMetric('sbr_successful_requests');
  const overloadReqs = getMetric('sbr_overload_requests_503');
  const httpErrReqs = getMetric('sbr_other_http_errors');
  const transportErrReqs = getMetric('sbr_transport_errors');

  return {
    metadata: {
      generator: 'self-balancing-router-k6-workload-generator',
      scenario_id: scenario.id,
      scenario_name: scenario.name,
      routing_policy: metadata.policy || 'unspecified',
      proxy_url: metadata.proxy_url || 'http://127.0.0.1:8080',
      total_duration_seconds: scenario.duration_seconds,
      timeline: scenario.timeline,
      stages_count: scenario.workload.stages.length,
    },
    workload_integrity: {
      total_requests: totalReqs,
      dropped_iterations: droppedCount,
      valid_offered_load: droppedCount === 0,
      observed_rps: httpReqs ? httpReqs.rate : 0,
    },
    outcomes: {
      successful_200: successReqs ? (successReqs.count || 0) : 0,
      overload_503: overloadReqs ? (overloadReqs.count || 0) : 0,
      other_http_errors: httpErrReqs ? (httpErrReqs.count || 0) : 0,
      transport_errors: transportErrReqs ? (transportErrReqs.count || 0) : 0,
      error_rate: totalReqs > 0 ? (((totalReqs - (successReqs ? successReqs.count : 0)) / totalReqs) * 100).toFixed(2) + '%' : '0%',
    },
    latency_ms: {
      overall: duration ? {
        min: duration.min,
        med: duration.med,
        p90: duration['p(90)'],
        p95: duration['p(95)'],
        p99: duration['p(99)'],
        max: duration.max,
        avg: duration.avg,
      } : null,
      successful_only: successDuration ? {
        min: successDuration.min,
        med: successDuration.med,
        p90: successDuration['p(90)'],
        p95: successDuration['p(95)'],
        p99: successDuration['p(99)'],
        max: successDuration.max,
        avg: successDuration.avg,
      } : null,
      measurement_phase: measurementDuration ? {
        min: measurementDuration.min,
        med: measurementDuration.med,
        p90: measurementDuration['p(90)'],
        p95: measurementDuration['p(95)'],
        p99: measurementDuration['p(99)'],
        max: measurementDuration.max,
        avg: measurementDuration.avg,
      } : null,
    },
  };
}

