/**
 * Unit and domain validation tests for Grafana k6 workload generator configuration.
 * Executable directly via standard Node.js test runner: `node --test experiments/k6/config_test.js`
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  resolveScenario,
  calculateVUs,
  constructTargetURL,
  buildK6Options,
  getTimelinePhase,
  classifyResponse,
  formatSummaryReport,
} from './config.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const catalogPath = path.resolve(__dirname, '..', 'scenarios.json');
const catalog = JSON.parse(fs.readFileSync(catalogPath, 'utf8'));

describe('k6 Workload Generator Validation Suite', () => {
  // 1. Loading all seven existing scenarios
  test('1. Loading all seven existing scenarios by ID, filename, and index', () => {
    assert.equal(catalog.scenarios.length, 7, 'Catalog must contain 7 scenarios');

    const expectedIDs = [
      'scenario_1_homogeneous',
      'scenario_2_heterogeneous',
      'scenario_3_sudden_degradation',
      'scenario_4_gradual_degradation',
      'scenario_5_backend_recovery',
      'scenario_6_traffic_bursts',
      'scenario_7_repeated_performance_changes',
    ];

    expectedIDs.forEach((id, idx) => {
      // By exact id
      const byId = resolveScenario(catalog, id);
      assert.equal(byId.id, id);

      // By index (1-based)
      const byIdx = resolveScenario(catalog, String(idx + 1));
      assert.equal(byIdx.id, id);

      // By filename-like string
      const fileNames = [
        `0${idx + 1}_${id.replace(/^scenario_\d+_/, '')}.json`,
        `0${idx + 1}_${id.replace(/^scenario_\d+_/, '')}`,
      ];
      fileNames.forEach(fn => {
        const byFile = resolveScenario(catalog, fn);
        assert.equal(byFile.id, id);
      });
    });
  });

  // 2. Invalid or missing scenario identifiers
  test('2. Missing scenario defaults to scenario_1; invalid keys throw descriptive error', () => {
    const defaultScenario = resolveScenario(catalog);
    assert.equal(defaultScenario.id, 'scenario_1_homogeneous');

    const emptyScenario = resolveScenario(catalog, '');
    assert.equal(emptyScenario.id, 'scenario_1_homogeneous');

    assert.throws(
      () => resolveScenario(catalog, 'non_existent_scenario_12345'),
      /Unknown scenario identifier: "non_existent_scenario_12345"/
    );
  });

  // 3. Correct translation of request-rate stages
  test('3. Correct translation of request-rate stages into k6 options', () => {
    const scenario = resolveScenario(catalog, 'scenario_1_homogeneous');
    const options = buildK6Options(scenario);

    assert.ok(options.scenarios);
    const stageKeys = Object.keys(options.scenarios);
    assert.equal(stageKeys.length, scenario.workload.stages.length);

    scenario.workload.stages.forEach((stage, idx) => {
      const k6Stage = options.scenarios[`stage_${idx}`];
      assert.equal(k6Stage.executor, 'constant-arrival-rate');
      assert.equal(k6Stage.rate, stage.target_rps);
      assert.equal(k6Stage.timeUnit, '1s');
      assert.equal(k6Stage.duration, `${stage.duration_seconds}s`);
      assert.equal(k6Stage.startTime, `${stage.start_seconds}s`);
      assert.ok(k6Stage.preAllocatedVUs > 0);
      assert.ok(k6Stage.maxVUs >= k6Stage.preAllocatedVUs);
    });
  });

  // 4. Accurate total workload duration
  test('4. Accurate total workload duration matching scenario specification', () => {
    catalog.scenarios.forEach(sc => {
      const options = buildK6Options(sc);
      let calculatedTotal = 0;
      Object.values(options.scenarios).forEach(stage => {
        const dur = parseFloat(stage.duration.replace('s', ''));
        calculatedTotal += dur;
      });
      assert.equal(
        calculatedTotal,
        sc.duration_seconds,
        `Scenario ${sc.id} stages duration sum must match duration_seconds`
      );
    });
  });

  // 5. Stable arrival-rate schedules
  test('5. Stable arrival-rate schedules with positive rates and durations', () => {
    catalog.scenarios.forEach(sc => {
      const options = buildK6Options(sc);
      Object.entries(options.scenarios).forEach(([name, stage]) => {
        assert.ok(stage.rate > 0, `${sc.id} ${name} rate must be positive`);
        const dur = parseFloat(stage.duration.replace('s', ''));
        assert.ok(dur > 0, `${sc.id} ${name} duration must be positive`);
      });
    });
  });

  // 6. Correct distinction between step changes and gradual ramps
  test('6. Step changes use discrete constant-arrival-rate stages without interpolation', () => {
    const burstScenario = resolveScenario(catalog, 'scenario_6_traffic_bursts');
    const options = buildK6Options(burstScenario);

    // Burst 1 jumps from 50 RPS (stage 1) to 160 RPS (stage 2) at t=30s
    const stage1 = options.scenarios['stage_1'];
    const stage2 = options.scenarios['stage_2'];

    assert.equal(stage1.executor, 'constant-arrival-rate');
    assert.equal(stage1.rate, 50);
    assert.equal(stage1.startTime, '15s');
    assert.equal(stage1.duration, '15s');

    assert.equal(stage2.executor, 'constant-arrival-rate');
    assert.equal(stage2.rate, 160);
    assert.equal(stage2.startTime, '30s'); // Instant step at t=30s
    assert.equal(stage2.duration, '15s');

    // Test hypothetical ramping pattern
    const rampingScenario = {
      id: 'ramping_test',
      duration_seconds: 60,
      timeline: { warmup_seconds: 10, measurement_seconds: 40, cooldown_seconds: 10 },
      workload: {
        pattern: 'ramping',
        stages: [
          { start_seconds: 0, duration_seconds: 30, target_rps: 50 },
          { start_seconds: 30, duration_seconds: 30, target_rps: 150 },
        ],
      },
    };
    const rampingOptions = buildK6Options(rampingScenario);
    assert.equal(rampingOptions.scenarios.ramping_workload.executor, 'ramping-arrival-rate');
  });

  // 7. Warm-up, measurement, and cooldown boundaries
  test('7. Accurate timeline phase transitions', () => {
    const timeline = { warmup_seconds: 15, measurement_seconds: 65, cooldown_seconds: 10 };
    // Warmup: [0, 15)
    assert.equal(getTimelinePhase(0, timeline), 'warmup');
    assert.equal(getTimelinePhase(14.99, timeline), 'warmup');

    // Measurement: [15, 80)
    assert.equal(getTimelinePhase(15.0, timeline), 'measurement');
    assert.equal(getTimelinePhase(50.0, timeline), 'measurement');
    assert.equal(getTimelinePhase(79.99, timeline), 'measurement');

    // Cooldown: [80, 90]
    assert.equal(getTimelinePhase(80.0, timeline), 'cooldown');
    assert.equal(getTimelinePhase(89.0, timeline), 'cooldown');
  });

  // 8. Valid request URL construction
  test('8. Request URL construction handles custom schemes, ports, and paths', () => {
    assert.equal(
      constructTargetURL('http://127.0.0.1:8080', '/api/workload'),
      'http://127.0.0.1:8080/api/workload'
    );
    assert.equal(
      constructTargetURL('http://127.0.0.1:8080/', 'api/workload'),
      'http://127.0.0.1:8080/api/workload'
    );
    assert.equal(
      constructTargetURL('https://proxy.internal:9443', '/custom/endpoint'),
      'https://proxy.internal:9443/custom/endpoint'
    );
  });

  // 9. Consistent configuration across routing policies
  test('9. Routing policy tagging remains independent of workload generation', () => {
    const scenario = resolveScenario(catalog, 'scenario_2_heterogeneous');
    const optRR = buildK6Options(scenario, { SBR_PROXY_POLICY: 'round-robin' });
    const optP2C = buildK6Options(scenario, { SBR_PROXY_POLICY: 'p2c' });
    const optUCB = buildK6Options(scenario, { SBR_PROXY_POLICY: 'ucb1' });

    assert.deepEqual(optRR, optP2C);
    assert.deepEqual(optP2C, optUCB);
  });

  // 10. Invalid proxy URLs and VU allocation settings
  test('10. Validation detects invalid proxy URLs and VU settings', () => {
    assert.throws(() => constructTargetURL('ftp://invalid-scheme'), /must start with http:\/\/ or https:\/\//);
    assert.throws(() => calculateVUs(-10), /targetRPS must be a positive number/);
    assert.throws(
      () => calculateVUs(100, { SBR_VUS_PREALLOCATED: '-5' }),
      /Invalid SBR_VUS_PREALLOCATED/
    );
    assert.throws(
      () => calculateVUs(100, { SBR_VUS_PREALLOCATED: '200', SBR_VUS_MAX: '50' }),
      /cannot exceed SBR_VUS_MAX/
    );
  });

  // 11. Correct metric classification for successful and failed responses
  test('11. Metric classification distinguishes HTTP 200, 503, other HTTP errors, and transport errors', () => {
    assert.equal(classifyResponse(200), 'success');
    assert.equal(classifyResponse(503), 'overload_503');
    assert.equal(classifyResponse(500), 'http_error');
    assert.equal(classifyResponse(404), 'http_error');
    assert.equal(classifyResponse(502), 'http_error');
    assert.equal(classifyResponse(0), 'transport_error');
  });

  // 12. Compatibility with existing JSON scenario validation
  test('12. Scenarios loaded by config.js match existing catalog parameters exactly', () => {
    catalog.scenarios.forEach(sc => {
      const resolved = resolveScenario(catalog, sc.id);
      assert.equal(resolved.id, sc.id);
      assert.equal(resolved.duration_seconds, sc.duration_seconds);
      assert.equal(resolved.timeline.warmup_seconds, sc.timeline.warmup_seconds);
      assert.equal(resolved.timeline.measurement_seconds, sc.timeline.measurement_seconds);
      assert.equal(resolved.timeline.cooldown_seconds, sc.timeline.cooldown_seconds);
      assert.equal(resolved.workload.stages.length, sc.workload.stages.length);
    });
  });
});

