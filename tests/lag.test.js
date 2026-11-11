import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm, writeFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {setup, source, row, batch, code} from './helpers.js';
import {listen} from '../src/http.js';
import {execute} from '../src/cli.js';
import {
  pearson, spearman, averageRanks, holmAdjust, lagAnalysis, DEFAULT_LAG_SEED,
} from '../src/lag.js';

const HOUR = 3600000;
const ORIGIN = '2026-11-01T00:00:00Z';

async function metricsLedger(t) {
  const {ledger} = await setup(t);
  await ledger.registerSource(source({id: 'sm', metrics: ['rainfall', 'soil_moisture']}));
  const state = await ledger.store.read();
  await ledger.registerStation({...state.stations.p1, id: 'pm', sources: ['sm']});
  return ledger;
}

function hour(value) {
  return `2026-11-01T${String(value).padStart(2, '0')}:00:00Z`;
}

function records(pairs, overrides = {}) {
  return pairs.map(([hourIndex, metric, value, rest = {}]) =>
    row({station: 'pm', time: hour(hourIndex), metric, value, unit: metric === 'rainfall' ? 'mm' : 'pct',
      ...rest, ...overrides}));
}

function hourly(rainfall, moisture, overrides = {}) {
  const pairs = [];
  rainfall.forEach((value, index) => pairs.push([index, 'rainfall', value]));
  moisture.forEach((value, index) => pairs.push([index, 'soil_moisture', value]));
  return records(pairs, overrides);
}

async function ingest(ledger, rows, batchId = 'b1', sourceId = 'sm') {
  return ledger.ingest(batch(rows, {
    id: batchId, source: sourceId, receivedAt: '2026-11-02T00:00:00Z',
  }));
}

function lagRequest(overrides = {}) {
  return {
    from: ORIGIN,
    until: hour(8),
    grid: {widthMs: HOUR},
    left: {station: 'pm', metric: 'rainfall'},
    right: {station: 'pm', metric: 'soil_moisture'},
    lags: [-1, 0, 1],
    seed: 20261101,
    permutations: 499,
    ...overrides,
  };
}

async function analysis(ledger, overrides = {}) {
  return lagAnalysis(await ledger.store.read(), lagRequest(overrides));
}

function byLag(report, lag) {
  return report.results.find(item => item.lag === lag);
}

test('positive lag means right data are later than left and a genuine one-hour lead is visible', async t => {
  const ledger = await metricsLedger(t);
  const rainfall = [4, 11, 2, 9, 6, 13, 1, 8, 12, 5, 10, 3];
  const moisture = [33, ...rainfall.slice(0, 11).map(value => 90 - 4 * value)];
  await ingest(ledger, hourly(rainfall, moisture));
  const report = lagAnalysis(await ledger.store.read(), {
    ...lagRequest(), from: ORIGIN, until: hour(12), lags: [-1, 0, 1], permutations: 1999,
  });
  assert.equal(report.head.length, 64);
  assert.equal(report.lags.join(','), '-1,0,1');
  const leading = byLag(report, 1);
  assert.equal(leading.estimable, true);
  assert.equal(leading.sampleSize, 11);
  assert.ok(Math.abs(leading.pearson + 1) < 1e-12);
  assert.ok(Math.abs(leading.spearman + 1) < 1e-12);
  assert.ok(leading.significantPearson);
  assert.ok(leading.significantSpearman);
  const simultaneous = byLag(report, 0);
  assert.equal(simultaneous.estimable, true);
  assert.ok(Math.abs(simultaneous.pearson) < 0.9);
  assert.notEqual(simultaneous.adjustedPearsonP, null);
  const trailing = byLag(report, -1);
  assert.equal(trailing.estimable, true);
  assert.ok(Math.abs(trailing.pearson) < 0.9);
  assert.equal(leading.excluded.paired, leading.sampleSize);
  assert.equal(report.caveats.some(item => item.code === 'association_not_causation'), true);
});

test('each result can be traced back to the actual paired values and observation revisions', async t => {
  const ledger = await metricsLedger(t);
  await ingest(ledger, hourly([1, 2, 3, 4, 5, 6, 7, 8], [8, 7, 6, 5, 4, 3, 2, 1]));
  const report = await analysis(ledger, {lags: [0]});
  const result = byLag(report, 0);
  assert.equal(result.pairs.length, 8);
  const first = result.pairs[0];
  assert.equal(first.gridIndex, 0);
  assert.equal(first.left.value, 1);
  assert.equal(first.right.value, 8);
  assert.equal(first.left.lineage[0].revision, 1);
  assert.match(first.left.lineage[0].key, /sm\|pm\|rainfall/);
  assert.match(first.right.lineage[0].key, /sm\|pm\|soil_moisture/);
  const firstTime = new Date(first.left.from).getTime();
  assert.equal(new Date(result.pairs[1].left.from).getTime() - firstTime, HOUR);
});

test('shared trends inflate every lag while a true lag isolates one peak', async t => {
  const ledger = await metricsLedger(t);
  await ingest(ledger, hourly([1, 2, 3, 4, 5, 6, 7, 8], [1, 2, 3, 4, 5, 6, 7, 8]));
  const trending = await analysis(ledger, {lags: [0, 1]});
  for (const result of trending.results) {
    assert.equal(result.estimable, true);
    assert.ok(Math.abs(result.pearson - 1) < 1e-12);
  }
  const ledgerB = await metricsLedger(t);
  const rainB = [4, 11, 2, 9, 6, 13, 1, 8, 12, 5, 10, 3];
  const moistB = [33, ...rainB.slice(0, 11).map(value => 90 - 4 * value)];
  await ingest(ledgerB, hourly(rainB, moistB));
  const causal = lagAnalysis(await ledgerB.store.read(), {
    ...lagRequest(), from: ORIGIN, until: hour(12), lags: [-1, 0, 1], permutations: 1999,
  });
  const peaks = causal.results.filter(item => Math.abs(item.pearson) > 0.99);
  assert.deepEqual(peaks.map(item => item.lag), [1]);
});

test('missing cells are excluded pairwise without zero fill, forward fill, or invented rows', async t => {
  const ledger = await metricsLedger(t);
  const rainfall = [1, null, 3, 4, 5, 6, 7, 8];
  const moisture = [8, 7, null, 5, 4, 3, 2, 1];
  const pairs = [];
  rainfall.forEach((value, index) => value !== null && pairs.push([index, 'rainfall', value]));
  moisture.forEach((value, index) => value !== null && pairs.push([index, 'soil_moisture', value]));
  await ingest(ledger, records(pairs));
  const report = await analysis(ledger, {lags: [0]});
  const result = byLag(report, 0);
  assert.equal(result.sampleSize, 6);
  assert.equal(result.excluded.paired, 6);
  assert.equal(result.excluded.leftMissing, 1);
  assert.equal(result.excluded.rightMissing, 1);
  assert.equal(result.excluded.candidateCells, 8);
  assert.equal(result.pairs.some(pair => pair.left.value === 0), false);
  assert.equal(result.pairs.some(pair => pair.right.value === 0), false);
  assert.equal(result.pairs.map(pair => pair.gridIndex).join(','), '0,3,4,5,6,7');
});

test('both sides missing is reported separately from one-sided gaps', async t => {
  const ledger = await metricsLedger(t);
  const rainfall = [null, 2, 3, 4, 5, 6, 7, 8];
  const moisture = [null, 7, 6, 5, 4, 3, 2, 1];
  const pairs = [];
  rainfall.forEach((value, index) => value !== null && pairs.push([index, 'rainfall', value]));
  moisture.forEach((value, index) => value !== null && pairs.push([index, 'soil_moisture', value]));
  await ingest(ledger, records(pairs));
  const report = await analysis(ledger, {lags: [0, 1]});
  const zero = byLag(report, 0);
  assert.equal(zero.excluded.bothMissing, 1);
  assert.equal(zero.sampleSize, 7);
  const shifted = byLag(report, 1);
  assert.equal(shifted.excluded.bothMissing, 0);
  assert.equal(shifted.excluded.rightMissing, 0);
  assert.equal(shifted.excluded.leftMissing, 1);
  assert.equal(shifted.sampleSize, 6);
});

test('constant series and insufficient samples are marked not estimable', async t => {
  const ledger = await metricsLedger(t);
  await ingest(ledger, hourly([1, 2, 3, 4, 5, 6, 7, 8], [5, 5, 5, 5, 5, 5, 5, 5]));
  const report = await analysis(ledger, {lags: [0]});
  const result = byLag(report, 0);
  assert.equal(result.estimable, false);
  assert.equal(result.reason, 'constant_series');
  assert.equal(result.constantSide, 'right');
  assert.equal(result.pearson, null);
  assert.equal(result.spearman, null);
  assert.equal(result.pearsonP, null);
  assert.equal(result.adjustedPearsonP, null);
  assert.equal(result.significantPearson, null);
  assert.equal(report.familySize, 0);
  const sparse = await analysis(ledger, {lags: [0], minimumCount: 9});
  assert.equal(byLag(sparse, 0).reason, 'insufficient_samples');
  assert.equal(byLag(sparse, 0).estimable, false);
});

test('empty grid and two constant sides are reported without fabricated values', async t => {
  const ledger = await metricsLedger(t);
  const empty = await analysis(ledger, {lags: [0, 1]});
  for (const result of empty.results) {
    assert.equal(result.estimable, false);
    assert.equal(result.reason, 'insufficient_samples');
    assert.equal(result.sampleSize, 0);
    assert.equal(result.pearson, null);
    assert.equal(result.excluded.paired, 0);
  }
  assert.equal(empty.left.cells, 0);
  assert.equal(empty.right.cells, 0);
  assert.equal(empty.familySize, 0);
  await ingest(ledger, hourly([4, 4, 4, 4], [9, 9, 9, 9]));
  const flat = await analysis(ledger, {lags: [0]});
  const result = byLag(flat, 0);
  assert.equal(result.estimable, false);
  assert.equal(result.reason, 'constant_series');
  assert.equal(result.constantSide, 'both');
  assert.equal(result.ties.left.largestTie, 4);
  assert.equal(result.ties.right.tieGroups, 1);
});

test('Spearman uses average ranks for ties and tolerates nonlinear monotone data', () => {
  assert.deepEqual(averageRanks([3, 1, 1, 2]), [4, 1.5, 1.5, 3]);
  assert.deepEqual(averageRanks([5, 5, 5]), [2, 2, 2]);
  const x = [1, 2, 3, 4, 5];
  const y = [1, 4, 9, 16, 25];
  assert.equal(spearman(x, y), 1);
  const repeated = spearman([1, 1, 2, 3, 4], [1, 1, 2, 3, 4]);
  assert.ok(Math.abs(repeated - 1) < 1e-12);
  assert.equal(pearson([1], [1]), null);
});

test('tie diagnostics show where ranks had to be averaged', async t => {
  const ledger = await metricsLedger(t);
  await ingest(ledger, hourly([10, 0, 0, 20, 0, 0, 0, 5], [30, 22, 20, 20, 24, 20, 20, 20]));
  const report = await analysis(ledger, {lags: [1]});
  const result = byLag(report, 1);
  assert.ok(result.ties.left.tieGroups >= 1);
  assert.ok(result.ties.right.largestTie >= 2);
});

test('preferred selection collapses duplicate sources so a cell is never counted twice', async t => {
  const ledger = await metricsLedger(t);
  await ledger.registerSource(source({id: 's2', priority: 1, metrics: ['rainfall', 'soil_moisture']}));
  const state = await ledger.store.read();
  await ledger.registerStation({...state.stations.p1, id: 'p2', sources: ['sm', 's2']});
  const base = hourly([1, 2, 3, 4, 5, 6, 7, 8], [8, 7, 6, 5, 4, 3, 2, 1], {station: 'p2'});
  await ledger.ingest(batch(base, {source: 'sm'}));
  const duplicate = hourly([5, 6, 7, 8, 9, 10, 11, 12], [1, 1, 1, 1, 1, 1, 1, 1], {station: 'p2'});
  await ledger.ingest(batch(duplicate, {id: 'b2', source: 's2'}));
  const report = lagAnalysis(await ledger.store.read(), {
    ...lagRequest(), left: {station: 'p2', metric: 'rainfall'}, right: {station: 'p2', metric: 'soil_moisture'}, lags: [0],
  });
  const result = byLag(report, 0);
  assert.equal(result.pairs[0].left.value, 5);
  assert.equal(result.pairs[0].left.observations, 1);
  assert.equal(result.pairs[0].left.lineage.length, 1);
  const all = lagAnalysis(await ledger.store.read(), {
    ...lagRequest(), left: {station: 'p2', metric: 'rainfall', selection: 'all'},
    right: {station: 'p2', metric: 'soil_moisture', selection: 'all'}, lags: [0],
  });
  assert.equal(byLag(all, 0).pairs[0].left.observations, 2);
});

test('each side aggregates into the shared grid with its own rule', async t => {
  const ledger = await metricsLedger(t);
  const detail = [
    row({station: 'pm', metric: 'rainfall', time: '2026-11-01T00:00:00Z', value: 3, unit: 'mm'}),
    row({station: 'pm', metric: 'rainfall', time: '2026-11-01T00:30:00Z', value: 5, unit: 'mm'}),
    row({station: 'pm', metric: 'soil_moisture', time: '2026-11-01T00:00:00Z', value: 20, unit: 'pct'}),
    row({station: 'pm', metric: 'soil_moisture', time: '2026-11-01T00:30:00Z', value: 40, unit: 'pct'}),
    row({station: 'pm', metric: 'rainfall', time: '2026-11-01T01:00:00Z', value: 1, unit: 'mm'}),
    row({station: 'pm', metric: 'soil_moisture', time: '2026-11-01T01:00:00Z', value: 60, unit: 'pct'}),
  ];
  await ingest(ledger, detail);
  const report = await analysis(ledger, {
    lags: [0],
    left: {station: 'pm', metric: 'rainfall', aggregation: 'sum', selection: 'all'},
    right: {station: 'pm', metric: 'soil_moisture', aggregation: 'mean', selection: 'all'},
  });
  const first = byLag(report, 0).pairs[0];
  assert.equal(first.left.value, 8);
  assert.equal(first.left.observations, 2);
  assert.equal(first.right.value, 30);
  assert.equal(first.right.observations, 2);
  assert.equal(report.left.aggregation, 'sum');
  assert.equal(report.right.aggregation, 'mean');
});

test('block permutation keeps block order, is seeded, and feeds one Holm family', async t => {
  const ledger = await metricsLedger(t);
  await ingest(ledger, hourly([10, 0, 0, 20, 0, 0, 0, 5], [30, 22, 20, 20, 24, 20, 20, 20]));
  const first = await analysis(ledger, {permutations: 99, blockCells: 2, seed: 7});
  const second = await analysis(ledger, {permutations: 99, blockCells: 2, seed: 7});
  assert.deepEqual(first.results.map(item => item.pearsonP), second.results.map(item => item.pearsonP));
  const other = await analysis(ledger, {permutations: 99, blockCells: 2, seed: 8});
  assert.notDeepEqual(first.results.map(item => item.pearsonP), other.results.map(item => item.pearsonP));
  const leading = byLag(first, 1);
  assert.equal(leading.blocks, Math.ceil(leading.sampleSize / 2));
  assert.ok(leading.pearsonP > 0 && leading.pearsonP <= 1);
  assert.ok(leading.adjustedPearsonP >= leading.pearsonP);
  assert.equal(first.blocking, 'paired_sample_grid_order');
  assert.equal(first.familySize, 3);
});

test('permutations zero and too few blocks suppress p-values without invalidating coefficients', async t => {
  const ledger = await metricsLedger(t);
  await ingest(ledger, hourly([10, 0, 0, 20, 0, 0, 0, 5], [30, 22, 20, 20, 24, 20, 20, 20]));
  const disabled = await analysis(ledger, {permutations: 0});
  const result = byLag(disabled, 1);
  assert.equal(result.estimable, true);
  assert.equal(result.pearsonP, null);
  assert.equal(result.permutationReason, 'permutations_disabled');
  assert.equal(result.adjustedPearsonP, null);
  assert.equal(disabled.familySize, 0);
  const blocked = await analysis(ledger, {blockCells: 8});
  assert.equal(byLag(blocked, 1).permutationReason, 'insufficient_blocks');
  assert.equal(byLag(blocked, 1).pearsonP, null);
});

test('Holm adjustment rejects the smallest raw p-value while spreading the penalty', () => {
  const adjusted = holmAdjust([
    {lag: 0, p: 0.01}, {lag: 1, p: 0.02}, {lag: 2, p: 0.03},
  ]);
  assert.equal(adjusted.get(0), 0.03);
  assert.equal(adjusted.get(1), 0.04);
  assert.equal(adjusted.get(2), 0.04);
  const capped = holmAdjust([{lag: 0, p: 0.9}]);
  assert.equal(capped.get(0), 0.9);
});

test('analysis is bound to observation version and changes when data are revised', async t => {
  const ledger = await metricsLedger(t);
  await ingest(ledger, hourly([1, 2, 3, 4, 5, 6, 7, 8], [8, 7, 6, 5, 4, 3, 2, 1]));
  const before = await analysis(ledger, {lags: [0]});
  assert.equal(before.sequence, (await ledger.status()).sequence);
  await ledger.ingest(batch([row({station: 'pm', metric: 'rainfall', value: 8, revision: 2})], {id: 'b2', source: 'sm'}));
  const after = await analysis(ledger, {lags: [0]});
  assert.notEqual(after.head, before.head);
  assert.notEqual(after.fingerprint, before.fingerprint);
  assert.notEqual(byLag(after, 0).pearson, byLag(before, 0).pearson);
  assert.equal(byLag(after, 0).pairs[0].left.lineage[0].revision, 2);
});

test('grid origin, suspect filtering and source pinning follow the side rules', async t => {
  const ledger = await metricsLedger(t);
  const pairs = [
    [0, 'rainfall', 1, {quality: 'suspect'}],
    [1, 'rainfall', 2],
    [0, 'soil_moisture', 40],
    [1, 'soil_moisture', 30],
  ];
  await ingest(ledger, records(pairs));
  const hidden = await analysis(ledger, {lags: [0]});
  assert.equal(byLag(hidden, 0).excluded.leftMissing, 1);
  const shown = await analysis(ledger, {
    lags: [0],
    left: {station: 'pm', metric: 'rainfall', includeSuspect: true},
    right: {station: 'pm', metric: 'soil_moisture'},
  });
  assert.equal(byLag(shown, 0).sampleSize, 2);
  const originShifted = await analysis(ledger, {
    grid: {widthMs: HOUR, origin: '2026-10-31T23:00:00Z'},
    lags: [0],
    left: {station: 'pm', metric: 'rainfall', includeSuspect: true},
    right: {station: 'pm', metric: 'soil_moisture'},
  });
  assert.equal(originShifted.grid.span, 9);
  assert.equal(byLag(originShifted, 0).pairs[0].gridIndex, 1);
  const pinned = await analysis(ledger, {
    lags: [0],
    left: {station: 'pm', metric: 'rainfall', source: 'sm', includeSuspect: true},
    right: {station: 'pm', metric: 'soil_moisture', source: 'sm'},
  });
  assert.equal(pinned.left.source, 'sm');
});

test('input validation rejects unknown fields, duplicate lags and excessive workloads', async t => {
  const ledger = await metricsLedger(t);
  const state = await ledger.store.read();
  assert.throws(() => lagAnalysis(state, lagRequest({unexpected: true})), code('UNKNOWN_FIELD'));
  assert.throws(() => lagAnalysis(state, lagRequest({lags: [0, 0]})), code('DUPLICATE'));
  assert.throws(() => lagAnalysis(state, lagRequest({lags: [99]})), code('LAG_RANGE'));
  assert.throws(() => lagAnalysis(state, lagRequest({minimumCount: 1})), code('INVALID_INTEGER'));
  assert.throws(() => lagAnalysis(state, lagRequest({permutations: 10001})), code('INVALID_INTEGER'));
  assert.throws(() => lagAnalysis(state, lagRequest({alpha: 2})), code('INVALID_NUMBER'));
  assert.throws(() => lagAnalysis(state, lagRequest({
    grid: {widthMs: HOUR, origin: hour(1)},
  })), code('GRID_ORIGIN'));
});

test('unknown station, missing side and reversed period are rejected', async t => {
  const ledger = await metricsLedger(t);
  const state = await ledger.store.read();
  assert.throws(() => lagAnalysis(state, lagRequest({
    left: {station: 'missing', metric: 'rainfall'},
  })), code('STATION_MISSING'));
  assert.throws(() => lagAnalysis(state, {
    from: hour(8), until: ORIGIN, grid: {widthMs: HOUR},
    left: {station: 'pm', metric: 'rainfall'}, right: {station: 'pm', metric: 'soil_moisture'},
    lags: [0],
  }), code('PERIOD'));
});

test('HTTP and CLI expose the lag analysis with the same deterministic output', async t => {
  const ledger = await metricsLedger(t);
  await ingest(ledger, hourly([10, 0, 0, 20, 0, 0, 0, 5], [30, 22, 20, 20, 24, 20, 20, 20]));
  const server = await listen(ledger, {port: 0});
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const response = await fetch(base + '/lag-analysis', {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify(lagRequest()),
  });
  assert.equal(response.status, 200);
  const served = await response.json();
  assert.equal(served.head.length, 64);
  const direct = await analysis(ledger);
  assert.equal(served.fingerprint, direct.fingerprint);
  const directory = await mkdtemp(path.join(os.tmpdir(), 'field-lag-'));
  t.after(() => rm(directory, {recursive: true, force: true}));
  const inputPath = path.join(directory, 'lag.json');
  await writeFile(inputPath, JSON.stringify(lagRequest()));
  const cliResult = await execute(['lag', '--root', ledger.store.root, '--input', inputPath]);
  assert.equal(cliResult.fingerprint, direct.fingerprint);
  assert.equal(cliResult.seed, lagRequest().seed);
  assert.equal(Number.isSafeInteger(DEFAULT_LAG_SEED), true);
});
