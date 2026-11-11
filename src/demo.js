import {mkdtemp, rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import {Ledger} from './service.js';
import {lagAnalysis} from './lag.js';
const root = await mkdtemp(path.join(os.tmpdir(), 'field-ledger-demo-'));
try {
  const ledger = new Ledger(root);
  await ledger.registerSource({id: 'cooperative', name: 'Synthetic cooperative', region: 'delta', metrics: ['rainfall']});
  await ledger.registerStation({id: 'plot1', name: 'Synthetic plot', region: 'delta', latitude: 10, longitude: 20,
    sources: ['cooperative'], activeFrom: '2026-01-01T00:00:00Z'});
  const batch = {id: 'day1', source: 'cooperative', receivedAt: '2026-11-01T03:00:00Z', records: [
    {station: 'plot1', metric: 'rainfall', time: '2026-11-01T00:00:00Z', revision: 1, value: 12, unit: 'mm'},
    {station: 'plot1', metric: 'rainfall', time: '2026-11-01T01:00:00Z', revision: 1, value: -2, unit: 'mm'},
  ]};
  const first = await ledger.ingest(batch);
  assert.equal(first.accepted, 1);
  assert.equal(first.quarantined, 1);
  assert.equal((await ledger.ingest(batch)).replay, true);
  const quarantined = (await ledger.quarantine())[0];
  await ledger.resolve({id: quarantined.id, action: 'replace', actor: 'operator', reason: 'corrected transcription',
    record: {...batch.records[1], value: 3}});
  await ledger.watermark({source: 'cooperative', time: '2026-11-01T02:00:00Z'});
  const query = {station: 'plot1', metric: 'rainfall', from: '2026-11-01T00:00:00Z', until: '2026-11-01T02:00:00Z', widthMs: 7200000};
  assert.equal((await ledger.windows(query)).windows[0].sum, 15);
  await ledger.export({id: 'snapshot1', purpose: 'synthetic agricultural analysis', query: {station: 'plot1'}});
  await ledger.ingest({id: 'correction', source: 'cooperative', receivedAt: '2026-11-01T04:00:00Z',
    records: [{...batch.records[0], revision: 2, value: 5}]});
  assert.equal((await ledger.windows(query)).windows[0].sum, 8);
  assert.equal((await ledger.getExport('snapshot1')).stale, true);
  await ledger.registerSource({id: 'soil', name: 'Synthetic soil source', region: 'delta',
    metrics: ['rainfall', 'soil_moisture']});
  await ledger.registerStation({id: 'plot2', name: 'Synthetic soil plot', region: 'delta',
    latitude: 10, longitude: 20, sources: ['soil'], activeFrom: '2026-01-01T00:00:00Z'});
  const rainfall = [4, 11, 2, 9, 6, 13, 1, 8];
  const moisture = [33, ...rainfall.slice(0, 7).map(value => 90 - 4 * value)];
  const lagRecords = [];
  rainfall.forEach((value, index) => lagRecords.push({station: 'plot2', metric: 'rainfall',
    time: `2026-11-01T0${index}:00:00Z`, revision: 1, value, unit: 'mm'}));
  moisture.forEach((value, index) => lagRecords.push({station: 'plot2', metric: 'soil_moisture',
    time: `2026-11-01T0${index}:00:00Z`, revision: 1, value, unit: 'pct'}));
  await ledger.ingest({id: 'soil-day1', source: 'soil', receivedAt: '2026-11-02T00:00:00Z', records: lagRecords});
  const lagReport = lagAnalysis(await ledger.store.read(), {
    from: '2026-11-01T00:00:00Z', until: '2026-11-01T08:00:00Z', grid: {widthMs: 3600000},
    left: {station: 'plot2', metric: 'rainfall'}, right: {station: 'plot2', metric: 'soil_moisture'},
    lags: [-1, 0, 1], permutations: 199, seed: 20261111,
  });
  const lead = lagReport.results.find(item => item.lag === 1);
  assert.equal(lead.estimable, true);
  assert.ok(Math.abs(lead.pearson + 1) < 1e-12);
  assert.equal(lead.pairs[0].left.lineage[0].batch, 'soil-day1');
  assert.equal(lagReport.caveats[0].code, 'association_not_causation');
  await ledger.checkpoint();
  const reopened = new Ledger(root);
  assert.equal((await reopened.verify()).valid, true);
  assert.equal((await reopened.status()).activeObservations, 18);
  const reopenedLag = lagAnalysis(await reopened.store.read(), {
    from: '2026-11-01T00:00:00Z', until: '2026-11-01T08:00:00Z', grid: {widthMs: 3600000},
    left: {station: 'plot2', metric: 'rainfall'}, right: {station: 'plot2', metric: 'soil_moisture'},
    lags: [-1, 0, 1], permutations: 199, seed: 20261111,
  });
  assert.equal(reopenedLag.head, lagReport.head);
  assert.equal(reopenedLag.results.find(item => item.lag === 1).pearson, lead.pearson);
  process.stdout.write(JSON.stringify({
    accepted: true,
    status: await reopened.status(),
    lag: {
      head: lagReport.head,
      positiveLagMeansRightLater: true,
      results: lagReport.results.map(item => ({lag: item.lag, estimable: item.estimable,
        sampleSize: item.sampleSize, pearson: item.pearson, adjustedPearsonP: item.adjustedPearsonP})),
    },
    offline: true,
  }) + String.fromCharCode(10));
} finally {
  await rm(root, {recursive: true, force: true});
}
