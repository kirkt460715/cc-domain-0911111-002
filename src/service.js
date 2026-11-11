import {Store} from './store.js';
import * as catalog from './catalog.js';
import * as ingestion from './ingest.js';
import * as queries from './query.js';
import * as windows from './windows.js';
import * as alerting from './alerts.js';
import * as snapshots from './exports.js';
import * as quality from './quality.js';
import * as reconciliation from './reconcile.js';
import * as lag from './lag.js';
import {event, summary, projectEvents} from './state.js';
import {clone, digest, identifier, text} from './value.js';
import {requireValue} from './errors.js';
import {recoverDeadLock} from './files.js';

export class Ledger {
  constructor(root, options = {}) {
    this.store = new Store(root, options);
  }
  async mutate(operation, actor, action, options = {}) {
    const identity = identifier(actor ?? 'local', 'actor');
    return this.store.transact(state => {
      const decision = action(state);
      if (decision.events.length) {
        const audit = {
          operation,
          actor: identity,
          sequence: state.sequence + 1,
          eventCount: decision.events.length,
          fingerprint: digest(decision.events),
        };
        decision.events.push(event('audit.recorded', audit));
      }
      return decision;
    }, options);
  }
  async registerSource(input, actor) {
    return this.mutate('source.register', actor, state => catalog.registerSource(state, input));
  }
  async updateSource(id, patch, actor) {
    return this.mutate('source.update', actor, state => catalog.updateSource(state, id, patch));
  }
  async registerStation(input, actor) {
    return this.mutate('station.register', actor, state => catalog.registerStation(state, input));
  }
  async updateStation(id, patch, actor) {
    return this.mutate('station.update', actor, state => catalog.updateStation(state, id, patch));
  }
  async ingest(input, actor, options = {}) {
    return this.mutate('batch.ingest', actor, state => ingestion.prepareBatch(state, input), options);
  }
  async preview(input) {
    const state = await this.store.read();
    const decision = ingestion.prepareBatch(state, input);
    const projected = projectEvents(state, decision.events);
    return {sequence: state.sequence, result: decision.result, before: summary(state), after: summary(projected),
      fingerprint: digest(input)};
  }
  async watermark(input, actor) {
    return this.mutate('watermark.advance', actor, state => ingestion.advanceWatermark(state, input));
  }
  async resolve(input, actor) {
    return this.mutate('quarantine.resolve', actor, state => ingestion.resolveQuarantine(state, input));
  }
  async query(input) {
    return queries.queryObservations(await this.store.read(), input);
  }
  async history(key) {
    return queries.observationHistory(await this.store.read(), key);
  }
  async timeline(input) {
    return queries.timeline(await this.store.read(), input);
  }
  async windows(input) {
    return windows.aggregateWindows(await this.store.read(), input);
  }
  async gaps(input) {
    return windows.gaps(await this.store.read(), input);
  }
  async saveRule(input, actor) {
    return this.mutate('rule.save', actor, state => alerting.saveRule(state, input));
  }
  async disableRule(id, actor) {
    return this.mutate('rule.disable', actor, state => alerting.disableRule(state, id));
  }
  async evaluate(input, actor) {
    return this.mutate('alerts.evaluate', actor, state => alerting.evaluateAlerts(state, input));
  }
  async acknowledge(input, actor) {
    return this.mutate('alert.acknowledge', actor, state => alerting.acknowledgeAlert(state, input));
  }
  async alerts(input) {
    return alerting.listAlerts(await this.store.read(), input);
  }
  async export(input, actor) {
    return this.mutate('export.create', actor, state => snapshots.createExport(state, input));
  }
  async getExport(id) {
    return snapshots.getExport(await this.store.read(), id);
  }
  async exportManifest(id) {
    return snapshots.exportManifest(await this.store.read(), id);
  }
  async revokeExport(input, actor) {
    return this.mutate('export.revoke', actor, state => snapshots.revokeExport(state, input));
  }
  async profile(input) {
    return quality.profile(await this.store.read(), input);
  }
  async spikes(input) {
    return quality.detectSpikes(await this.store.read(), input);
  }
  async flatlines(input) {
    return quality.flatlines(await this.store.read(), input);
  }
  async disagreements(input) {
    return quality.sourceDisagreement(await this.store.read(), input);
  }
  async freshness(input) {
    return quality.freshness(await this.store.read(), input);
  }
  async inventory(input) {
    return reconciliation.inventory(await this.store.read(), input);
  }
  async reconcile(input) {
    return reconciliation.compareInventory(await this.store.read(), input);
  }
  async impact(input) {
    return reconciliation.correctionImpact(await this.store.read(), input);
  }
  async lagAnalysis(input) {
    return lag.lagAnalysis(await this.store.read(), input);
  }
  async verify() {
    const storage = await this.store.verify();
    const business = reconciliation.auditState(await this.store.read());
    return {valid: storage.valid && business.valid, storage, business};
  }
  async status() {
    return summary(await this.store.read());
  }
  async catalog() {
    const state = await this.store.read();
    return {sources: Object.values(state.sources), stations: Object.values(state.stations),
      metrics: clone(catalog.METRICS), sequence: state.sequence};
  }
  async quarantine({status = 'open', source, batch} = {}) {
    requireValue(['open', 'discarded', 'replaced', 'all'].includes(status), 'QUARANTINE_STATUS', 'Invalid quarantine status');
    const state = await this.store.read();
    return Object.values(state.quarantine).filter(item =>
      (status === 'all' || item.status === status) && (!source || item.source === source) && (!batch || item.batch === batch));
  }
  async batches({source} = {}) {
    const state = await this.store.read();
    return Object.values(state.batches).filter(batch => !source || batch.source === source)
      .map(({result, ...batch}) => ({...batch, accepted: result.accepted, duplicate: result.duplicate, quarantined: result.quarantined}));
  }
  async audit({actor, operation, after = 0} = {}) {
    const state = await this.store.read();
    return state.audits.filter(item => (!actor || item.actor === actor) &&
      (!operation || item.operation === operation) && item.sequence > after);
  }
  async checkpoint() {
    return this.store.compact();
  }
  async backup(destination) {
    return this.store.backup(destination);
  }
  async recoverLock() {
    await this.store.initialize();
    return recoverDeadLock(this.store.root);
  }
}
