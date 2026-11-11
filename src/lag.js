import {array, choice, digest, exactKeys, identifier, integer, number, timestamp, unique} from './value.js';
import {requireValue} from './errors.js';
import {metric} from './catalog.js';
import {filterObservations, preferSources, queryInput} from './query.js';
import {stableSum} from './windows.js';

export const DEFAULT_LAG_SEED = 20261111;
const DEFAULT_PERMUTATIONS = 999;
const DEFAULT_MINIMUM_COUNT = 3;
const DEFAULT_ALPHA = 0.05;
const MAX_SPAN = 20000;
const MAX_LAGS = 64;
const MAX_PERMUTATIONS = 10000;
const MAX_PERMUTATION_WORK = 10_000_000;

export const CAVEATS = Object.freeze([
  {
    code: 'association_not_causation',
    message: 'Lagged correlation quantifies association only; it never establishes causation, direction, or a physical lead-lag mechanism.',
  },
  {
    code: 'shared_trend_inflation',
    message: 'Common trends, seasonality, or one concentrated rainfall event can inflate coefficients at several lags; compare the whole lag profile, exclusions and paired values before interpreting a peak.',
  },
  {
    code: 'complete_cells_only',
    message: 'Pairs use only grid cells where both sides carry observations. Missing cells are excluded; they are never zero-filled, forward-filled, or counted from multiple sources twice.',
  },
]);

function sideInput(input, label) {
  exactKeys(input, ['station', 'metric', 'source', 'quality', 'selection', 'includeSuspect', 'aggregation'],
    ['station', 'metric']);
  const station = identifier(input.station, `${label}.station`);
  const definition = metric(input.metric);
  const includeSuspect = input.includeSuspect ?? false;
  requireValue(typeof includeSuspect === 'boolean', 'BOOLEAN', `${label}.includeSuspect must be boolean`);
  return {
    station,
    metric: input.metric,
    unit: definition.unit,
    source: input.source ? identifier(input.source, `${label}.source`) : null,
    quality: input.quality ? choice(input.quality, ['measured', 'estimated', 'suspect'], `${label}.quality`) : null,
    selection: choice(input.selection ?? 'preferred', ['all', 'preferred'], `${label}.selection`),
    includeSuspect,
    aggregation: choice(input.aggregation ?? definition.aggregation, ['sum', 'mean'], `${label}.aggregation`),
  };
}

export function lagInput(input = {}) {
  exactKeys(input,
    ['from', 'until', 'grid', 'left', 'right', 'lags', 'minimumCount', 'permutations', 'seed', 'blockCells', 'alpha'],
    ['from', 'until', 'grid', 'left', 'right', 'lags']);
  const from = timestamp(input.from, 'from');
  const until = timestamp(input.until, 'until');
  requireValue(from < until, 'PERIOD', 'Lag analysis end must follow start');
  exactKeys(input.grid, ['origin', 'widthMs'], ['widthMs']);
  const widthMs = integer(input.grid.widthMs, 'grid.widthMs', 1000, 604800000);
  const origin = timestamp(input.grid.origin ?? from, 'grid.origin');
  requireValue(origin <= from, 'GRID_ORIGIN', 'Grid origin cannot be later than the analysis start');
  const span = Math.ceil((Date.parse(until) - Date.parse(origin)) / widthMs);
  requireValue(span >= 1 && span <= MAX_SPAN, 'LIMIT', 'Grid span is outside its allowed range', {span, maximum: MAX_SPAN});
  const lags = unique(array(input.lags, 'lags', MAX_LAGS).map(value =>
    integer(value, 'lag', -(MAX_SPAN - 1), MAX_SPAN - 1)), 'lags').sort((a, b) => a - b);
  requireValue(lags.every(lag => Math.abs(lag) < span), 'LAG_RANGE', 'Lag must stay inside the grid span', {span});
  const minimumCount = integer(input.minimumCount ?? DEFAULT_MINIMUM_COUNT, 'minimumCount', 2, 100000);
  const permutations = integer(input.permutations ?? DEFAULT_PERMUTATIONS, 'permutations', 0, MAX_PERMUTATIONS);
  const seed = integer(input.seed ?? DEFAULT_LAG_SEED, 'seed', 0, Number.MAX_SAFE_INTEGER);
  const blockCells = integer(input.blockCells ?? 1, 'blockCells', 1, span);
  const alpha = number(input.alpha ?? DEFAULT_ALPHA, 'alpha', 0, 1);
  const work = permutations * lags.length * span;
  requireValue(work <= MAX_PERMUTATION_WORK, 'LIMIT', 'Permutation workload is too large',
    {work, maximum: MAX_PERMUTATION_WORK});
  return {
    from, until,
    grid: {origin, widthMs, originMs: Date.parse(origin), span},
    left: sideInput(input.left, 'left'),
    right: sideInput(input.right, 'right'),
    lags, minimumCount, permutations, seed, blockCells, alpha,
  };
}

function buildCells(state, side, window, grid) {
  const station = state.stations[side.station];
  requireValue(station, 'STATION_MISSING', 'Unknown station', {station: side.station});
  if (side.source) {
    requireValue(station.sources.includes(side.source), 'SOURCE_STATION', 'Source is not assigned to station');
  }
  const filter = {station: side.station, metric: side.metric, from: window.from, until: window.until};
  if (side.source) {
    filter.source = side.source;
  }
  if (side.quality) {
    filter.quality = side.quality;
  }
  let rows = filterObservations(state, queryInput(filter));
  if (!side.quality && !side.includeSuspect) {
    rows = rows.filter(row => row.quality !== 'suspect');
  }
  if (side.selection === 'preferred') {
    rows = preferSources(state, rows);
  }
  const buckets = new Map();
  for (const row of rows) {
    const index = Math.floor((Date.parse(row.time) - grid.originMs) / grid.widthMs);
    if (index < 0 || index >= grid.span) {
      continue;
    }
    if (!buckets.has(index)) {
      buckets.set(index, []);
    }
    buckets.get(index).push(row);
  }
  const cells = new Map();
  for (const [index, bucket] of buckets) {
    const values = bucket.map(row => row.value);
    const total = stableSum(values);
    const begin = grid.originMs + index * grid.widthMs;
    cells.set(index, {
      index,
      from: new Date(begin).toISOString(),
      until: new Date(begin + grid.widthMs).toISOString(),
      value: side.aggregation === 'sum' ? total : total / values.length,
      aggregation: side.aggregation,
      observations: bucket.length,
      lineage: bucket.map(row => ({key: row.key, revision: row.revision, batch: row.batch})),
    });
  }
  return cells;
}

export function pearson(x, y) {
  const n = x.length;
  const meanX = stableSum(x) / n;
  const meanY = stableSum(y) / n;
  let sxx = 0;
  let syy = 0;
  let sxy = 0;
  for (let index = 0; index < n; index += 1) {
    const dx = x[index] - meanX;
    const dy = y[index] - meanY;
    sxx += dx * dx;
    syy += dy * dy;
    sxy += dx * dy;
  }
  if (sxx === 0 || syy === 0) {
    return null;
  }
  const coefficient = sxy / Math.sqrt(sxx * syy);
  return Math.max(-1, Math.min(1, coefficient));
}

export function averageRanks(values) {
  const order = [...values.keys()].sort((a, b) => (values[a] - values[b]) || a - b);
  const ranks = new Array(values.length);
  let cursor = 0;
  while (cursor < order.length) {
    let end = cursor + 1;
    while (end < order.length && values[order[end]] === values[order[cursor]]) {
      end += 1;
    }
    const tiedRank = (cursor + 1 + end) / 2;
    for (let index = cursor; index < end; index += 1) {
      ranks[order[index]] = tiedRank;
    }
    cursor = end;
  }
  return ranks;
}

export function spearman(x, y) {
  return pearson(averageRanks(x), averageRanks(y));
}

function tieSummary(values) {
  const counts = new Map();
  for (const value of values) {
    counts.set(value, (counts.get(value) ?? 0) + 1);
  }
  let tieGroups = 0;
  let largestTie = 1;
  for (const count of counts.values()) {
    if (count > 1) {
      tieGroups += 1;
      largestTie = Math.max(largestTie, count);
    }
  }
  return {tieGroups, largestTie};
}

function pairCells(leftCells, rightCells, lag, span) {
  const lower = Math.max(0, -lag);
  const upper = Math.min(span, span - lag);
  const pairs = [];
  const excluded = {leftMissing: 0, rightMissing: 0, bothMissing: 0};
  for (let index = lower; index < upper; index += 1) {
    const left = leftCells.get(index);
    const right = rightCells.get(index + lag);
    if (left && right) {
      pairs.push({gridIndex: index, left, right});
    } else if (!left && !right) {
      excluded.bothMissing += 1;
    } else if (!left) {
      excluded.leftMissing += 1;
    } else {
      excluded.rightMissing += 1;
    }
  }
  return {pairs, excluded, candidateCells: upper - lower};
}

function mulberry32(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function permutationSeed(seed, lag) {
  return Number.parseInt(digest({seed, lag}).slice(0, 8), 16);
}

function permutationTest(pairs, blockCells, observed, request) {
  const sampleSize = pairs.length;
  const blocks = Math.ceil(sampleSize / blockCells);
  if (request.permutations === 0) {
    return {blocks, reason: 'permutations_disabled', pearsonP: null, spearmanP: null};
  }
  if (blocks < 2) {
    return {blocks, reason: 'insufficient_blocks', pearsonP: null, spearmanP: null};
  }
  const left = pairs.map(pair => pair.left.value);
  const right = pairs.map(pair => pair.right.value);
  const leftRanks = averageRanks(left);
  const order = Array.from({length: blocks}, (_, index) => index);
  const random = mulberry32(permutationSeed(request.seed, request.lag));
  let pearsonExtreme = 0;
  let spearmanExtreme = 0;
  for (let iteration = 0; iteration < request.permutations; iteration += 1) {
    for (let index = blocks - 1; index > 0; index -= 1) {
      const swap = Math.floor(random() * (index + 1));
      [order[index], order[swap]] = [order[swap], order[index]];
    }
    const shuffledRight = [];
    for (const blockIndex of order) {
      const start = blockIndex * blockCells;
      for (let index = start; index < Math.min(start + blockCells, sampleSize); index += 1) {
        shuffledRight.push(right[index]);
      }
    }
    const permutedPearson = pearson(left, shuffledRight);
    if (permutedPearson !== null && Math.abs(permutedPearson) >= Math.abs(observed.pearson)) {
      pearsonExtreme += 1;
    }
    const permutedSpearman = pearson(leftRanks, averageRanks(shuffledRight));
    if (permutedSpearman !== null && Math.abs(permutedSpearman) >= Math.abs(observed.spearman)) {
      spearmanExtreme += 1;
    }
  }
  const denominator = request.permutations + 1;
  return {
    blocks,
    reason: null,
    pearsonP: (1 + pearsonExtreme) / denominator,
    spearmanP: (1 + spearmanExtreme) / denominator,
  };
}

export function holmAdjust(entries) {
  const familySize = entries.length;
  const ordered = [...entries].sort((a, b) => (a.p - b.p) || a.lag - b.lag);
  const adjusted = new Map();
  let running = 0;
  ordered.forEach((entry, index) => {
    running = Math.max(running, (familySize - index) * entry.p);
    adjusted.set(entry.lag, Math.min(1, running));
  });
  return adjusted;
}

function sideSummary(side, cells, grid) {
  const present = cells.size;
  const values = [...cells.values()].map(cell => cell.value);
  const constant = values.length > 0 && values.every(value => value === values[0]);
  return {
    station: side.station,
    metric: side.metric,
    unit: side.unit,
    source: side.source,
    selection: side.selection,
    quality: side.quality,
    includeSuspect: side.includeSuspect,
    aggregation: side.aggregation,
    cells: present,
    span: grid.span,
    coverage: present / grid.span,
    constant,
  };
}

export function lagAnalysis(state, input) {
  const request = lagInput(input);
  const window = {from: request.from, until: request.until};
  const leftCells = buildCells(state, request.left, window, request.grid);
  const rightCells = buildCells(state, request.right, window, request.grid);
  const results = request.lags.map(lag => {
    const {pairs, excluded, candidateCells} = pairCells(leftCells, rightCells, lag, request.grid.span);
    const sampleSize = pairs.length;
    const base = {
      lag,
      sampleSize,
      estimable: false,
      reason: null,
      constantSide: null,
      pearson: null,
      spearman: null,
      pearsonP: null,
      spearmanP: null,
      adjustedPearsonP: null,
      adjustedSpearmanP: null,
      significantPearson: null,
      significantSpearman: null,
      permutations: request.permutations,
      blocks: null,
      permutationReason: null,
      excluded: {
        candidateCells,
        paired: sampleSize,
        leftMissing: excluded.leftMissing,
        rightMissing: excluded.rightMissing,
        bothMissing: excluded.bothMissing,
      },
      ties: {
        left: tieSummary(pairs.map(pair => pair.left.value)),
        right: tieSummary(pairs.map(pair => pair.right.value)),
      },
      pairs: pairs.map(pair => ({
        gridIndex: pair.gridIndex,
        left: {
          from: pair.left.from,
          until: pair.left.until,
          value: pair.left.value,
          observations: pair.left.observations,
          lineage: pair.left.lineage,
        },
        right: {
          from: pair.right.from,
          until: pair.right.until,
          value: pair.right.value,
          observations: pair.right.observations,
          lineage: pair.right.lineage,
        },
      })),
    };
    if (sampleSize < request.minimumCount) {
      base.reason = 'insufficient_samples';
      base.permutationReason = 'not_estimable';
      base.blocks = Math.ceil(sampleSize / request.blockCells);
      return base;
    }
    const leftValues = pairs.map(pair => pair.left.value);
    const rightValues = pairs.map(pair => pair.right.value);
    const r = pearson(leftValues, rightValues);
    if (r === null) {
      const leftConstant = leftValues.every(value => value === leftValues[0]);
      const rightConstant = rightValues.every(value => value === rightValues[0]);
      base.reason = 'constant_series';
      base.constantSide = leftConstant && rightConstant ? 'both' : leftConstant ? 'left' : 'right';
      base.permutationReason = 'not_estimable';
      base.blocks = Math.ceil(sampleSize / request.blockCells);
      return base;
    }
    const rho = spearman(leftValues, rightValues);
    base.estimable = true;
    base.pearson = r;
    base.spearman = rho;
    const permutation = permutationTest(pairs, request.blockCells, {pearson: r, spearman: rho},
      {...request, lag});
    base.blocks = permutation.blocks;
    base.permutationReason = permutation.reason;
    base.pearsonP = permutation.pearsonP;
    base.spearmanP = permutation.spearmanP;
    return base;
  });
  const pearsonFamily = results.filter(result => result.estimable && result.pearsonP !== null)
    .map(result => ({lag: result.lag, p: result.pearsonP}));
  const spearmanFamily = results.filter(result => result.estimable && result.spearmanP !== null)
    .map(result => ({lag: result.lag, p: result.spearmanP}));
  const adjustedPearson = holmAdjust(pearsonFamily);
  const adjustedSpearman = holmAdjust(spearmanFamily);
  for (const result of results) {
    if (adjustedPearson.has(result.lag)) {
      result.adjustedPearsonP = adjustedPearson.get(result.lag);
      result.significantPearson = result.adjustedPearsonP <= request.alpha;
    }
    if (adjustedSpearman.has(result.lag)) {
      result.adjustedSpearmanP = adjustedSpearman.get(result.lag);
      result.significantSpearman = result.adjustedSpearmanP <= request.alpha;
    }
  }
  const fingerprint = digest({
    head: state.head,
    request: {
      from: request.from, until: request.until, grid: request.grid, lags: request.lags,
      left: request.left, right: request.right, minimumCount: request.minimumCount,
      permutations: request.permutations, seed: request.seed, blockCells: request.blockCells, alpha: request.alpha,
    },
  });
  return {
    version: 1,
    sequence: state.sequence,
    head: state.head,
    window: {from: request.from, until: request.until},
    grid: {origin: request.grid.origin, widthMs: request.grid.widthMs, span: request.grid.span},
    left: sideSummary(request.left, leftCells, request.grid),
    right: sideSummary(request.right, rightCells, request.grid),
    lags: request.lags,
    minimumCount: request.minimumCount,
    permutations: request.permutations,
    seed: request.seed,
    blockCells: request.blockCells,
    alpha: request.alpha,
    blocking: 'paired_sample_grid_order',
    familySize: pearsonFamily.length,
    fingerprint,
    caveats: CAVEATS,
    results,
  };
}
