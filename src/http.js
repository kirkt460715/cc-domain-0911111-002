import http from 'node:http';
import {LedgerError, errorDocument, fail} from './errors.js';
import {jsonParse, integer, secureEqual} from './value.js';

async function body(request, maximum) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > maximum) {
      fail('BODY_LIMIT', 'Request exceeds body limit', {maximum}, 413);
    }
    chunks.push(chunk);
  }
  if (!size) {
    return {};
  }
  const contentType = request.headers['content-type']?.split(';')[0];
  if (contentType !== 'application/json') {
    fail('CONTENT_TYPE', 'Expected application/json', {}, 415);
  }
  return jsonParse(Buffer.concat(chunks).toString('utf8'));
}
function send(response, status, data) {
  const encoded = JSON.stringify(data);
  response.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(encoded),
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
  });
  response.end(encoded);
}
function queryParameters(url) {
  const result = {};
  for (const [key, value] of url.searchParams) {
    if (Object.hasOwn(result, key)) {
      fail('QUERY_DUPLICATE', 'Query parameter repeated');
    }
    if (['limit', 'after'].includes(key)) {
      result[key] = Number(value);
    } else if (['active', 'includeDeleted'].includes(key)) {
      if (!['true', 'false'].includes(value)) {
        fail('QUERY_BOOLEAN', 'Boolean query parameter must be true or false');
      }
      result[key] = value === 'true';
    } else {
      result[key] = value;
    }
  }
  return result;
}
export function createServer(ledger, options = {}) {
  const maximum = integer(options.maximumBody ?? 8 * 1024 * 1024, 'maximumBody', 1024);
  const token = options.token ?? null;
  const server = http.createServer(async (request, response) => {
    try {
      if (token && !secureEqual(request.headers.authorization, `Bearer ${token}`)) {
        fail('UNAUTHORIZED', 'Authentication required', {}, 401);
      }
      const url = new URL(request.url, 'http://localhost');
      const route = url.pathname;
      const actor = request.headers['x-actor'] ?? 'http';
      let result;
      if (request.method === 'GET') {
        const query = queryParameters(url);
        switch (route) {
          case '/status':
            result = await ledger.status();
            break;
          case '/catalog':
            result = await ledger.catalog();
            break;
          case '/observations':
            result = await ledger.query(query);
            break;
          case '/history':
            result = await ledger.history(query.key);
            break;
          case '/quarantine':
            result = await ledger.quarantine(query);
            break;
          case '/alerts':
            result = await ledger.alerts(query);
            break;
          case '/batches':
            result = await ledger.batches(query);
            break;
          case '/audit':
            result = await ledger.audit(query);
            break;
          case '/verify':
            result = await ledger.verify();
            break;
          default:
            if (route.startsWith('/exports/')) {
              result = await ledger.getExport(decodeURIComponent(route.slice(9)));
            } else {
              fail('NOT_FOUND', 'Endpoint does not exist', {}, 404);
            }
        }
      } else if (request.method === 'POST') {
        const input = await body(request, maximum);
        switch (route) {
          case '/sources':
            result = await ledger.registerSource(input, actor);
            break;
          case '/stations':
            result = await ledger.registerStation(input, actor);
            break;
          case '/batches': {
            const expected = request.headers['if-match'];
            const expectedSequence = expected === undefined ? undefined : integer(Number(expected), 'if-match', 0);
            result = await ledger.ingest(input, actor, {expectedSequence});
            break;
          }
          case '/preview':
            result = await ledger.preview(input);
            break;
          case '/watermarks':
            result = await ledger.watermark(input, actor);
            break;
          case '/quarantine/resolve':
            result = await ledger.resolve(input, actor);
            break;
          case '/windows':
            result = await ledger.windows(input);
            break;
          case '/gaps':
            result = await ledger.gaps(input);
            break;
          case '/rules':
            result = await ledger.saveRule(input, actor);
            break;
          case '/alerts/evaluate':
            result = await ledger.evaluate(input, actor);
            break;
          case '/alerts/acknowledge':
            result = await ledger.acknowledge(input, actor);
            break;
          case '/exports':
            result = await ledger.export(input, actor);
            break;
          case '/exports/revoke':
            result = await ledger.revokeExport(input, actor);
            break;
          case '/quality/profile':
            result = await ledger.profile(input);
            break;
          case '/quality/spikes':
            result = await ledger.spikes(input);
            break;
          case '/quality/flatlines':
            result = await ledger.flatlines(input);
            break;
          case '/quality/disagreements':
            result = await ledger.disagreements(input);
            break;
          case '/quality/freshness':
            result = await ledger.freshness(input);
            break;
          case '/inventory':
            result = await ledger.inventory(input);
            break;
          case '/reconcile':
            result = await ledger.reconcile(input);
            break;
          case '/impact':
            result = await ledger.impact(input);
            break;
          case '/lag-analysis':
            result = await ledger.lagAnalysis(input);
            break;
          default:
            fail('NOT_FOUND', 'Endpoint does not exist', {}, 404);
        }
      } else {
        fail('METHOD', 'Method is not supported', {}, 405);
      }
      send(response, 200, result);
    } catch (error) {
      if (!response.headersSent && !response.destroyed) {
        send(response, error instanceof LedgerError ? error.status : 500, errorDocument(error));
      }
    }
  });
  server.requestTimeout = 30000;
  server.headersTimeout = 10000;
  server.keepAliveTimeout = 1000;
  return server;
}
export async function listen(ledger, {port = 8080, token} = {}) {
  integer(port, 'port', 0, 65535);
  const server = createServer(ledger, {token});
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', resolve);
  });
  return server;
}
