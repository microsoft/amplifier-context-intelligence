/**
 * Tests for admin-api.js — the distinct ci_admin_key credential + typed
 * fetch wrappers (doc 17 §D.2 / §F.1).
 * Run with: node --test admin-api.test.mjs
 * Node.js built-in test runner (no dependencies required).
 */

import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

// ── Browser globals MUST be set before importing admin-api.js ─────────────

const lsStore = {};
globalThis.localStorage = {
  getItem: k => lsStore[k] ?? null,
  setItem: (k, v) => {
    lsStore[k] = String(v);
  },
  removeItem: k => {
    delete lsStore[k];
  },
  clear: () => Object.keys(lsStore).forEach(k => delete lsStore[k]),
};

let fetchCalls = [];
let nextFetchResponse = null;
globalThis.fetch = async (url, opts = {}) => {
  fetchCalls.push({ url, opts: opts ?? {} });
  if (nextFetchResponse) return nextFetchResponse;
  return { ok: true, status: 200, json: async () => ({}) };
};

function resetFetch() {
  fetchCalls = [];
  nextFetchResponse = null;
}
function resetLs() {
  Object.keys(lsStore).forEach(k => delete lsStore[k]);
}

const mod = await import('./admin-api.js');
const {
  adminAuthHeaders,
  fetchStatus,
  fetchDeadLetters,
  replayWorker,
  purgeWorker,
  listIdentities,
  putIdentity,
  deleteIdentity,
  listKeys,
  putKey,
  deleteKey,
} = mod;

beforeEach(() => {
  resetFetch();
  resetLs();
});

describe('adminAuthHeaders() — distinct credential (ci_admin_key)', () => {
  test('attaches Authorization when ci_admin_key is present', () => {
    localStorage.setItem('ci_admin_key', 'my-admin-token');
    const headers = adminAuthHeaders();
    assert.equal(headers['Authorization'], 'Bearer my-admin-token');
    assert.equal(headers['Content-Type'], 'application/json');
  });

  test('omits Authorization when ci_admin_key is absent', () => {
    const headers = adminAuthHeaders();
    assert.equal(headers['Authorization'], undefined);
  });

  test('does NOT read ci_api_key (the dashboard credential is a distinct store)', () => {
    localStorage.setItem('ci_api_key', 'dashboard-token');
    const headers = adminAuthHeaders();
    assert.equal(headers['Authorization'], undefined);
  });
});

describe('URL builders — encodeURIComponent for path segments', () => {
  test('replayWorker encodes the worker key', async () => {
    await replayWorker('worker/with/slashes');
    assert.equal(
      fetchCalls[0].url,
      '/admin/queues/dead-letter/worker%2Fwith%2Fslashes/replay'
    );
    assert.equal(fetchCalls[0].opts.method, 'POST');
  });

  test('purgeWorker encodes the worker key', async () => {
    await purgeWorker('a b');
    assert.equal(fetchCalls[0].url, '/admin/queues/dead-letter/a%20b/purge');
  });

  test('putIdentity encodes the oid', async () => {
    await putIdentity('11111111-2222-3333-4444-555566667777', { id: 'alice' });
    assert.equal(
      fetchCalls[0].url,
      '/admin/identities/11111111-2222-3333-4444-555566667777'
    );
    assert.equal(fetchCalls[0].opts.method, 'PUT');
    assert.equal(fetchCalls[0].opts.body, JSON.stringify({ id: 'alice' }));
  });

  test('deleteIdentity encodes the oid', async () => {
    await deleteIdentity('11111111-2222-3333-4444-555566667777');
    assert.equal(fetchCalls[0].opts.method, 'DELETE');
  });

  test('putKey encodes the hash', async () => {
    await putKey('a'.repeat(64), { id: 'carol' });
    assert.equal(fetchCalls[0].url, `/admin/keys/${'a'.repeat(64)}`);
  });

  test('deleteKey encodes the hash', async () => {
    await deleteKey('b'.repeat(64));
    assert.equal(fetchCalls[0].url, `/admin/keys/${'b'.repeat(64)}`);
  });
});

describe('err.status propagation on non-ok responses', () => {
  test('fetchStatus throws with err.status = response.status', async () => {
    nextFetchResponse = { ok: false, status: 401, json: async () => ({}) };
    await assert.rejects(fetchStatus(), err => {
      assert.equal(err.status, 401);
      return true;
    });
  });

  test('fetchDeadLetters throws with err.status = 401', async () => {
    nextFetchResponse = { ok: false, status: 401, json: async () => ({}) };
    await assert.rejects(fetchDeadLetters(), err => {
      assert.equal(err.status, 401);
      return true;
    });
  });

  test('listIdentities throws with err.status = 503 (wrong mode)', async () => {
    nextFetchResponse = { ok: false, status: 503, json: async () => ({}) };
    await assert.rejects(listIdentities(), err => {
      assert.equal(err.status, 503);
      return true;
    });
  });

  test('listKeys throws with err.status = 503 (wrong mode)', async () => {
    nextFetchResponse = { ok: false, status: 503, json: async () => ({}) };
    await assert.rejects(listKeys(), err => {
      assert.equal(err.status, 503);
      return true;
    });
  });

  test('deleteKey throws with err.status = 409 (admin-key hash protected)', async () => {
    nextFetchResponse = { ok: false, status: 409, json: async () => ({}) };
    await assert.rejects(deleteKey('c'.repeat(64)), err => {
      assert.equal(err.status, 409);
      return true;
    });
  });
});

describe('successful responses return parsed JSON', () => {
  test('fetchStatus resolves with the parsed body', async () => {
    nextFetchResponse = { ok: true, status: 200, json: async () => ({ uptime_seconds: 5 }) };
    const data = await fetchStatus();
    assert.deepEqual(data, { uptime_seconds: 5 });
  });

  test('replayWorker resolves with {worker_key, replayed}', async () => {
    nextFetchResponse = {
      ok: true,
      status: 200,
      json: async () => ({ worker_key: 'w1', replayed: 3 }),
    };
    const data = await replayWorker('w1');
    assert.deepEqual(data, { worker_key: 'w1', replayed: 3 });
  });
});
