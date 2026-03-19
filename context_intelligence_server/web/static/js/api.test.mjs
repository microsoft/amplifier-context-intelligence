/**
 * Tests for api.js - auth token integration
 * Run with: node api.test.mjs
 * Node.js built-in test runner (no dependencies required)
 */

import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

// ── Browser globals MUST be set before importing api.js ──────────────────────

const localStorageStore = {};
globalThis.localStorage = {
  getItem:    (key) => localStorageStore[key] ?? null,
  setItem:    (key, val) => { localStorageStore[key] = String(val); },
  removeItem: (key) => { delete localStorageStore[key]; },
  clear:      () => { Object.keys(localStorageStore).forEach(k => delete localStorageStore[k]); },
};

let capturedFetchCalls = [];
globalThis.fetch = async (url, options = {}) => {
  capturedFetchCalls.push({ url, options: options ?? {} });
  return {
    ok: true,
    status: 200,
    json: async () => ({ result: 'mocked' }),
  };
};

// ── Import module under test (after globals are in place) ────────────────────
const { fetchStatus, postCypher } = await import('./api.js');

// ── Helpers ───────────────────────────────────────────────────────────────────
function resetFetchCalls() { capturedFetchCalls = []; }
function resetLocalStorage() {
  Object.keys(localStorageStore).forEach(k => delete localStorageStore[k]);
}

// ─────────────────────────────────────────────────────────────────────────────
// fetchStatus()
// ─────────────────────────────────────────────────────────────────────────────

describe('fetchStatus()', () => {
  beforeEach(() => { resetFetchCalls(); resetLocalStorage(); });

  test('fetches /status endpoint', async () => {
    await fetchStatus();
    assert.equal(capturedFetchCalls.length, 1);
    assert.equal(capturedFetchCalls[0].url, '/status');
  });

  test('does NOT send Authorization header — even when token exists in localStorage', async () => {
    localStorage.setItem('ci_api_key', 'super-secret-token');
    await fetchStatus();
    const headers = capturedFetchCalls[0].options?.headers ?? {};
    assert.ok(
      !headers['Authorization'],
      'fetchStatus must not include Authorization header (endpoint is auth-exempt)'
    );
  });

  test('does NOT send Authorization header when no token', async () => {
    await fetchStatus();
    const headers = capturedFetchCalls[0].options?.headers ?? {};
    assert.ok(!headers['Authorization']);
  });

  test('throws when response is not ok', async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async () => ({ ok: false, status: 503, json: async () => ({}) });
    try {
      await assert.rejects(
        () => fetchStatus(),
        (err) => {
          assert.ok(err instanceof Error, 'should throw an Error');
          assert.ok(err.message.includes('503'), `expected "503" in message, got: "${err.message}"`);
          return true;
        }
      );
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// postCypher()
// ─────────────────────────────────────────────────────────────────────────────

describe('postCypher()', () => {
  beforeEach(() => {
    resetFetchCalls();
    resetLocalStorage();
    localStorage.setItem('ci_api_key', 'test-token-abc123');
  });

  test('POSTs to /cypher', async () => {
    await postCypher('MATCH (n) RETURN n');
    assert.equal(capturedFetchCalls.length, 1);
    assert.equal(capturedFetchCalls[0].url, '/cypher');
    assert.equal(capturedFetchCalls[0].options.method, 'POST');
  });

  test('includes Authorization: Bearer <token> header', async () => {
    await postCypher('MATCH (n) RETURN n');
    const headers = capturedFetchCalls[0].options.headers;
    assert.equal(
      headers['Authorization'],
      'Bearer test-token-abc123',
      'postCypher must include Authorization header with token from localStorage'
    );
  });

  test('includes Content-Type: application/json header', async () => {
    await postCypher('MATCH (n) RETURN n');
    const headers = capturedFetchCalls[0].options.headers;
    assert.equal(headers['Content-Type'], 'application/json');
  });

  test('sends query, params, workspace in JSON body', async () => {
    await postCypher('MATCH (n) RETURN n', { limit: 5 }, 'my-workspace');
    const body = JSON.parse(capturedFetchCalls[0].options.body);
    assert.equal(body.query, 'MATCH (n) RETURN n');
    assert.deepEqual(body.params, { limit: 5 });
    assert.equal(body.workspace, 'my-workspace');
  });

  test('defaults params to {} when not provided', async () => {
    await postCypher('MATCH (n) RETURN n');
    const body = JSON.parse(capturedFetchCalls[0].options.body);
    assert.deepEqual(body.params, {});
  });

  test('defaults workspace to * when not provided', async () => {
    await postCypher('MATCH (n) RETURN n');
    const body = JSON.parse(capturedFetchCalls[0].options.body);
    assert.equal(body.workspace, '*');
  });

  test('does NOT include Authorization header when localStorage has no token', async () => {
    resetLocalStorage(); // clear all — no token
    await postCypher('MATCH (n) RETURN n');
    const headers = capturedFetchCalls[0].options.headers;
    assert.ok(
      !headers['Authorization'],
      'postCypher must not add Authorization when no token stored'
    );
  });

  test('throws when response is not ok', async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async () => ({ ok: false, status: 401, json: async () => ({}) });
    try {
      await assert.rejects(
        () => postCypher('MATCH (n) RETURN n'),
        (err) => {
          assert.ok(err instanceof Error);
          assert.ok(err.message.includes('401'), `expected "401" in message, got: "${err.message}"`);
          return true;
        }
      );
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
