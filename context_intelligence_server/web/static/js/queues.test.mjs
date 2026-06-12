/**
 * Tests for queues.js - authenticated dead-letter fetch wrappers (C2)
 * Run with: node --test queues.test.mjs
 * Node.js built-in test runner (no dependencies required)
 */

import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

// ── Browser globals MUST be set before importing queues.js ──────────────────

const localStorageStore = {};
globalThis.localStorage = {
  getItem:    (key) => localStorageStore[key] ?? null,
  setItem:    (key, val) => { localStorageStore[key] = String(val); },
  removeItem: (key) => { delete localStorageStore[key]; },
  clear:      () => { Object.keys(localStorageStore).forEach(k => delete localStorageStore[k]); },
};

let capturedFetchCalls = [];
const okFetch = async (url, options = {}) => {
  capturedFetchCalls.push({ url, options: options ?? {} });
  return {
    ok: true,
    status: 200,
    json: async () => ({ ok: true }),
  };
};
globalThis.fetch = okFetch;

// ── Import module under test (after globals are in place) ────────────────────
const { fetchDeadLetters, replayWorker, purgeWorker, computeInvariant } = await import('./queues.js');

// ── Helpers ──────────────────────────────────────────────────────────────────
function resetFetchCalls() { capturedFetchCalls = []; }
function resetLocalStorage() {
  Object.keys(localStorageStore).forEach(k => delete localStorageStore[k]);
}

// ──────────────────────────────────────────────────────────────────────────────
// fetchDeadLetters()
// ──────────────────────────────────────────────────────────────────────────────

describe('fetchDeadLetters()', () => {
  beforeEach(() => { globalThis.fetch = okFetch; resetFetchCalls(); resetLocalStorage(); localStorage.setItem('ci_api_key', 'tok'); });

  test('GETs /queues/dead-letter with Bearer and no body', async () => {
    await fetchDeadLetters();
    assert.equal(capturedFetchCalls.length, 1);
    assert.equal(capturedFetchCalls[0].url, '/queues/dead-letter');
    const opts = capturedFetchCalls[0].options;
    // GET: method is either undefined or 'GET'
    assert.ok(opts.method === undefined || opts.method === 'GET');
    assert.equal(opts.headers.Authorization, 'Bearer tok');
    assert.equal(opts.body, undefined);
  });
});

// ──────────────────────────────────────────────────────────────────────────────
// replayWorker()
// ──────────────────────────────────────────────────────────────────────────────

describe('replayWorker()', () => {
  beforeEach(() => { globalThis.fetch = okFetch; resetFetchCalls(); resetLocalStorage(); localStorage.setItem('ci_api_key', 'tok'); });

  test('POSTs to /queues/dead-letter/<key>/replay with Bearer', async () => {
    await replayWorker('abc-123');
    assert.equal(capturedFetchCalls.length, 1);
    assert.equal(capturedFetchCalls[0].url, '/queues/dead-letter/abc-123/replay');
    const opts = capturedFetchCalls[0].options;
    assert.equal(opts.method, 'POST');
    assert.equal(opts.headers.Authorization, 'Bearer tok');
  });

  test('URL-encodes the worker_key', async () => {
    await replayWorker('a b/c');
    assert.equal(capturedFetchCalls[0].url, '/queues/dead-letter/a%20b%2Fc/replay');
  });
});

// ──────────────────────────────────────────────────────────────────────────────
// purgeWorker()
// ──────────────────────────────────────────────────────────────────────────────

describe('purgeWorker()', () => {
  beforeEach(() => { globalThis.fetch = okFetch; resetFetchCalls(); resetLocalStorage(); localStorage.setItem('ci_api_key', 'tok'); });

  test('POSTs to /queues/dead-letter/<key>/purge with Bearer', async () => {
    await purgeWorker('_no_session__my-workspace');
    assert.equal(capturedFetchCalls.length, 1);
    assert.equal(capturedFetchCalls[0].url, '/queues/dead-letter/_no_session__my-workspace/purge');
    const opts = capturedFetchCalls[0].options;
    assert.equal(opts.method, 'POST');
    assert.equal(opts.headers.Authorization, 'Bearer tok');
  });
});

// ──────────────────────────────────────────────────────────────────────────────
// fetch wrappers surface HTTP status on failure (U2)
// ──────────────────────────────────────────────────────────────────────────────

describe('fetch wrappers surface HTTP status on failure (U2)', () => {
  beforeEach(() => { resetFetchCalls(); resetLocalStorage(); localStorage.setItem('ci_api_key', 'tok'); });

  test('replayWorker rejects with err.status===400 on bad request', async () => {
    globalThis.fetch = async () => ({ ok: false, status: 400, json: async () => ({}) });
    try {
      await assert.rejects(
        () => replayWorker('bad key'),
        (err) => { assert.equal(err.status, 400); return true; }
      );
    } finally {
      globalThis.fetch = okFetch;
    }
  });

  test('fetchDeadLetters rejects with err.status===401 when unauthorized', async () => {
    globalThis.fetch = async () => ({ ok: false, status: 401, json: async () => ({}) });
    try {
      await assert.rejects(
        () => fetchDeadLetters(),
        (err) => { assert.equal(err.status, 401); return true; }
      );
    } finally {
      globalThis.fetch = okFetch;
    }
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// computeInvariant() — calm/loud state keyed off backend `degraded` flag (C3)
// ─────────────────────────────────────────────────────────────────────────────

describe('computeInvariant()', () => {
  test('balanced: residual 0, dead 0, degraded false → calm card', () => {
    const r = computeInvariant({ accepted: 10, written: 7, in_queue: 3, dead: 0, residual: 0, degraded: false });
    assert.equal(r.residualText, '0');
    assert.equal(r.badgeText, 'Balanced ✓');
    assert.equal(r.badgeClass, 'badge badge-primary');
    assert.equal(r.cardClass, 'card invariant');
    assert.equal(r.aria, 'Pipeline balanced');
    assert.equal(r.equation, '10 − 7 − 3 − 0 = 0');
  });

  test('accounted-but-pending: residual 0, dead>0, degraded true → dead-lettered loud', () => {
    const r = computeInvariant({ accepted: 10, written: 7, in_queue: 2, dead: 1, residual: 0, degraded: true });
    assert.equal(r.residualText, '0');
    assert.equal(r.badgeText, '1 DEAD-LETTERED');
    assert.equal(r.badgeClass, 'badge badge-error');
    assert.equal(r.cardClass, 'card invariant degraded');
    assert.equal(r.aria, '1 events dead-lettered — accounted for, needs attention');
    assert.equal(r.equation, '10 − 7 − 2 − 1 = 0');
  });

  test('true loss: residual +3 → off-by loud', () => {
    const r = computeInvariant({ accepted: 10, written: 4, in_queue: 3, dead: 0, residual: 3, degraded: true });
    assert.equal(r.residualText, '+3');
    assert.equal(r.badgeText, 'OFF BY 3 — INVESTIGATE');
    assert.equal(r.badgeClass, 'badge badge-error');
    assert.equal(r.cardClass, 'card invariant degraded');
    assert.equal(r.aria, 'Pipeline off by 3 events — investigate possible event loss');
  });

  test('negative residual: residual -2 → off-by with abs value, signed text', () => {
    const r = computeInvariant({ accepted: 5, written: 5, in_queue: 1, dead: 1, residual: -2, degraded: true });
    assert.equal(r.residualText, '-2');
    assert.equal(r.badgeText, 'OFF BY 2 — INVESTIGATE');
  });

  test('derives degraded from residual/dead when flag absent', () => {
    const r = computeInvariant({ accepted: 10, written: 7, in_queue: 2, dead: 1, residual: 0 });
    assert.equal(r.cardClass, 'card invariant degraded');
    assert.equal(r.badgeText, '1 DEAD-LETTERED');
  });
});
