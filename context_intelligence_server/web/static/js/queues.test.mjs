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
const { fetchDeadLetters, replayWorker, purgeWorker, computeInvariant, computeTotals, deadLetterRowData } = await import('./queues.js');

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

// ─────────────────────────────────────────────────────────────────────────────
// computeTotals() — only Replayed + Write retries chips (rest live in equation)
// ─────────────────────────────────────────────────────────────────────────────

describe('computeTotals()', () => {
  test('maps ONLY Replayed and Write retries from metrics', () => {
    const totals = computeTotals({
      accepted_total: 1,
      written_total: 2,
      replayed_total: 3,
      write_retries_total: 4,
      in_queue_total: 5,
      dead_letter_total: 6,
    });
    const labels = totals.map(t => t.label);
    const values = totals.map(t => t.value);
    assert.deepEqual(labels, ['Replayed', 'Write retries']);
    assert.deepEqual(values, [3, 4]);
    // redundant chips (already in the invariant equation / dead-letter table) are dropped
    for (const dropped of ['Accepted', 'Written', 'In queue', 'Dead-letter', 'Oldest unflushed']) {
      assert.ok(!labels.includes(dropped), `${dropped} should NOT be a chip`);
    }
  });

  test('defaults missing metrics to 0', () => {
    const totals = computeTotals({});
    const values = totals.map(t => t.value);
    assert.deepEqual(values, [0, 0]);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// deadLetterRowData() — maps a dead-letter entry to row fields
// ─────────────────────────────────────────────────────────────────────────────

describe('deadLetterRowData()', () => {
  test('maps worker_key/item_count/last_error/last_ts', () => {
    const row = deadLetterRowData({ worker_key: 'k1', item_count: 4, last_error: 'boom', last_ts: 1700000000 });
    assert.deepEqual(row, { workerKey: 'k1', itemCount: 4, lastError: 'boom', lastTs: 1700000000 });
  });

  test('applies defaults for missing fields (lastTs null)', () => {
    const row = deadLetterRowData({});
    assert.deepEqual(row, { workerKey: '', itemCount: 0, lastError: '', lastTs: null });
  });
});

// ───────────────────────────────────────────────────────────────────────────
// queues.js source wiring — browser-only render + 3s poll + actions (C2)
// Source-asserted: importing queues.js in node MUST NOT run the DOM block, so
// we read the file as text and assert the browser wiring is present.
// ───────────────────────────────────────────────────────────────────────────

describe('queues.js source wiring', () => {
  const src = readFileSync(new URL('./queues.js', import.meta.url), 'utf8');

  test('polls refresh every 3 seconds via setInterval', () => {
    assert.match(src, /setInterval\(\s*refresh\s*,\s*3000\s*\)/);
  });

  test('guards DOM code with a typeof document check (importable in node)', () => {
    assert.ok(src.includes("typeof document !== 'undefined'"), 'missing IS_BROWSER guard');
  });

  test('renders the invariant/totals/dead-letter elements by id', () => {
    for (const id of ['invariant-card', 'invariant-eq', 'invariant-badge', 'totals-row', 'dead-letter-body']) {
      assert.ok(src.includes(`'${id}'`), `missing element id '${id}'`);
    }
  });

  test('S2: no standalone invariant-result element', () => {
    assert.ok(!src.includes("getElementById('invariant-result')"), 'invariant-result must not exist (S2)');
  });

  test('C2 poll guard: re-render early-returns while a Purge confirm is open', () => {
    assert.ok(src.includes("querySelector('.actions[data-confirming]')"), 'missing poll-vs-confirm guard');
  });

  test('U1 honest feedback: consumes the response integer for Replay and Purge', () => {
    assert.ok(src.includes('Re-enqueued'), "missing 'Re-enqueued' feedback");
    assert.ok(src.includes('Purged'), "missing 'Purged' feedback");
    assert.ok(src.includes('.replayed'), 'must read .replayed from response');
    assert.ok(src.includes('.purged'), 'must read .purged from response');
  });

  test('U2 error honesty: 401 clears key + reshows auth overlay; 400 distinct Invalid', () => {
    assert.ok(src.includes("removeItem('ci_api_key')"), '401 must clear ci_api_key');
    assert.ok(src.includes("getElementById('auth-overlay')"), '401 must re-show auth overlay');
    assert.ok(src.includes('Invalid'), "400 must show distinct 'Invalid' message");
  });

  test('Focus-to-Cancel: focus moves to the Cancel button after confirm', () => {
    assert.ok(src.includes('purge-cancel'), 'missing purge-cancel control');
    assert.ok(src.includes('.focus()'), 'must move focus to Cancel');
  });

  test('wires actions through replayWorker/purgeWorker', () => {
    assert.ok(src.includes('replayWorker('), 'must call replayWorker');
    assert.ok(src.includes('purgeWorker('), 'must call purgeWorker');
  });
});
