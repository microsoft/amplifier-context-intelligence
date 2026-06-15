/**
 * Import-based behavioral tests for queues-panel.js
 * Run with: node --test queues-panel.test.mjs
 * Node.js built-in test runner (no dependencies required).
 *
 * DISCIPLINE: the fixtures here are built from the REAL /status.metrics shape
 * sampled from the running system, NOT invented. The end-to-end renderQueues
 * test and the computeInvariant real-shape tests are RED-provable against the
 * old short-name binding bug (m.accepted instead of m.accepted_total) and
 * against the "passed whole /status object where .metrics was expected" bug.
 *
 * The REAL verified /status.metrics shape (nested under top-level /status as
 * .metrics):
 *   {
 *     accepted_total:int, written_total:int, replayed_total:int,
 *     write_retries_total:int, in_queue_total:int, dead_letter_total:int,
 *     residual:int, degraded:bool
 *   }
 */

import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

// ── Browser globals MUST be set before importing queues-panel.js ────────────

const lsStore = {};
globalThis.localStorage = {
  getItem:    (k) => lsStore[k] ?? null,
  setItem:    (k, v) => { lsStore[k] = String(v); },
  removeItem: (k) => { delete lsStore[k]; },
  clear:      () => { Object.keys(lsStore).forEach(k => delete lsStore[k]); },
};

let fetchCalls = [];
let nextFetchResponse = null;
globalThis.fetch = async (url, opts = {}) => {
  fetchCalls.push({ url, opts: opts ?? {} });
  if (nextFetchResponse) return nextFetchResponse;
  return { ok: true, status: 200, json: async () => ({}) };
};

// ── Minimal mock document ───────────────────────────────────────────────────
// Covers exactly the operations the render functions touch.

function makeElement(id) {
  return {
    id,
    className: '',
    textContent: '',
    innerHTML: '',
    _attrs: {},
    _confirmOpen: null, // element returned for '.actions[data-confirming]'
    _cells: {},         // key -> cell element for '.actions[data-key="key"]'
    _listeners: {},
    _focused: false,
    setAttribute(k, v) { this._attrs[k] = v; },
    getAttribute(k) { return this._attrs[k] ?? null; },
    addEventListener(type, fn) { (this._listeners[type] ||= []).push(fn); },
    focus() { this._focused = true; },
    querySelector(sel) {
      if (sel === '.actions[data-confirming]') return this._confirmOpen;
      if (sel === '#purge-cancel') return this._cells.__cancel || null;
      const m = sel.match(/\.actions\[data-key="(.*)"\]$/);
      if (m) return this._cells[m[1]] || null;
      return null;
    },
  };
}

let els = {};
globalThis.document = {
  getElementById: (id) => els[id] || null,
};

function setupDom() {
  els = {};
  for (const id of [
    'invariant-card', 'invariant-eq', 'invariant-badge',
    'totals-row', 'dead-letter-body',
  ]) {
    els[id] = makeElement(id);
  }
}

// ── Import module under test (after globals are in place) ────────────────────
const mod = await import('./queues-panel.js');
const {
  computeInvariant,
  renderQueues,
  renderDeadLetters,
  renderDeadLetterError,
  fetchDeadLetters,
  replayWorker,
  purgeWorker,
} = mod;

function resetFetch() { fetchCalls = []; nextFetchResponse = null; }
function resetLs() { Object.keys(lsStore).forEach(k => delete lsStore[k]); }

// ─────────────────────────────────────────────────────────────────────────────
// computeInvariant() — pure helper, REAL shape, RED-provable binding
// ─────────────────────────────────────────────────────────────────────────────

describe('computeInvariant() — real *_total shape', () => {
  test('balanced pipeline → Balanced badge, calm card', () => {
    const m = {
      accepted_total: 10, written_total: 10, replayed_total: 0,
      write_retries_total: 0, in_queue_total: 0, dead_letter_total: 0,
      residual: 0, degraded: false,
    };
    const inv = computeInvariant(m);
    assert.equal(inv.equation, '10 − 10 − 0 − 0 = 0');
    assert.equal(inv.badgeText, 'Balanced ✓');
    assert.equal(inv.cardClass, 'card invariant');
  });

  test('dead-lettered (residual 0, dead 1) → "1 DEAD-LETTERED", loud card', () => {
    // THE binding fixture. Buggy short-name code reads m.accepted (undefined)
    // → 0 across the board → "0 − 0 − 0 − 0 = 0" / "Balanced ✓".
    const m = {
      accepted_total: 12, written_total: 11, replayed_total: 3,
      write_retries_total: 2, in_queue_total: 0, dead_letter_total: 1,
      residual: 0, degraded: true,
    };
    const inv = computeInvariant(m);
    assert.equal(inv.equation, '12 − 11 − 0 − 1 = 0');
    assert.equal(inv.badgeText, '1 DEAD-LETTERED');
    assert.equal(inv.cardClass, 'card invariant degraded');
  });

  test('off-by residual → "OFF BY n — INVESTIGATE", loud card', () => {
    const m = {
      accepted_total: 10, written_total: 8, in_queue_total: 1,
      dead_letter_total: 0, residual: 1, degraded: true,
    };
    const inv = computeInvariant(m);
    assert.equal(inv.equation, '10 − 8 − 1 − 0 = +1');
    assert.equal(inv.badgeText, 'OFF BY 1 — INVESTIGATE');
    assert.equal(inv.cardClass, 'card invariant degraded');
  });

  test('loud/calm derived LOCALLY from residual/dead, NOT m.degraded', () => {
    // m.degraded lies (false) but dead>0 with residual 0 → must still be loud.
    const m = {
      accepted_total: 7, written_total: 5, in_queue_total: 0,
      dead_letter_total: 2, residual: 0, degraded: false,
    };
    const inv = computeInvariant(m);
    assert.equal(inv.equation, '7 − 5 − 0 − 2 = 0');
    assert.equal(inv.badgeText, '2 DEAD-LETTERED');
    assert.equal(inv.cardClass, 'card invariant degraded');
  });

  test('missing metrics default to 0', () => {
    assert.equal(computeInvariant({}).equation, '0 − 0 − 0 − 0 = 0');
    assert.equal(computeInvariant({}).badgeText, 'Balanced ✓');
    assert.equal(computeInvariant(null).equation, '0 − 0 − 0 − 0 = 0');
    assert.equal(computeInvariant(undefined).equation, '0 − 0 − 0 − 0 = 0');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// renderQueues() — end-to-end binding from a realistic WHOLE /status object
// ─────────────────────────────────────────────────────────────────────────────

describe('renderQueues(status) — end-to-end binding', () => {
  beforeEach(() => { setupDom(); });

  test('drives the invariant card from status.metrics (whole /status object)', () => {
    // RED-provable two ways:
    //  1. short-name bug → "0 − 0 − 0 − 0 = 0" / "Balanced ✓"
    //  2. "passed whole object where .metrics expected" bug → reads
    //     status.accepted_total (undefined) → same "0 − 0 − 0 − 0".
    const status = {
      uptime_seconds: 42.0,
      metrics: {
        accepted_total: 12, written_total: 11, replayed_total: 3,
        write_retries_total: 2, in_queue_total: 0, dead_letter_total: 1,
        residual: 0, degraded: true,
      },
    };
    renderQueues(status);
    assert.equal(els['invariant-eq'].textContent, '12 − 11 − 0 − 1 = 0');
    assert.equal(els['invariant-badge'].textContent, '1 DEAD-LETTERED');
    assert.equal(els['invariant-card'].className, 'card invariant degraded');
  });

  test('renders totals chips for Replayed + Write retries', () => {
    const status = {
      metrics: {
        accepted_total: 12, written_total: 11, replayed_total: 3,
        write_retries_total: 2, in_queue_total: 0, dead_letter_total: 1,
        residual: 0, degraded: true,
      },
    };
    renderQueues(status);
    const html = els['totals-row'].innerHTML;
    assert.ok(html.includes('Replayed') && html.includes('3'), `got: ${html}`);
    assert.ok(html.includes('Write retries') && html.includes('2'), `got: ${html}`);
  });

  test('status without metrics → balanced default (no throw)', () => {
    renderQueues({});
    assert.equal(els['invariant-eq'].textContent, '0 − 0 − 0 − 0 = 0');
    assert.equal(els['invariant-badge'].textContent, 'Balanced ✓');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// fetch wrappers — authenticated, defensive encoding, honest err.status
// ─────────────────────────────────────────────────────────────────────────────

describe('fetch wrappers', () => {
  beforeEach(() => { resetFetch(); resetLs(); localStorage.setItem('ci_api_key', 'tok-123'); });

  test('fetchDeadLetters() GETs /queues/dead-letter with auth header', async () => {
    nextFetchResponse = {
      ok: true, status: 200,
      json: async () => ({ dead_letters: [{ worker_key: 'w1', item_count: 1 }] }),
    };
    const out = await fetchDeadLetters();
    assert.equal(fetchCalls[0].url, '/queues/dead-letter');
    assert.equal(fetchCalls[0].opts.headers['Authorization'], 'Bearer tok-123');
    assert.deepEqual(out.dead_letters[0], { worker_key: 'w1', item_count: 1 });
  });

  test('replayWorker() POSTs to encoded .../replay with auth header', async () => {
    nextFetchResponse = { ok: true, status: 200, json: async () => ({ worker_key: 'a/b c', replayed: 4 }) };
    const out = await replayWorker('a/b c');
    assert.equal(fetchCalls[0].url, '/queues/dead-letter/a%2Fb%20c/replay');
    assert.equal(fetchCalls[0].opts.method, 'POST');
    assert.equal(fetchCalls[0].opts.headers['Authorization'], 'Bearer tok-123');
    assert.equal(out.replayed, 4);
  });

  test('purgeWorker() POSTs to encoded .../purge', async () => {
    nextFetchResponse = { ok: true, status: 200, json: async () => ({ worker_key: 'w1', purged: 2 }) };
    const out = await purgeWorker('w1');
    assert.equal(fetchCalls[0].url, '/queues/dead-letter/w1/purge');
    assert.equal(fetchCalls[0].opts.method, 'POST');
    assert.equal(out.purged, 2);
  });

  test('non-ok response throws with err.status attached', async () => {
    nextFetchResponse = { ok: false, status: 400, json: async () => ({}) };
    await assert.rejects(
      () => replayWorker('w1'),
      (err) => { assert.equal(err.status, 400); return true; }
    );
  });

  test('401 surfaces as err.status 401', async () => {
    nextFetchResponse = { ok: false, status: 401, json: async () => ({}) };
    await assert.rejects(
      () => fetchDeadLetters(),
      (err) => { assert.equal(err.status, 401); return true; }
    );
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// poll-vs-confirm guard + dead-letter rendering
// ─────────────────────────────────────────────────────────────────────────────

describe('renderDeadLetters() poll-vs-confirm guard', () => {
  beforeEach(() => { setupDom(); });

  test('empty list → all-clear row', () => {
    renderDeadLetters([]);
    assert.ok(els['dead-letter-body'].innerHTML.includes('all clear'),
      els['dead-letter-body'].innerHTML);
  });

  test('entries → rows include worker key + item count', () => {
    renderDeadLetters([{ worker_key: 'sess-42', item_count: 3, last_error: 'boom', last_ts: null }]);
    const html = els['dead-letter-body'].innerHTML;
    assert.ok(html.includes('sess-42'), html);
    assert.ok(html.includes('3'), html);
  });

  test('does NOT wipe an open Purge confirm (poll guard)', () => {
    const body = els['dead-letter-body'];
    body.innerHTML = '<tr>CONFIRM-OPEN</tr>';
    body._confirmOpen = makeElement('cell'); // .actions[data-confirming] present
    renderDeadLetters([{ worker_key: 'w1', item_count: 9 }]);
    assert.equal(body.innerHTML, '<tr>CONFIRM-OPEN</tr>', 'poll must not overwrite open confirm');
  });

  test('renderDeadLetterError also respects the confirm guard', () => {
    const body = els['dead-letter-body'];
    body.innerHTML = '<tr>CONFIRM-OPEN</tr>';
    body._confirmOpen = makeElement('cell');
    renderDeadLetterError();
    assert.equal(body.innerHTML, '<tr>CONFIRM-OPEN</tr>');
  });

  test('renderDeadLetterError renders the retry message when no confirm open', () => {
    renderDeadLetterError();
    assert.ok(els['dead-letter-body'].innerHTML.includes("Couldn't load dead-letter queues"),
      els['dead-letter-body'].innerHTML);
  });
});
