/**
 * Tests for admin-auth-overlay.js — the §D.3 explicit allow-list.
 * Run with: node --test admin-auth-overlay.test.mjs
 *
 * The gate is an ALLOW-LIST (200 or 503 accept), NOT a reject-list. An
 * earlier draft rejected only 401/403 and silently accepted anything else
 * (500, 404, 502, a transient gateway error) — that inverted-accept
 * regression is what these tests guard against (doc 17 §D.3 warning).
 */

import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

const { classifyAdminProbeStatus, probeAdminKey, wireAdminOverlay } = await import(
  './admin-auth-overlay.js'
);

function fakeFetch(status) {
  return async () => ({ status });
}

function throwingFetch() {
  return async () => {
    throw new TypeError('network error');
  };
}

describe('classifyAdminProbeStatus() — explicit allow-list', () => {
  test('200 accepts (valid admin + store active)', () => {
    assert.equal(classifyAdminProbeStatus(200), true);
  });

  test('503 accepts (valid admin, store inactive for this mode)', () => {
    assert.equal(classifyAdminProbeStatus(503), true);
  });

  test('401 rejects', () => {
    assert.equal(classifyAdminProbeStatus(401), false);
  });

  test('403 rejects', () => {
    assert.equal(classifyAdminProbeStatus(403), false);
  });

  test('500 rejects — guards the inverted-accept regression', () => {
    assert.equal(classifyAdminProbeStatus(500), false);
  });

  test('404 rejects — guards the inverted-accept regression', () => {
    assert.equal(classifyAdminProbeStatus(404), false);
  });

  test('502 rejects — guards the inverted-accept regression', () => {
    assert.equal(classifyAdminProbeStatus(502), false);
  });
});

describe('probeAdminKey() — end-to-end probe classification', () => {
  test('200 -> accepted:true, no network error', async () => {
    const result = await probeAdminKey('tok', fakeFetch(200));
    assert.deepEqual(result, { accepted: true, networkError: false, status: 200 });
  });

  test('503 -> accepted:true (store inactive, still proves admin)', async () => {
    const result = await probeAdminKey('tok', fakeFetch(503));
    assert.equal(result.accepted, true);
    assert.equal(result.status, 503);
  });

  test('401 -> accepted:false, no network error', async () => {
    const result = await probeAdminKey('tok', fakeFetch(401));
    assert.equal(result.accepted, false);
    assert.equal(result.networkError, false);
  });

  test('403 -> accepted:false', async () => {
    const result = await probeAdminKey('tok', fakeFetch(403));
    assert.equal(result.accepted, false);
  });

  test('500 -> accepted:false (NOT silently accepted)', async () => {
    const result = await probeAdminKey('tok', fakeFetch(500));
    assert.equal(result.accepted, false);
  });

  test('404 -> accepted:false (NOT silently accepted)', async () => {
    const result = await probeAdminKey('tok', fakeFetch(404));
    assert.equal(result.accepted, false);
  });

  test('502 -> accepted:false (NOT silently accepted)', async () => {
    const result = await probeAdminKey('tok', fakeFetch(502));
    assert.equal(result.accepted, false);
  });

  test('network throw -> accepted:false, networkError:true, no store', async () => {
    const result = await probeAdminKey('tok', throwingFetch());
    assert.deepEqual(result, { accepted: false, networkError: true, status: null });
  });
});

// ── wireAdminOverlay() — THE shipping login gate (council BLOCKER) ───────────
// admin.js calls this exact function; there is no hand-rolled duplicate probe.
// These tests exercise the shipping path end-to-end, so a future re-inversion
// of the 200|503 allow-list FAILS the suite.

function makeStorage() {
  const store = {};
  return {
    getItem: k => (k in store ? store[k] : null),
    setItem: (k, v) => {
      store[k] = String(v);
    },
    removeItem: k => {
      delete store[k];
    },
    _store: store,
  };
}

function makeEl() {
  return {
    value: '',
    style: { display: '' },
    _listeners: {},
    _focused: false,
    addEventListener(type, fn) {
      (this._listeners[type] ||= []).push(fn);
    },
    dispatch(type, evt) {
      for (const fn of this._listeners[type] || []) fn(evt);
    },
    focus() {
      this._focused = true;
    },
  };
}

function buildOverlay({ status, throwNet = false, token = 'sekret' } = {}) {
  const overlay = makeEl();
  const input = makeEl();
  const submit = makeEl();
  const errMsg = makeEl();
  const signOutBtn = makeEl();
  const storage = makeStorage();
  input.value = token;
  const fetchImpl = throwNet
    ? async () => {
        throw new TypeError('network error');
      }
    : async () => ({ status });
  const api = wireAdminOverlay({ overlay, input, submit, errMsg, signOutBtn, storage, fetchImpl });
  return { overlay, input, submit, errMsg, signOutBtn, storage, api };
}

describe('wireAdminOverlay() — shipping login gate uses the single probe impl', () => {
  test('probe 200 -> stores ci_admin_key + hides overlay', async () => {
    const h = buildOverlay({ status: 200 });
    await h.api.tryAuth();
    assert.equal(h.storage.getItem('ci_admin_key'), 'sekret');
    assert.equal(h.overlay.style.display, 'none');
  });

  test('probe 503 -> stores + hides (valid admin, store inactive for mode)', async () => {
    const h = buildOverlay({ status: 503 });
    await h.api.tryAuth();
    assert.equal(h.storage.getItem('ci_admin_key'), 'sekret');
    assert.equal(h.overlay.style.display, 'none');
  });

  test('probe 401 -> overlay stays up, error shown, token NOT stored', async () => {
    const h = buildOverlay({ status: 401 });
    await h.api.tryAuth();
    assert.equal(h.storage.getItem('ci_admin_key'), null);
    assert.equal(h.errMsg.style.display, '');
  });

  test('probe 403 -> reject, token NOT stored', async () => {
    const h = buildOverlay({ status: 403 });
    await h.api.tryAuth();
    assert.equal(h.storage.getItem('ci_admin_key'), null);
    assert.equal(h.errMsg.style.display, '');
  });

  test('probe 500 -> reject, token NOT stored (guards re-inversion)', async () => {
    const h = buildOverlay({ status: 500 });
    await h.api.tryAuth();
    assert.equal(h.storage.getItem('ci_admin_key'), null);
    assert.equal(h.errMsg.style.display, '');
  });

  test('probe 404 -> reject, token NOT stored', async () => {
    const h = buildOverlay({ status: 404 });
    await h.api.tryAuth();
    assert.equal(h.storage.getItem('ci_admin_key'), null);
  });

  test('probe 502 -> reject, token NOT stored', async () => {
    const h = buildOverlay({ status: 502 });
    await h.api.tryAuth();
    assert.equal(h.storage.getItem('ci_admin_key'), null);
  });

  test('network throw -> reject, token NOT stored, error shown', async () => {
    const h = buildOverlay({ throwNet: true });
    await h.api.tryAuth();
    assert.equal(h.storage.getItem('ci_admin_key'), null);
    assert.equal(h.errMsg.style.display, '');
  });

  test('empty token -> no fetch, no store (early return)', async () => {
    const h = buildOverlay({ status: 200, token: '   ' });
    await h.api.tryAuth();
    assert.equal(h.storage.getItem('ci_admin_key'), null);
  });

  test('Enter key in the input triggers the gate', async () => {
    const h = buildOverlay({ status: 200 });
    h.input.dispatch('keydown', { key: 'Enter' });
    // let the async tryAuth settle
    await new Promise(r => setTimeout(r, 0));
    assert.equal(h.storage.getItem('ci_admin_key'), 'sekret');
  });

  test('sign-out clears ci_admin_key and re-shows the overlay (fix 7)', () => {
    const h = buildOverlay({ status: 200 });
    h.storage.setItem('ci_admin_key', 'sekret');
    h.overlay.style.display = 'none';
    h.signOutBtn.dispatch('click', {});
    assert.equal(h.storage.getItem('ci_admin_key'), null);
    assert.equal(h.overlay.style.display, '');
  });
});
