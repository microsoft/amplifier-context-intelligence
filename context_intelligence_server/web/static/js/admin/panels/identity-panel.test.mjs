/**
 * Tests for identity-panel.js (P2, doc 17 §E.2 / §F.1).
 * Run with: node --test identity-panel.test.mjs
 * Node.js built-in test runner (no dependencies required).
 */

import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

// ── Browser globals MUST be set before importing the module under test ────

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
let fetchResponder = null;
globalThis.fetch = async (url, opts = {}) => {
  fetchCalls.push({ url, opts: opts ?? {} });
  if (fetchResponder) return fetchResponder(url, opts);
  return { ok: true, status: 200, json: async () => ({}) };
};

// ── Minimal mock document ───────────────────────────────────────────────────

function makeElement(id) {
  return {
    id,
    value: '',
    innerHTML: '',
    textContent: '',
    style: { display: '' },
    dataset: {},
    _attrs: {},
    _listeners: {},
    setAttribute(k, v) {
      this._attrs[k] = v;
    },
    getAttribute(k) {
      return this._attrs[k] ?? null;
    },
    removeAttribute(k) {
      delete this._attrs[k];
    },
    addEventListener(type, fn) {
      (this._listeners[type] ||= []).push(fn);
    },
    dispatch(type, evt) {
      for (const fn of this._listeners[type] || []) fn(evt);
    },
  };
}

let els = {};
globalThis.document = {
  getElementById: id => els[id] || null,
};

const CORE_IDS = [
  'identity-keys-card',
  'identity-identities-card',
  'admin-keys-body',
  'admin-identities-body',
  'key-add-btn',
  'key-hash-input',
  'key-id-input',
  'key-hash-error',
  'key-form-error',
  'oid-add-btn',
  'oid-input',
  'oid-id-input',
  'oid-display-input',
  'oid-error',
  'oid-form-error',
];

function setupDom() {
  els = {};
  for (const id of CORE_IDS) els[id] = makeElement(id);
}

function makeButtonMock(dataset, row) {
  return {
    dataset,
    disabled: false,
    _row: row,
    closest(sel) {
      if (sel === 'button') return this;
      if (sel.startsWith('tr')) return this._row;
      return null;
    },
  };
}

function makeCellMock() {
  const cell = {
    innerHTML: '',
    _cancelBtn: null,
    querySelector(sel) {
      if (sel === '[data-confirm-cancel]') return cell._cancelBtn;
      return null;
    },
  };
  return cell;
}

function makeRowMock(datasetKeyName, value) {
  const cell = makeCellMock();
  return {
    dataset: { [datasetKeyName]: value },
    _cell: cell,
    querySelector(sel) {
      if (sel === '[data-actions]') return cell;
      return null;
    },
  };
}

// ── Import module under test ───────────────────────────────────────────────

const mod = await import('./identity-panel.js');
const {
  normalizeGuid,
  isValidGuid,
  normalizeHash,
  isValidHash,
  keyRowHtml,
  identityRowHtml,
  notConfiguredHtml,
  renderKeys,
  renderIdentities,
  renderKeysError,
  renderIdentitiesError,
  refreshKeys,
  refreshIdentities,
  setIdentityMode,
} = mod;

// A richer row mock: arbitrary dataset + a real [data-actions] cell.
function makeRow(dataset) {
  const cell = makeCellMock();
  return {
    dataset,
    _cell: cell,
    querySelector(sel) {
      if (sel === '[data-actions]') return cell;
      return null;
    },
  };
}

beforeEach(() => {
  fetchCalls = [];
  fetchResponder = null;
  setupDom();
});

// ── Pure validation / normalization ─────────────────────────────────────────

describe('GUID validation and case normalization (doc 17 §G item 9)', () => {
  test('lowercase valid GUID is accepted', () => {
    assert.equal(isValidGuid('11111111-2222-3333-4444-555566667777'), true);
  });

  test('UPPERCASE GUID (Azure Portal style) normalizes to lowercase and is ACCEPTED', () => {
    const upper = '11111111-2222-3333-4444-555566667777'.toUpperCase();
    assert.equal(normalizeGuid(upper), '11111111-2222-3333-4444-555566667777');
    assert.equal(isValidGuid(upper), true);
  });

  test('all-zeros GUID is rejected', () => {
    assert.equal(isValidGuid('00000000-0000-0000-0000-000000000000'), false);
  });

  test('malformed GUID is rejected', () => {
    assert.equal(isValidGuid('not-a-guid'), false);
  });

  test('whitespace around a GUID is trimmed before validation', () => {
    assert.equal(isValidGuid('  11111111-2222-3333-4444-555566667777  '), true);
  });
});

describe('Hash validation and case normalization (doc 17 §G item 9)', () => {
  test('lowercase 64-hex hash is accepted', () => {
    assert.equal(isValidHash('a'.repeat(64)), true);
  });

  test('UPPERCASE SHA-256 (Windows Get-FileHash style) normalizes and is ACCEPTED', () => {
    const upper = 'A'.repeat(64);
    assert.equal(normalizeHash(upper), 'a'.repeat(64));
    assert.equal(isValidHash(upper), true);
  });

  test('wrong length is rejected', () => {
    assert.equal(isValidHash('a'.repeat(63)), false);
  });

  test('non-hex characters are rejected', () => {
    assert.equal(isValidHash('g'.repeat(64)), false);
  });
});

// ── Row builders ─────────────────────────────────────────────────────────

describe('keyRowHtml() / identityRowHtml() — mode-specific columns', () => {
  test('keyRowHtml renders hash + contributor + delete action', () => {
    const html = keyRowHtml({ hash: 'a'.repeat(64), id: 'carol' });
    assert.match(html, /data-hash="a{64}"/);
    assert.match(html, /carol/);
    assert.match(html, /data-action="delete-key"/);
  });

  test('identityRowHtml renders oid + contributor + display name + delete action', () => {
    const html = identityRowHtml({
      oid: '11111111-2222-3333-4444-555566667777',
      id: 'alice',
      display_name: 'Alice Smith',
    });
    assert.match(html, /data-oid="11111111-2222-3333-4444-555566667777"/);
    assert.match(html, /alice/);
    assert.match(html, /Alice Smith/);
    assert.match(html, /data-action="delete-identity"/);
  });

  test('identityRowHtml renders em-dash for missing display name', () => {
    const html = identityRowHtml({ oid: '1'.repeat(8) + '-2222-3333-4444-555566667777', id: 'bob' });
    assert.match(html, /—/);
  });
});

describe('notConfiguredHtml() — doc 06 B.6 "Not configured" state', () => {
  test('renders the pill.admin signal, not an error', () => {
    const html = notConfiguredHtml('API keys');
    assert.match(html, /class="pill admin"/);
    assert.match(html, /Not configured/);
    assert.doesNotMatch(html, /result-error/);
  });
});

// ── Mode switch (doc 17 §E.2) ──────────────────────────────────────────────

describe('setIdentityMode() — mode switch', () => {
  test('static mode: keys card gets live table+form; identities card gets Not configured', () => {
    setIdentityMode('static', () => {});
    const keysCard = document.getElementById('identity-keys-card');
    const idCard = document.getElementById('identity-identities-card');
    assert.match(keysCard.innerHTML, /Hash/);
    assert.match(keysCard.innerHTML, /Contributor/);
    assert.match(idCard.innerHTML, /Not configured/);
  });

  test('entra mode: identities card gets live table+form; keys card gets Not configured', () => {
    setIdentityMode('entra', () => {});
    const keysCard = document.getElementById('identity-keys-card');
    const idCard = document.getElementById('identity-identities-card');
    assert.match(idCard.innerHTML, /OID/);
    assert.match(idCard.innerHTML, /Display name/);
    assert.match(keysCard.innerHTML, /Not configured/);
  });
});

// ── GUID client-hint (identities form) ─────────────────────────────────────

describe('Identity add form — GUID client hint', () => {
  test('a malformed OID sets aria-invalid and shows the field error', async () => {
    setIdentityMode('entra', () => {});
    const oidInput = document.getElementById('oid-input');
    const oidError = document.getElementById('oid-error');
    oidInput.value = 'not-a-guid';
    const addBtn = document.getElementById('oid-add-btn');
    for (const fn of addBtn._listeners.click) await fn();
    assert.equal(oidInput._attrs['aria-invalid'], 'true');
    assert.equal(oidError.style.display, '');
  });

  test('the all-zeros GUID is rejected by the client hint too', async () => {
    setIdentityMode('entra', () => {});
    const oidInput = document.getElementById('oid-input');
    oidInput.value = '00000000-0000-0000-0000-000000000000';
    const addBtn = document.getElementById('oid-add-btn');
    for (const fn of addBtn._listeners.click) await fn();
    assert.equal(oidInput._attrs['aria-invalid'], 'true');
  });

  test('a valid UPPERCASE OID is accepted and PUT with the lowercased value', async () => {
    setIdentityMode('entra', () => {});
    const oidInput = document.getElementById('oid-input');
    const idInput = document.getElementById('oid-id-input');
    oidInput.value = '11111111-2222-3333-4444-555566667777'.toUpperCase();
    idInput.value = 'alice';
    const addBtn = document.getElementById('oid-add-btn');
    for (const fn of addBtn._listeners.click) await fn();
    const putCall = fetchCalls.find(c => c.opts.method === 'PUT');
    assert.ok(putCall, 'expected a PUT call');
    assert.equal(putCall.url, '/admin/identities/11111111-2222-3333-4444-555566667777');
  });
});

describe('Key add form — hash client hint', () => {
  test('a malformed hash sets aria-invalid and shows the field error', async () => {
    setIdentityMode('static', () => {});
    const hashInput = document.getElementById('key-hash-input');
    const hashError = document.getElementById('key-hash-error');
    hashInput.value = 'not-a-hash';
    const addBtn = document.getElementById('key-add-btn');
    for (const fn of addBtn._listeners.click) await fn();
    assert.equal(hashInput._attrs['aria-invalid'], 'true');
    assert.equal(hashError.style.display, '');
  });

  test('an UPPERCASE hash is accepted and PUT with the lowercased value', async () => {
    setIdentityMode('static', () => {});
    const hashInput = document.getElementById('key-hash-input');
    const idInput = document.getElementById('key-id-input');
    hashInput.value = 'A'.repeat(64);
    idInput.value = 'carol';
    const addBtn = document.getElementById('key-add-btn');
    for (const fn of addBtn._listeners.click) await fn();
    const putCall = fetchCalls.find(c => c.opts.method === 'PUT');
    assert.ok(putCall);
    assert.equal(putCall.url, `/admin/keys/${'a'.repeat(64)}`);
  });
});

// ── Delete confirm idiom ────────────────────────────────────────────────────

describe('Delete confirm idiom (keys)', () => {
  test('clicking Delete swaps to the inline confirm and focuses Cancel', () => {
    setIdentityMode('static', () => {});
    const body = document.getElementById('admin-keys-body');
    const row = makeRowMock('hash', 'a'.repeat(64));
    const cancelBtn = { focus() { this._focused = true; } };
    row._cell._cancelBtn = cancelBtn;
    const deleteBtn = makeButtonMock({ action: 'delete-key' }, row);

    body.dispatch('click', { target: deleteBtn });

    assert.match(row._cell.innerHTML, /confirm-q/);
    assert.equal(row.dataset.confirming, '1');
    assert.equal(cancelBtn._focused, true);
  });

  test('Cancel restores the Delete button', () => {
    setIdentityMode('static', () => {});
    const body = document.getElementById('admin-keys-body');
    const row = makeRowMock('hash', 'a'.repeat(64));
    row.dataset.confirming = '1';
    const cancelBtn = makeButtonMock({ confirmCancel: '' }, row);

    body.dispatch('click', { target: cancelBtn });

    assert.equal(row.dataset.confirming, undefined);
    assert.match(row._cell.innerHTML, /data-action="delete-key"/);
  });
});

describe('Delete confirm idiom (identities)', () => {
  test('clicking Delete swaps to the inline confirm', () => {
    setIdentityMode('entra', () => {});
    const body = document.getElementById('admin-identities-body');
    const row = makeRowMock('oid', '11111111-2222-3333-4444-555566667777');
    const deleteBtn = makeButtonMock({ action: 'delete-identity' }, row);

    body.dispatch('click', { target: deleteBtn });

    assert.match(row._cell.innerHTML, /confirm-q/);
    assert.match(row._cell.innerHTML, /Delete 11111111-2222-3333-4444-555566667777\?/);
  });
});

// ── List re-fetch after mutation ────────────────────────────────────────────

describe('refreshKeys() / refreshIdentities() — list re-fetch', () => {
  test('refreshKeys renders the fetched list', async () => {
    fetchResponder = () => ({
      ok: true,
      status: 200,
      json: async () => ({ keys: [{ hash: 'a'.repeat(64), id: 'carol' }] }),
    });
    await refreshKeys(() => {});
    const body = document.getElementById('admin-keys-body');
    assert.match(body.innerHTML, /carol/);
  });

  test('refreshIdentities renders the fetched list', async () => {
    fetchResponder = () => ({
      ok: true,
      status: 200,
      json: async () => ({
        identities: [{ oid: '11111111-2222-3333-4444-555566667777', id: 'alice' }],
      }),
    });
    await refreshIdentities(() => {});
    const body = document.getElementById('admin-identities-body');
    assert.match(body.innerHTML, /alice/);
  });

  test('renderKeys renders the empty state distinctly', () => {
    renderKeys([]);
    const body = document.getElementById('admin-keys-body');
    assert.match(body.innerHTML, /No API keys registered/);
  });

  test('401 on refreshKeys calls onAuthLost', async () => {
    fetchResponder = () => ({ ok: false, status: 401, json: async () => ({}) });
    let authLost = false;
    await refreshKeys(() => {
      authLost = true;
    });
    assert.equal(authLost, true);
  });
});

// ── Non-401 error state (council fix 3) ─────────────────────────────────────

describe('non-401 refresh failure renders a distinct error row (not empty)', () => {
  test('renderKeysError renders a .result-error row, not an empty table', () => {
    renderKeysError();
    const body = document.getElementById('admin-keys-body');
    assert.match(body.innerHTML, /result-error/);
    assert.match(body.innerHTML, /Couldn't load API keys/);
    assert.doesNotMatch(body.innerHTML, /No API keys registered/);
  });

  test('renderIdentitiesError renders a .result-error row, not an empty table', () => {
    renderIdentitiesError();
    const body = document.getElementById('admin-identities-body');
    assert.match(body.innerHTML, /result-error/);
    assert.match(body.innerHTML, /Couldn't load OID identities/);
  });

  test('refreshKeys on a 500 renders the error row (not silent, not onAuthLost)', async () => {
    fetchResponder = () => ({ ok: false, status: 500, json: async () => ({}) });
    let authLost = false;
    await refreshKeys(() => {
      authLost = true;
    });
    assert.equal(authLost, false);
    const body = document.getElementById('admin-keys-body');
    assert.match(body.innerHTML, /Couldn't load API keys/);
  });

  test('refreshIdentities on a 503 renders the error row (not silent)', async () => {
    fetchResponder = () => ({ ok: false, status: 503, json: async () => ({}) });
    await refreshIdentities(() => {});
    const body = document.getElementById('admin-identities-body');
    assert.match(body.innerHTML, /Couldn't load OID identities/);
  });
});

// ── Per-row Edit affordance (council fix 2) ─────────────────────────────────

describe('Edit prefills the add/update form (removes hand-transcribe hazard)', () => {
  // Use fresh identifiers no other test marks confirming, so the module-level
  // confirming Sets can't bleed a confirm state into these pure-render checks.
  test('rows carry Edit + Delete buttons (spec §E.2)', () => {
    const html = keyRowHtml({ hash: 'f'.repeat(64), id: 'carol' });
    assert.match(html, /data-action="edit-key"/);
    assert.match(html, /data-action="delete-key"/);
    const idHtml = identityRowHtml({ oid: '99999999-8888-7777-6666-555544443333', id: 'alice', display_name: 'Alice' });
    assert.match(idHtml, /data-action="edit-identity"/);
    assert.match(idHtml, /data-action="delete-identity"/);
  });

  test('keyRowHtml stamps data-id on the row so Edit can prefill the contributor', () => {
    const html = keyRowHtml({ hash: 'f'.repeat(64), id: 'carol' });
    assert.match(html, /data-id="carol"/);
  });

  test('identityRowHtml stamps data-id + data-display on the row', () => {
    const html = identityRowHtml({
      oid: '99999999-8888-7777-6666-555544443333',
      id: 'alice',
      display_name: 'Alice Smith',
    });
    assert.match(html, /data-id="alice"/);
    assert.match(html, /data-display="Alice Smith"/);
  });

  test('clicking Edit on a key row prefills the hash + contributor inputs', () => {
    setIdentityMode('static', () => {});
    const body = document.getElementById('admin-keys-body');
    const row = makeRow({ hash: 'a'.repeat(64), id: 'carol' });
    const editBtn = makeButtonMock({ action: 'edit-key' }, row);
    body.dispatch('click', { target: editBtn });
    assert.equal(document.getElementById('key-hash-input').value, 'a'.repeat(64));
    assert.equal(document.getElementById('key-id-input').value, 'carol');
  });

  test('clicking Edit on an identity row prefills oid + contributor + display', () => {
    setIdentityMode('entra', () => {});
    const body = document.getElementById('admin-identities-body');
    const row = makeRow({
      oid: '11111111-2222-3333-4444-555566667777',
      id: 'alice',
      display: 'Alice Smith',
    });
    const editBtn = makeButtonMock({ action: 'edit-identity' }, row);
    body.dispatch('click', { target: editBtn });
    assert.equal(document.getElementById('oid-input').value, '11111111-2222-3333-4444-555566667777');
    assert.equal(document.getElementById('oid-id-input').value, 'alice');
    assert.equal(document.getElementById('oid-display-input').value, 'Alice Smith');
  });
});

// ── Double-submit guard on DELETE confirm (council fix 4) ────────────────────

describe('DELETE confirm disables the Confirm button before the await', () => {
  test('key delete: Confirm button is disabled synchronously on click', async () => {
    setIdentityMode('static', () => {});
    // delete + subsequent list refresh both succeed
    fetchResponder = (url, opts) => {
      if (opts && opts.method === 'DELETE') return { ok: true, status: 200, json: async () => ({ deleted: true }) };
      return { ok: true, status: 200, json: async () => ({ keys: [] }) };
    };
    const body = document.getElementById('admin-keys-body');
    const row = makeRow({ hash: 'a'.repeat(64), id: 'carol' });
    const confirmBtn = makeButtonMock({ confirmAction: 'delete-key' }, row);

    body.dispatch('click', { target: confirmBtn });
    // Synchronously — before any await resolves — the button must be disabled.
    assert.equal(confirmBtn.disabled, true);
    // let the async chain settle so there are no dangling promises
    await new Promise(r => setTimeout(r, 0));
    await new Promise(r => setTimeout(r, 0));
  });

  test('identity delete: Confirm button is disabled synchronously on click', async () => {
    setIdentityMode('entra', () => {});
    fetchResponder = (url, opts) => {
      if (opts && opts.method === 'DELETE') return { ok: true, status: 200, json: async () => ({ deleted: true }) };
      return { ok: true, status: 200, json: async () => ({ identities: [] }) };
    };
    const body = document.getElementById('admin-identities-body');
    const row = makeRow({ oid: '11111111-2222-3333-4444-555566667777', id: 'alice' });
    const confirmBtn = makeButtonMock({ confirmAction: 'delete-identity' }, row);

    body.dispatch('click', { target: confirmBtn });
    assert.equal(confirmBtn.disabled, true);
    await new Promise(r => setTimeout(r, 0));
    await new Promise(r => setTimeout(r, 0));
  });
});
