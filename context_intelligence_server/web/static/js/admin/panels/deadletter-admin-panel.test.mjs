/**
 * Tests for deadletter-admin-panel.js (P1, doc 17 §E.1 / §F.1).
 * Run with: node --test deadletter-admin-panel.test.mjs
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
let fetchResponder = null; // (url, opts) => response, or null for default 200 {}
globalThis.fetch = async (url, opts = {}) => {
  fetchCalls.push({ url, opts: opts ?? {} });
  if (fetchResponder) return fetchResponder(url, opts);
  return { ok: true, status: 200, json: async () => ({}) };
};

// ── Minimal mock document (mirrors queues-panel.test.mjs's approach: plain
//    string innerHTML for content assertions; wired sub-elements for the
//    click-delegation tests, since we don't need a real HTML parser — the
//    module's own confirmingState is the source of truth for render output,
//    not DOM-scraping). ────────────────────────────────────────────────────

function makeBody() {
  return {
    innerHTML: '',
    dataset: {},
    _listeners: {},
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

function setupDom() {
  els = { 'admin-dl-body': makeBody() };
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
    _confirmBtn: null,
    querySelector(sel) {
      if (sel === '[data-confirm-cancel]') return cell._cancelBtn;
      if (sel === '[data-confirm-action]') return cell._confirmBtn;
      return null;
    },
  };
  return cell;
}

function makeRowMock(workerKey, itemCount) {
  const cell = makeCellMock();
  return {
    dataset: { workerKey, itemCount: String(itemCount) },
    _cell: cell,
    querySelector(sel) {
      if (sel === '[data-actions]') return cell;
      return null;
    },
  };
}

// ── Import module under test (after globals are in place) ─────────────────

const mod = await import('./deadletter-admin-panel.js');
const {
  actionsCellHtml,
  confirmCellHtml,
  dlRowHtml,
  renderDeadLetters,
  renderDeadLetterError,
  refreshDeadLetters,
  wireDeadLetterActions,
  isConfirming,
  setConfirming,
  clearConfirming,
} = mod;

function resetAll() {
  fetchCalls = [];
  fetchResponder = null;
  clearConfirming('w1');
  clearConfirming('w2');
  clearConfirming('workerA');
  clearConfirming('workerB');
  setupDom();
}

beforeEach(resetAll);

// ── Row render (from deadLetterRowData, via dlRowHtml) ─────────────────────

describe('dlRowHtml() — row render', () => {
  test('renders worker_key, item_count, last_error, actions cell', () => {
    const html = dlRowHtml({
      worker_key: 'w1',
      item_count: 3,
      last_error: 'boom',
      last_ts: null,
    });
    assert.match(html, /data-worker-key="w1"/);
    assert.match(html, /class="mono dl-key"/);
    assert.match(html, />3</);
    assert.match(html, /boom/);
    assert.match(html, /data-action="replay"/);
    assert.match(html, /data-action="purge"/);
    assert.doesNotMatch(html, /data-confirming/);
  });

  test('renders the confirm cell when confirmingState has an open confirm', () => {
    setConfirming('w1', 'purge');
    const html = dlRowHtml({ worker_key: 'w1', item_count: 5, last_error: '', last_ts: null });
    assert.match(html, /data-confirming="purge"/);
    assert.match(html, /confirm-q/);
    assert.match(html, /This deletes 5 records/);
  });
});

describe('confirmCellHtml() — proportional friction (doc 17 §G.2(b))', () => {
  test('purge names the record count and warns it cannot be undone', () => {
    const html = confirmCellHtml('purge', 'w1', 7);
    assert.match(html, /Purge w1\? This deletes 7 records\. This cannot be undone\./);
  });

  test('purge singular record count', () => {
    const html = confirmCellHtml('purge', 'w1', 1);
    assert.match(html, /1 record\. /);
  });

  test('replay uses the plain confirm (no count)', () => {
    const html = confirmCellHtml('replay', 'w1', 3);
    assert.match(html, /Replay w1\?/);
    assert.doesNotMatch(html, /record/);
  });
});

describe('actionsCellHtml() — resting state', () => {
  test('Replay is btn-primary, Purge is btn-danger', () => {
    const html = actionsCellHtml('w1', 3);
    assert.match(html, /class="btn btn-primary" data-action="replay"/);
    assert.match(html, /class="btn btn-danger" data-action="purge"/);
  });
});

// ── renderDeadLetters — empty / error / content states ─────────────────────

describe('renderDeadLetters() — empty vs load-error rows distinct', () => {
  test('empty list renders all-clear (teal), not an error', () => {
    renderDeadLetters([]);
    const body = document.getElementById('admin-dl-body');
    assert.match(body.innerHTML, /all-clear/);
    assert.match(body.innerHTML, /No dead letters/);
    assert.doesNotMatch(body.innerHTML, /result-error/);
  });

  test('renderDeadLetterError() renders a distinct "couldn\'t load" row', () => {
    renderDeadLetterError();
    const body = document.getElementById('admin-dl-body');
    assert.match(body.innerHTML, /result-error/);
    assert.match(body.innerHTML, /Couldn't load/);
    assert.doesNotMatch(body.innerHTML, /all-clear/);
  });

  test('non-empty list renders one row per entry', () => {
    renderDeadLetters([
      { worker_key: 'w1', item_count: 1, last_error: 'e1', last_ts: null },
      { worker_key: 'w2', item_count: 2, last_error: 'e2', last_ts: null },
    ]);
    const body = document.getElementById('admin-dl-body');
    assert.match(body.innerHTML, /w1/);
    assert.match(body.innerHTML, /w2/);
  });
});

// ── refreshDeadLetters — 401 routes to onAuthLost, else renders error ──────

describe('refreshDeadLetters()', () => {
  test('401 calls onAuthLost, does not render the generic error row', async () => {
    fetchResponder = () => ({ ok: false, status: 401, json: async () => ({}) });
    let authLostCalled = false;
    await refreshDeadLetters(() => {
      authLostCalled = true;
    });
    assert.equal(authLostCalled, true);
  });

  test('non-401 failure renders the load-error row', async () => {
    fetchResponder = () => ({ ok: false, status: 500, json: async () => ({}) });
    await refreshDeadLetters(() => {});
    const body = document.getElementById('admin-dl-body');
    assert.match(body.innerHTML, /Couldn't load/);
  });
});

// ── wireDeadLetterActions — confirm swap / focus / escape / endpoint calls ─

describe('wireDeadLetterActions() — confirm swap', () => {
  test('clicking Replay sets data-confirming and focuses Cancel', () => {
    const body = document.getElementById('admin-dl-body');
    wireDeadLetterActions(body, () => {});

    const row = makeRowMock('w1', 3);
    const cancelBtn = { focus() { this._focused = true; } };
    row._cell._cancelBtn = cancelBtn;
    const replayBtn = makeButtonMock({ action: 'replay', workerKey: 'w1' }, row);

    body.dispatch('click', { target: replayBtn });

    assert.equal(row.dataset.confirming, 'replay');
    assert.equal(isConfirming('w1'), true);
    assert.match(row._cell.innerHTML, /confirm-q/);
    assert.equal(cancelBtn._focused, true);
    clearConfirming('w1');
  });

  test('Escape cancels an open confirm', () => {
    const body = document.getElementById('admin-dl-body');
    wireDeadLetterActions(body, () => {});

    const row = makeRowMock('w1', 3);
    row.dataset.confirming = 'replay';
    setConfirming('w1', 'replay');
    row._cell.innerHTML = confirmCellHtml('replay', 'w1', 3);

    const escTarget = { closest: sel => (sel === 'tr[data-confirming]' ? row : null) };
    body.dispatch('keydown', { key: 'Escape', target: escTarget });

    assert.equal(row.dataset.confirming, undefined);
    assert.equal(isConfirming('w1'), false);
    assert.match(row._cell.innerHTML, /data-action="replay"/);
  });

  test('non-Escape key is a no-op', () => {
    const body = document.getElementById('admin-dl-body');
    wireDeadLetterActions(body, () => {});
    const row = makeRowMock('w1', 3);
    row.dataset.confirming = 'replay';
    setConfirming('w1', 'replay');
    const target = { closest: () => row };
    body.dispatch('keydown', { key: 'Enter', target });
    assert.equal(row.dataset.confirming, 'replay'); // unchanged
    clearConfirming('w1');
  });

  test('confirming Replay calls the replay endpoint and shows the result badge', async () => {
    fetchResponder = url => {
      assert.match(url, /\/admin\/queues\/dead-letter\/w1\/replay/);
      return { ok: true, status: 200, json: async () => ({ worker_key: 'w1', replayed: 4 }) };
    };
    // second call (refreshDeadLetters) fetches the list
    let callCount = 0;
    const origResponder = fetchResponder;
    fetchResponder = (url, opts) => {
      callCount += 1;
      if (callCount === 1) return origResponder(url, opts);
      return { ok: true, status: 200, json: async () => ({ dead_letters: [] }) };
    };

    const body = document.getElementById('admin-dl-body');
    wireDeadLetterActions(body, () => {});
    const row = makeRowMock('w1', 3);
    setConfirming('w1', 'replay');
    const confirmBtn = makeButtonMock({ confirmAction: 'replay', workerKey: 'w1' }, row);
    row._cell._confirmBtn = confirmBtn;

    body.dispatch('click', { target: confirmBtn });
    // allow the async handler to complete
    await new Promise(r => setTimeout(r, 0));
    await new Promise(r => setTimeout(r, 0));

    assert.match(row._cell.innerHTML, /Replayed 4/);
    assert.equal(isConfirming('w1'), false);
  });

  test('confirming Purge calls the purge endpoint and shows the result badge', async () => {
    let callCount = 0;
    fetchResponder = url => {
      callCount += 1;
      if (callCount === 1) {
        assert.match(url, /\/admin\/queues\/dead-letter\/w2\/purge/);
        return { ok: true, status: 200, json: async () => ({ worker_key: 'w2', purged: 9 }) };
      }
      return { ok: true, status: 200, json: async () => ({ dead_letters: [] }) };
    };

    const body = document.getElementById('admin-dl-body');
    wireDeadLetterActions(body, () => {});
    const row = makeRowMock('w2', 9);
    setConfirming('w2', 'purge');
    const confirmBtn = makeButtonMock({ confirmAction: 'purge', workerKey: 'w2' }, row);
    row._cell._confirmBtn = confirmBtn;

    body.dispatch('click', { target: confirmBtn });
    await new Promise(r => setTimeout(r, 0));
    await new Promise(r => setTimeout(r, 0));

    assert.match(row._cell.innerHTML, /Purged 9/);
  });

  test('401 on confirm calls onAuthLost', async () => {
    fetchResponder = () => ({ ok: false, status: 401, json: async () => ({}) });
    let authLost = false;
    const body = document.getElementById('admin-dl-body');
    wireDeadLetterActions(body, () => {
      authLost = true;
    });
    const row = makeRowMock('w1', 3);
    setConfirming('w1', 'replay');
    const confirmBtn = makeButtonMock({ confirmAction: 'replay', workerKey: 'w1' }, row);
    row._cell._confirmBtn = confirmBtn;

    body.dispatch('click', { target: confirmBtn });
    await new Promise(r => setTimeout(r, 0));

    assert.equal(authLost, true);
  });

  test('400 on confirm shows "Invalid"', async () => {
    fetchResponder = () => ({ ok: false, status: 400, json: async () => ({}) });
    const body = document.getElementById('admin-dl-body');
    wireDeadLetterActions(body, () => {});
    const row = makeRowMock('w1', 3);
    setConfirming('w1', 'purge');
    const confirmBtn = makeButtonMock({ confirmAction: 'purge', workerKey: 'w1' }, row);
    row._cell._confirmBtn = confirmBtn;

    body.dispatch('click', { target: confirmBtn });
    await new Promise(r => setTimeout(r, 0));

    assert.match(row._cell.innerHTML, /Invalid/);
  });

  test('generic failure (e.g. 500) shows "Failed — retry"', async () => {
    fetchResponder = () => ({ ok: false, status: 500, json: async () => ({}) });
    const body = document.getElementById('admin-dl-body');
    wireDeadLetterActions(body, () => {});
    const row = makeRowMock('w1', 3);
    setConfirming('w1', 'replay');
    const confirmBtn = makeButtonMock({ confirmAction: 'replay', workerKey: 'w1' }, row);
    row._cell._confirmBtn = confirmBtn;

    body.dispatch('click', { target: confirmBtn });
    await new Promise(r => setTimeout(r, 0));

    assert.match(row._cell.innerHTML, /Failed — retry/);
  });
});

// ── Sibling-row confirm survives a post-action refresh (doc 17 §G item 10) ─

describe('sibling-row confirm survives a post-action refresh', () => {
  test('row A stays confirming while row B completes its own action', async () => {
    // Row A is independently confirming (e.g. the operator opened Replay on A).
    setConfirming('workerA', 'replay');

    let callCount = 0;
    fetchResponder = () => {
      callCount += 1;
      if (callCount === 1) {
        // B's purge action.
        return { ok: true, status: 200, json: async () => ({ worker_key: 'workerB', purged: 2 }) };
      }
      // refreshDeadLetters() re-fetches the whole list after B's action.
      return {
        ok: true,
        status: 200,
        json: async () => ({
          dead_letters: [
            { worker_key: 'workerA', item_count: 1, last_error: '', last_ts: null },
            { worker_key: 'workerB', item_count: 0, last_error: '', last_ts: null },
          ],
        }),
      };
    };

    const body = document.getElementById('admin-dl-body');
    wireDeadLetterActions(body, () => {});
    const rowB = makeRowMock('workerB', 2);
    const confirmBtnB = makeButtonMock({ confirmAction: 'purge', workerKey: 'workerB' }, rowB);
    rowB._cell._confirmBtn = confirmBtnB;

    body.dispatch('click', { target: confirmBtnB });
    // allow runAction's async chain (purge -> refreshDeadLetters -> fetch -> render) to settle
    await new Promise(r => setTimeout(r, 0));
    await new Promise(r => setTimeout(r, 0));
    await new Promise(r => setTimeout(r, 0));

    // The list refresh triggered by B's action rebuilt the WHOLE tbody — assert
    // A's row in that rebuilt output still shows its open confirm.
    assert.match(body.innerHTML, /data-worker-key="workerA" data-item-count="1" data-confirming="replay"/);
    assert.match(body.innerHTML, /Replay workerA\?/);
    // B is no longer confirming (its action completed).
    assert.doesNotMatch(body.innerHTML, /data-worker-key="workerB"[^>]*data-confirming/);

    clearConfirming('workerA');
  });
});
