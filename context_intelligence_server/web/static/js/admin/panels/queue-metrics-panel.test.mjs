/**
 * Tests for queue-metrics-panel.js (P3, doc 17 §E.3 / §F.1).
 * Run with: node --test queue-metrics-panel.test.mjs
 * Node.js built-in test runner (no dependencies required).
 */

import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

// ── Minimal mock document ───────────────────────────────────────────────────

function makeElement(id) {
  return {
    id,
    className: '',
    textContent: '',
    innerHTML: '',
    href: '',
    style: { color: '', display: '' },
    _attrs: {},
    setAttribute(k, v) {
      this._attrs[k] = v;
    },
    getAttribute(k) {
      return this._attrs[k] ?? null;
    },
    removeAttribute(k) {
      delete this._attrs[k];
    },
    classList: {
      _set: new Set(),
      toggle(cls, on) {
        if (on) this._set.add(cls);
        else this._set.delete(cls);
      },
      contains(cls) {
        return this._set.has(cls);
      },
    },
    querySelectorAll() {
      return [];
    },
  };
}

let els = {};
globalThis.document = {
  getElementById: id => els[id] || null,
};

function setupDom() {
  els = {};
  for (const id of [
    'admin-invariant-card',
    'admin-invariant-eq',
    'admin-invariant-badge',
    'admin-totals-row',
    'admin-neo4j-status',
    'admin-neo4j-url',
    'admin-neo4j-browser-url',
    'admin-dl-body',
  ]) {
    els[id] = makeElement(id);
  }
}

const mod = await import('./queue-metrics-panel.js');
const { renderInvariant, renderTotals, renderNeo4j, renderDegraded, renderQueueMetrics } = mod;

beforeEach(setupDom);

// ── computeInvariant/computeTotals reuse (not re-authored) ─────────────────

describe('renderInvariant() — reuses queues-panel.js computeInvariant', () => {
  test('balanced pipeline renders "Balanced ✓" and the calm card class', () => {
    renderInvariant({
      accepted_total: 10,
      written_total: 10,
      replayed_total: 0,
      write_retries_total: 0,
      in_queue_total: 0,
      dead_letter_total: 0,
      residual: 0,
      degraded: false,
    });
    const card = document.getElementById('admin-invariant-card');
    const badge = document.getElementById('admin-invariant-badge');
    assert.equal(card.className, 'card invariant');
    assert.equal(badge.textContent, 'Balanced ✓');
  });

  test('dead-lettered pipeline renders the degraded card class', () => {
    renderInvariant({
      accepted_total: 12,
      written_total: 11,
      replayed_total: 0,
      write_retries_total: 0,
      in_queue_total: 0,
      dead_letter_total: 1,
      residual: 0,
      degraded: true,
    });
    const card = document.getElementById('admin-invariant-card');
    assert.equal(card.className, 'card invariant degraded');
  });
});

describe('renderTotals()', () => {
  test('renders Replayed and Write retries chips', () => {
    renderTotals({ replayed_total: 3, write_retries_total: 2 });
    const row = document.getElementById('admin-totals-row');
    assert.match(row.innerHTML, /Replayed: 3/);
    assert.match(row.innerHTML, /Write retries: 2/);
  });
});

// ── Neo4j visibility (Bolt always, browser url str|null) ───────────────────

describe('renderNeo4j() — browser-url str|null guard (doc 17 §G item 4)', () => {
  test('Bolt url and connection status always render', () => {
    renderNeo4j({ neo4j_connected: true, neo4j_url: 'bolt://host:7687', neo4j_browser_url: null });
    assert.equal(document.getElementById('admin-neo4j-url').textContent, 'bolt://host:7687');
    assert.match(document.getElementById('admin-neo4j-status').textContent, /Connected/);
  });

  test('null browser url renders em-dash, never throws calling a string method', () => {
    assert.doesNotThrow(() => {
      renderNeo4j({ neo4j_connected: true, neo4j_url: 'bolt://x', neo4j_browser_url: null });
    });
    const el = document.getElementById('admin-neo4j-browser-url');
    assert.equal(el.textContent, '—');
    assert.equal(el.getAttribute('href'), null);
  });

  test('a non-null browser url is shown and set as href', () => {
    renderNeo4j({ neo4j_connected: true, neo4j_url: 'bolt://x', neo4j_browser_url: 'http://host:7474' });
    const el = document.getElementById('admin-neo4j-browser-url');
    assert.equal(el.textContent, 'http://host:7474');
    assert.equal(el.href, 'http://host:7474');
  });

  test('disconnected shows the disconnected glyph and destructive color', () => {
    renderNeo4j({ neo4j_connected: false, neo4j_url: 'bolt://x', neo4j_browser_url: null });
    const statusEl = document.getElementById('admin-neo4j-status');
    assert.match(statusEl.textContent, /Disconnected/);
    assert.equal(statusEl.style.color, 'var(--destructive)');
  });
});

// ── Degraded highlight ──────────────────────────────────────────────────────

describe('renderDegraded()', () => {
  test('returns true and would mark rows degraded when residual != 0', () => {
    const degraded = renderDegraded({
      accepted_total: 10,
      written_total: 8,
      in_queue_total: 1,
      dead_letter_total: 0,
      residual: 1,
    });
    assert.equal(degraded, true);
  });

  test('returns false for a balanced pipeline', () => {
    const degraded = renderDegraded({
      accepted_total: 10,
      written_total: 10,
      in_queue_total: 0,
      dead_letter_total: 0,
      residual: 0,
    });
    assert.equal(degraded, false);
  });
});

describe('renderQueueMetrics() — entry point receives the WHOLE /status object', () => {
  test('extracts .metrics itself (mirrors queues-panel.js renderQueues)', () => {
    renderQueueMetrics({
      metrics: {
        accepted_total: 5,
        written_total: 5,
        replayed_total: 0,
        write_retries_total: 0,
        in_queue_total: 0,
        dead_letter_total: 0,
        residual: 0,
        degraded: false,
      },
      neo4j_connected: true,
      neo4j_url: 'bolt://x',
      neo4j_browser_url: null,
    });
    assert.equal(document.getElementById('admin-invariant-badge').textContent, 'Balanced ✓');
    assert.equal(document.getElementById('admin-neo4j-url').textContent, 'bolt://x');
  });

  test('missing .metrics defaults to {} without throwing', () => {
    assert.doesNotThrow(() => renderQueueMetrics({}));
  });
});
