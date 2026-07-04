// queue-metrics-panel.js — P3, doc 17 §E.3.
//
// Reuses `computeInvariant`/`computeTotals` from queues-panel.js (exported) —
// does NOT re-author them (doc 06 machete note). No top-level DOM access.

import { computeInvariant, computeTotals } from '../../queues-panel.js';

function escapeAttr(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// renderInvariant(metrics) — mirrors the dashboard Queues tab (dashboard.html:133-140).
export function renderInvariant(metrics) {
  const inv = computeInvariant(metrics);
  const card = document.getElementById('admin-invariant-card');
  const eq = document.getElementById('admin-invariant-eq');
  const badge = document.getElementById('admin-invariant-badge');
  if (card) {
    card.className = inv.cardClass;
    card.setAttribute('aria-label', inv.aria);
  }
  if (eq) eq.textContent = inv.equation;
  if (badge) {
    badge.className = inv.badgeClass;
    badge.textContent = inv.badgeText;
  }
}

export function renderTotals(metrics) {
  const row = document.getElementById('admin-totals-row');
  if (!row) return;
  row.innerHTML = computeTotals(metrics)
    .map(t => `<span class="stat-chip">${escapeAttr(t.label)}: ${escapeAttr(t.value)}</span>`)
    .join('');
}

// renderNeo4j(status) — Bolt url + health always visible; browser url is
// str|null (doc 17 §G item 4) — guard on truthiness, never call string
// methods on it unconditionally.
export function renderNeo4j(status) {
  const s = status || {};
  const statusEl = document.getElementById('admin-neo4j-status');
  if (statusEl) {
    if (s.neo4j_connected) {
      statusEl.textContent = '\u25cf Connected';
      statusEl.style.color = 'var(--primary)';
    } else {
      statusEl.textContent = '\u25cb Disconnected';
      statusEl.style.color = 'var(--destructive)';
    }
  }
  const urlEl = document.getElementById('admin-neo4j-url');
  if (urlEl) urlEl.textContent = s.neo4j_url || '\u2014';

  const browserEl = document.getElementById('admin-neo4j-browser-url');
  if (browserEl) {
    if (s.neo4j_browser_url) {
      browserEl.textContent = s.neo4j_browser_url;
      browserEl.href = s.neo4j_browser_url;
      browserEl.style.display = '';
    } else {
      browserEl.textContent = '\u2014';
      browserEl.removeAttribute('href');
      browserEl.style.display = '';
    }
  }
}

// renderDegraded(metrics) — doc 06 B.5: highlight per-worker dead-letter rows
// when the pipeline is degraded (residual != 0 or dead-letters present).
// Applies `tr.degraded` to every row currently in the dead-letter table body.
export function renderDegraded(metrics) {
  const inv = computeInvariant(metrics);
  const degraded = inv.cardClass.includes('degraded');
  const body = document.getElementById('admin-dl-body');
  if (!body) return degraded;
  for (const row of body.querySelectorAll('tr[data-worker-key]')) {
    row.classList.toggle('degraded', degraded);
  }
  return degraded;
}

// renderQueueMetrics(status) — the entry point admin.js calls on every
// refresh. Receives the WHOLE /status object (mirrors queues-panel.js
// renderQueues), extracts .metrics itself.
export function renderQueueMetrics(status) {
  const metrics = (status && status.metrics) || {};
  renderInvariant(metrics);
  renderTotals(metrics);
  renderNeo4j(status);
  renderDegraded(metrics);
}

// ── Refresh freshness signal (council fix 3) ───────────────────────────────
// The /status poll drives every panel; a silent failure used to be a bare
// console.error. These render a VISIBLE "Updated <time>" / "Refresh failed"
// signal into #admin-refresh-status so an operator can tell live data from
// stale. Reuses .card-description (ok) and .result-error (failure) — no new
// component.

export function renderRefreshOk(ts = Date.now()) {
  const el = document.getElementById('admin-refresh-status');
  if (!el) return;
  el.className = 'card-description';
  let stamp;
  try {
    stamp = new Date(ts).toLocaleTimeString();
  } catch {
    stamp = '';
  }
  el.textContent = stamp ? `Updated ${stamp}` : 'Updated';
}

export function renderRefreshError() {
  const el = document.getElementById('admin-refresh-status');
  if (!el) return;
  el.className = 'result-error';
  el.textContent = 'Refresh failed — retrying…';
}
