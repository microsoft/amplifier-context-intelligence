// queues-panel.js — node-importable queues panel (C2).
//
// Module-split decision (load-bearing): there is NO top-level DOM access in
// this file. Every DOM operation lives inside an exported function that
// dashboard.js invokes. That makes the module import-safe under node so the
// behavioral tests can drive its functions directly with a mock document —
// the import-based discipline that was missing before.
//
// BINDING FIX (the core driver of this task): computeInvariant reads the REAL
// /status.metrics field names (*_total), and renderQueues receives the WHOLE
// /status object and extracts .metrics itself. The prior code read short names
// (m.accepted) and passed the whole /status object where .metrics was
// expected — both produced a silent "0 − 0 − 0 − 0" all-clear that masked
// real dead-letter / off-by states.
//
// The REAL verified /status.metrics shape (nested under /status as .metrics):
//   { accepted_total, written_total, replayed_total, write_retries_total,
//     in_queue_total, dead_letter_total, residual, degraded }

import { authHeaders } from './api.js';

// ── Pure helpers (safe in any environment) ──────────────────────────────────

// computeInvariant(metrics) — invariant card model from the REAL *_total shape.
//
// residual is computed LOCALLY as accepted − written − in_queue − dead from the
// individually-read *_total fields (NOT from m.residual), so every number in
// the displayed equation is bound to a real field — that is what makes the
// short-name bug provably RED. loud/calm is derived LOCALLY from residual/dead
// (NOT from m.degraded), so the card and the dashboard hint agree on the
// dead-letter case (residual==0, dead>0).
export function computeInvariant(metrics) {
  const m = metrics || {};
  const accepted = m.accepted_total ?? 0;
  const written = m.written_total ?? 0;
  const inQueue = m.in_queue_total ?? 0;
  const dead = m.dead_letter_total ?? 0;
  const residual = accepted - written - inQueue - dead;
  const off = Math.abs(residual);
  const residualText = residual === 0 ? '0' : (residual > 0 ? `+${residual}` : `${residual}`);
  const equation = `${accepted} − ${written} − ${inQueue} − ${dead} = ${residualText}`;

  let badgeText, badgeClass, cardClass, aria;
  if (residual !== 0) {
    badgeText = `OFF BY ${off} — INVESTIGATE`;
    badgeClass = 'badge badge-error';
    cardClass = 'card invariant degraded';
    aria = `Pipeline off by ${off} events — investigate possible event loss`;
  } else if (dead > 0) {
    badgeText = `${dead} DEAD-LETTERED`;
    badgeClass = 'badge badge-error';
    cardClass = 'card invariant degraded';
    aria = `${dead} events dead-lettered — accounted for, needs attention`;
  } else {
    badgeText = 'Balanced ✓';
    badgeClass = 'badge badge-primary';
    cardClass = 'card invariant';
    aria = 'Pipeline balanced';
  }
  return { equation, residualText, badgeText, badgeClass, cardClass, aria };
}

// computeTotals(metrics) — chips for the two totals NOT already shown in the
// invariant equation (Replayed, Write retries). Missing metrics default to 0.
export function computeTotals(metrics) {
  const m = metrics || {};
  return [
    { label: 'Replayed', value: m.replayed_total ?? 0 },
    { label: 'Write retries', value: m.write_retries_total ?? 0 },
  ];
}

// deadLetterRowData(entry) — map a dead-letter API entry to row fields.
export function deadLetterRowData(entry) {
  const e = entry || {};
  return {
    workerKey: e.worker_key ?? '',
    itemCount: e.item_count ?? 0,
    lastError: e.last_error ?? '',
    lastTs: e.last_ts ?? null,
  };
}

// escapeAttr(s) — escape the five characters that break HTML attribute/text
// contexts. Worker keys and error strings are server-supplied.
function escapeAttr(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// fmtTs(ts) — render a unix-seconds timestamp; null/'' → em-dash.
function fmtTs(ts) {
  if (ts === null || ts === '' || ts === undefined) return '—';
  const n = Number(ts);
  if (!Number.isFinite(n) || n <= 0) return '—';
  try {
    return new Date(n * 1000).toLocaleString();
  } catch {
    return '—';
  }
}

// ── Authenticated fetch wrappers ────────────────────────────────────────────
// Each wrapper attaches the HTTP status to thrown errors (err.status) so
// action handlers can branch on 401/400 honestly. worker_key is
// encodeURIComponent'd defensively (it is a file stem: a session UUID or a
// _no_session__<workspace-slug>); an unsafe key yields HTTP 400 from the server.

function _httpError(label, res) {
  const err = new Error(`${label} failed: ${res.status}`);
  err.status = res.status;
  return err;
}

export async function fetchDeadLetters() {
  const res = await fetch('/queues/dead-letter', { headers: authHeaders() });
  if (!res.ok) throw _httpError('dead-letter list', res);
  return res.json();
}

export async function replayWorker(workerKey) {
  const url = `/queues/dead-letter/${encodeURIComponent(workerKey)}/replay`;
  const res = await fetch(url, { method: 'POST', headers: authHeaders() });
  if (!res.ok) throw _httpError('replay', res);
  return res.json();
}

export async function purgeWorker(workerKey) {
  const url = `/queues/dead-letter/${encodeURIComponent(workerKey)}/purge`;
  const res = await fetch(url, { method: 'POST', headers: authHeaders() });
  if (!res.ok) throw _httpError('purge', res);
  return res.json();
}

// ── DOM render functions (invoked by dashboard.js / tests) ───────────────────
// Module-scoped state for the last rendered entries, used to re-render the
// table after an inline Purge confirm is cancelled. No DOM is touched at module
// load — only inside the functions below.

let lastEntries = [];

// renderQueues(status) — the load-bearing entry point. Receives the WHOLE
// /status object and extracts status.metrics ITSELF (do not pass .metrics in).
export function renderQueues(status) {
  const metrics = (status && status.metrics) || {};
  renderInvariant(metrics);
  renderTotals(metrics);
}

// renderInvariant(metrics) — equation tail carries the number, badge carries
// the word/colour.
function renderInvariant(metrics) {
  const inv = computeInvariant(metrics);
  const card = document.getElementById('invariant-card');
  const eq = document.getElementById('invariant-eq');
  const badge = document.getElementById('invariant-badge');
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

// renderTotals(metrics) — build the totals chips into #totals-row.
function renderTotals(metrics) {
  const row = document.getElementById('totals-row');
  if (!row) return;
  row.innerHTML = computeTotals(metrics)
    .map(t => `<span class="stat-chip">${escapeAttr(t.label)}: ${escapeAttr(t.value)}</span>`)
    .join('');
}

// actionsCellHtml(d) — Replay + Purge buttons for one dead-letter row.
function actionsCellHtml(d) {
  const key = escapeAttr(d.workerKey);
  return `<td class="actions" data-key="${key}">`
    + `<button class="btn btn-primary" data-action="replay" data-key="${key}" `
    + `aria-label="Replay dead-lettered events for ${key}">Replay</button>`
    + `<button class="btn btn-danger" data-action="purge" data-key="${key}" `
    + `aria-label="Purge dead-lettered events for ${key}">Purge</button>`
    + `</td>`;
}

// renderDeadLetters(entries) — render the dead-letter table body. Poll guard:
// if an inline Purge confirm is open, bail so the 3s poll cannot wipe an
// irreversible-action confirmation out from under the user.
export function renderDeadLetters(entries) {
  const body = document.getElementById('dead-letter-body');
  if (!body) return;
  if (body.querySelector('.actions[data-confirming]')) return; // poll guard
  lastEntries = entries || [];
  if (lastEntries.length === 0) {
    body.innerHTML = `<tr class="table-empty"><td class="all-clear" colspan="4">`
      + `● No dead letters — all clear</td></tr>`;
    return;
  }
  body.innerHTML = lastEntries.map(entry => {
    const d = deadLetterRowData(entry);
    const key = escapeAttr(d.workerKey);
    const err = escapeAttr(d.lastError);
    return `<tr>`
      + `<td class="mono dl-key" title="${key}">${key}</td>`
      + `<td>${escapeAttr(d.itemCount)}</td>`
      + `<td class="result-error dl-error" title="${err}">${err}</td>`
      + `<td>${escapeAttr(fmtTs(d.lastTs))}</td>`
      + actionsCellHtml(d)
      + `</tr>`;
  }).join('');
}

// renderDeadLetterError() — a failed dead-letter LOAD renders a distinct
// "couldn't load" row, NOT an all-clear row. Respects the poll guard.
export function renderDeadLetterError() {
  const body = document.getElementById('dead-letter-body');
  if (!body) return;
  if (body.querySelector('.actions[data-confirming]')) return; // poll guard
  body.innerHTML = `<tr class="table-empty"><td class="result-error" colspan="4">`
    + `Couldn't load dead-letter queues — retrying…</td></tr>`;
}

// showRowBadge(workerKey, text, cls) — replace a row's actions cell with a
// single status badge (honest feedback after an action completes).
export function showRowBadge(workerKey, text, cls) {
  const body = document.getElementById('dead-letter-body');
  if (!body) return;
  const cell = body.querySelector(`.actions[data-key="${escapeAttr(workerKey)}"]`);
  if (cell) cell.innerHTML = `<span class="badge ${cls}">${escapeAttr(text)}</span>`;
}

// handleActionError(err, workerKey, onAuthLost) — error honesty.
// 401 → auth lost; 400 → distinct 'Invalid'; else → 'Failed — retry'.
export function handleActionError(err, workerKey, onAuthLost) {
  if (err && err.status === 401) {
    if (typeof onAuthLost === 'function') onAuthLost();
  } else if (err && err.status === 400) {
    showRowBadge(workerKey, 'Invalid', 'badge-error');
  } else {
    showRowBadge(workerKey, 'Failed — retry', 'badge-error');
  }
}

// beginPurgeConfirm(cell, workerKey) — replace the action buttons with an
// inline confirm (Purge is irreversible). Marks the cell data-confirming so
// the poll guard leaves it alone, and moves focus to Cancel (Focus-to-Cancel).
export function beginPurgeConfirm(cell, workerKey) {
  if (!cell) return;
  const key = escapeAttr(workerKey);
  cell.setAttribute('data-confirming', '1');
  cell.innerHTML = `<span class="confirm-q">Purge ${key}?</span>`
    + `<button class="btn btn-danger" data-action="purge-confirm" data-key="${key}">Confirm</button>`
    + `<button class="btn" data-action="purge-cancel" data-key="${key}" id="purge-cancel">Cancel</button>`;
  const cancel = cell.querySelector('#purge-cancel');
  if (cancel) cancel.focus();
}

// wireDeadLetterActions({onAuthLost}) — attach delegated click/keydown handlers
// to #dead-letter-body for replay / purge / purge-confirm / purge-cancel.
// Escape cancels an open confirm.
export function wireDeadLetterActions({ onAuthLost } = {}) {
  const body = document.getElementById('dead-letter-body');
  if (!body) return;

  body.addEventListener('click', async (ev) => {
    const btn = ev.target.closest('button[data-action]');
    if (!btn) return;
    const action = btn.getAttribute('data-action');
    const key = btn.getAttribute('data-key');
    const cell = btn.closest('.actions');

    if (action === 'replay') {
      try {
        const r = await replayWorker(key);
        showRowBadge(key, `Re-enqueued ${r.replayed ?? 0}`, 'badge-primary');
      } catch (err) {
        handleActionError(err, key, onAuthLost);
      }
    } else if (action === 'purge') {
      beginPurgeConfirm(cell, key);
    } else if (action === 'purge-confirm') {
      try {
        const r = await purgeWorker(key);
        showRowBadge(key, `Purged ${r.purged ?? 0}`, 'badge-primary');
      } catch (err) {
        handleActionError(err, key, onAuthLost);
      }
    } else if (action === 'purge-cancel') {
      renderDeadLetters(lastEntries);
    }
  });

  body.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape' && body.querySelector('.actions[data-confirming]')) {
      renderDeadLetters(lastEntries);
    }
  });
}
