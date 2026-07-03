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

// ── Authenticated fetch (read-only) ─────────────────────────────────────────
// The wrapper attaches the HTTP status to thrown errors (err.status) so the
// caller can branch on 401 (auth lost) honestly. Only the read-only list fetch
// remains; drain (replay/purge) moved to the admin surface (doc 04 §3).

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

// NOTE (doc 04 §3): dead-letter DRAIN (replay/purge) is an ADMIN operation and
// lives ONLY on the admin surface. The general dashboard is READ-ONLY for dead
// letters — it fetches and renders the list (above), but never mutates. The
// replay/purge fetch wrappers, action buttons, and confirm flow were removed
// from this panel accordingly; the backend routes now live under /admin/* and
// require admin authority.

// ── DOM render functions (invoked by dashboard.js / tests) ───────────────────
// No DOM is touched at module load — only inside the functions below. These are
// all READ-ONLY: they render the invariant/totals cards and the dead-letter
// LIST. There are no mutation controls here (see doc 04 §3 note above).

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

// renderDeadLetters(entries) — render the READ-ONLY dead-letter table body
// (worker key, item count, last error, last timestamp). No action controls:
// drain (replay/purge) is admin-only and lives on the admin surface (doc 04 §3).
export function renderDeadLetters(entries) {
  const body = document.getElementById('dead-letter-body');
  if (!body) return;
  const list = entries || [];
  if (list.length === 0) {
    body.innerHTML = `<tr class="table-empty"><td class="all-clear" colspan="3">`
      + `● No dead letters — all clear</td></tr>`;
    return;
  }
  body.innerHTML = list.map(entry => {
    const d = deadLetterRowData(entry);
    const key = escapeAttr(d.workerKey);
    const err = escapeAttr(d.lastError);
    return `<tr>`
      + `<td class="mono dl-key" title="${key}">${key}</td>`
      + `<td>${escapeAttr(d.itemCount)}</td>`
      + `<td class="result-error dl-error" title="${err}">${err}</td>`
      + `<td>${escapeAttr(fmtTs(d.lastTs))}</td>`
      + `</tr>`;
  }).join('');
}

// renderDeadLetterError() — a failed dead-letter LOAD renders a distinct
// "couldn't load" row, NOT an all-clear row.
export function renderDeadLetterError() {
  const body = document.getElementById('dead-letter-body');
  if (!body) return;
  body.innerHTML = `<tr class="table-empty"><td class="result-error" colspan="3">`
    + `Couldn't load dead-letter queues — retrying…</td></tr>`;
}
