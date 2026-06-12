// queues.js - authenticated dead-letter fetch wrappers (C2)
// Wraps the /queues/dead-letter endpoints. Each wrapper attaches the HTTP
// status to thrown errors (err.status) so action handlers can branch on
// 401/400 honestly instead of guessing from the message (U2).

import { fetchStatus, authHeaders } from './api.js';

// computeInvariant(metrics) — pure helper for the invariant card (C3).
// Keys calm/loud state off the backend `degraded` flag (which the backend
// computes as residual != 0 OR dead > 0), NOT off residual===0, so the card
// and the dashboard DEGRADED hint AGREE for the dead-letter case (residual==0,
// dead>0, degraded=true). Three-way wording distinguishes the two loud cases.
export function computeInvariant(metrics) {
  const m = metrics || {};
  const accepted = m.accepted ?? 0;
  const written = m.written ?? 0;
  const inQueue = m.in_queue ?? 0;
  const dead = m.dead ?? 0;
  const residual = m.residual ?? (accepted - written - inQueue - dead);
  const degraded = (m.degraded != null) ? !!m.degraded : (residual !== 0 || dead > 0);
  const residualText = residual === 0 ? '0' : (residual > 0 ? `+${residual}` : `${residual}`);
  const off = Math.abs(residual);
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
  void degraded;
  return { equation, residualText, badgeText, badgeClass, cardClass, aria };
}

// computeTotals(metrics) — chips for the two totals NOT already shown in the
// invariant equation. Accepted/Written/In-queue/Dead-letter all appear in the
// equation (dead-letter also has its own table), so only Replayed and Write
// retries get chips here. Missing metrics default to 0.
export function computeTotals(metrics) {
  const m = metrics || {};
  return [
    { label: 'Replayed', value: m.replayed_total ?? 0 },
    { label: 'Write retries', value: m.write_retries_total ?? 0 },
  ];
}

// deadLetterRowData(entry) — maps a dead-letter API entry to row fields.
// lastTs defaults to null (absence of a timestamp), other fields to empty/0.
export function deadLetterRowData(entry) {
  const e = entry || {};
  return {
    workerKey: e.worker_key ?? '',
    itemCount: e.item_count ?? 0,
    lastError: e.last_error ?? '',
    lastTs: e.last_ts ?? null,
  };
}

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
