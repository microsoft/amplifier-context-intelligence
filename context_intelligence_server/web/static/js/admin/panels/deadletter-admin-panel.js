// deadletter-admin-panel.js — P1, doc 17 §E.1.
//
// Extends the read-only dead-letter list already in queues-panel.js
// (fetchDeadLetters/renderDeadLetters) with admin Replay/Purge actions. Reuses
// the exported pure helper `deadLetterRowData` from queues-panel.js — does NOT
// re-author it (doc 06 machete note). Its own fetch goes through admin-api.js
// (the queues-panel fetch reads ci_api_key, the wrong credential here).
//
// Confirming state is tracked in a small module-level map (workerKey -> kind),
// NOT scraped back out of the DOM after a wholesale rebuild. This is what
// makes "a sibling row's action-triggered refresh cannot wipe an unrelated
// in-flight confirm" (doc 17 §G item 10) correct AND simple: renderDeadLetters
// just asks the map what each row's confirm state is when it rebuilds.
//
// No top-level DOM access — all DOM work lives inside the exported functions
// below (mirrors queues-panel.js:1-14).

import { deadLetterRowData } from '../../queues-panel.js';
import { fetchDeadLetters, replayWorker, purgeWorker } from '../admin-api.js';

const BODY_ID = 'admin-dl-body';

// workerKey -> 'replay' | 'purge'. Exported test seam (setConfirming /
// clearConfirming / isConfirming) doubles as the internal wiring API.
const confirmingState = new Map();

export function isConfirming(workerKey) {
  return confirmingState.has(workerKey);
}

export function setConfirming(workerKey, kind) {
  confirmingState.set(workerKey, kind);
}

export function clearConfirming(workerKey) {
  confirmingState.delete(workerKey);
}

// ── Pure helpers ────────────────────────────────────────────────────────────

function escapeAttr(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// fmtTs(ts) — replicates queues-panel.js's private formatting (not exported
// there, so mirrored here rather than reaching into its internals).
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

// actionsCellHtml(workerKey, itemCount) — the resting (non-confirming) state
// of the Actions cell: Replay + Purge buttons.
export function actionsCellHtml(workerKey, itemCount) {
  const key = escapeAttr(workerKey);
  return (
    `<button type="button" class="btn btn-primary" data-action="replay" data-worker-key="${key}">Replay</button> ` +
    `<button type="button" class="btn btn-danger" data-action="purge" data-worker-key="${key}" data-count="${escapeAttr(itemCount)}">Purge</button>`
  );
}

// confirmCellHtml(kind, workerKey, count) — the inline .confirm-q swap
// (doc 06 A.5/B.3). Purge's confirm names the record count it will destroy
// (doc 17 §G.2(b) — proportional friction for an irreversible action).
export function confirmCellHtml(kind, workerKey, count) {
  const key = escapeAttr(workerKey);
  const n = Number(count) || 0;
  const question =
    kind === 'purge'
      ? `Purge ${key}? This deletes ${n} record${n === 1 ? '' : 's'}. This cannot be undone.`
      : `Replay ${key}?`;
  const confirmBtnClass = kind === 'purge' ? 'btn btn-danger' : 'btn btn-primary';
  return (
    `<span class="confirm-q">${question}</span> ` +
    `<button type="button" class="${confirmBtnClass}" data-confirm-action="${kind}" data-worker-key="${key}">Confirm</button> ` +
    `<button type="button" class="btn" data-confirm-cancel>Cancel</button>`
  );
}

// dlRowHtml(entry) — pure row builder for a single dead-letter entry. Renders
// the confirm cell instead of the resting actions cell when confirmingState
// has an open confirm for this worker key.
export function dlRowHtml(entry) {
  const d = deadLetterRowData(entry);
  const key = escapeAttr(d.workerKey);
  const err = escapeAttr(d.lastError);
  const kind = confirmingState.get(d.workerKey);
  const cellHtml = kind
    ? confirmCellHtml(kind, d.workerKey, d.itemCount)
    : actionsCellHtml(d.workerKey, d.itemCount);
  const confirmingAttr = kind ? ` data-confirming="${kind}"` : '';
  return (
    `<tr data-worker-key="${key}" data-item-count="${escapeAttr(d.itemCount)}"${confirmingAttr}>` +
    `<td class="mono dl-key" title="${key}">${key}</td>` +
    `<td>${escapeAttr(d.itemCount)}</td>` +
    `<td class="result-error dl-error" title="${err}">${err}</td>` +
    `<td>${escapeAttr(fmtTs(d.lastTs))}</td>` +
    `<td class="actions" data-actions>${cellHtml}</td>` +
    `</tr>`
  );
}

// ── DOM render functions (invoked by admin.js / tests) ──────────────────────

// renderDeadLetters(entries) — rebuilds the table body. Each row's confirm
// state comes from confirmingState (see dlRowHtml), so a wholesale rebuild
// triggered by ONE row's completed action cannot wipe ANOTHER row's
// independently-open confirm (doc 17 §G item 10).
export function renderDeadLetters(entries) {
  const body = document.getElementById(BODY_ID);
  if (!body) return;
  const list = entries || [];
  if (list.length === 0) {
    body.innerHTML =
      `<tr class="table-empty"><td class="all-clear" colspan="5">` +
      `● No dead letters — all clear</td></tr>`;
    return;
  }
  body.innerHTML = list.map(dlRowHtml).join('');
}

// renderDeadLetterError() — a failed LOAD renders a distinct "couldn't load"
// row, never an all-clear row.
export function renderDeadLetterError() {
  const body = document.getElementById(BODY_ID);
  if (!body) return;
  body.innerHTML =
    `<tr class="table-empty"><td class="result-error" colspan="5">` +
    `Couldn't load dead-letter queues — retrying…</td></tr>`;
}

// refreshDeadLetters(onAuthLost) — fetch + render, routing a 401 to the
// shared onAuthLost hook rather than rendering a generic error.
export async function refreshDeadLetters(onAuthLost) {
  try {
    const data = await fetchDeadLetters();
    renderDeadLetters(data.dead_letters || []);
  } catch (err) {
    if (err && err.status === 401) {
      if (onAuthLost) onAuthLost();
      return;
    }
    renderDeadLetterError();
  }
}

function findActionsCell(row) {
  return row.querySelector('[data-actions]');
}

async function runAction(row, kind, onAuthLost) {
  const workerKey = row.dataset.workerKey;
  const cell = findActionsCell(row);
  const confirmBtn = cell?.querySelector('[data-confirm-action]');
  if (confirmBtn) confirmBtn.disabled = true; // guard against double-submit

  try {
    const result = kind === 'replay' ? await replayWorker(workerKey) : await purgeWorker(workerKey);
    const n = kind === 'replay' ? result.replayed : result.purged;
    const label = kind === 'replay' ? 'Replayed' : 'Purged';
    if (cell) {
      cell.innerHTML = `<span class="badge result-ok" style="display:inline-flex">${label} ${n}</span>`;
    }
    clearConfirming(workerKey);
    delete row.dataset.confirming;
    await refreshDeadLetters(onAuthLost);
  } catch (err) {
    if (err && err.status === 401) {
      clearConfirming(workerKey);
      if (onAuthLost) onAuthLost();
      return;
    }
    const msg = err && err.status === 400 ? 'Invalid' : 'Failed — retry';
    if (cell) cell.innerHTML = `<span class="result-error">${msg}</span>`;
    clearConfirming(workerKey);
    delete row.dataset.confirming;
  }
}

// wireDeadLetterActions(body, onAuthLost) — event delegation for the Replay/
// Purge/Confirm/Cancel button flow. Idempotent (guards via a dataset flag) so
// re-invoking on the same body element is a no-op.
export function wireDeadLetterActions(body, onAuthLost) {
  if (!body || body.dataset.wired === '1') return;
  body.dataset.wired = '1';

  body.addEventListener('click', e => {
    const btn = e.target.closest('button');
    if (!btn) return;
    const row = btn.closest('tr[data-worker-key]');
    if (!row) return;
    const workerKey = row.dataset.workerKey;

    if (btn.dataset.action) {
      const kind = btn.dataset.action;
      setConfirming(workerKey, kind);
      const cell = findActionsCell(row);
      if (cell) {
        cell.innerHTML = confirmCellHtml(kind, workerKey, row.dataset.itemCount);
        row.dataset.confirming = kind;
        const cancelBtn = cell.querySelector('[data-confirm-cancel]');
        if (cancelBtn) cancelBtn.focus();
      }
      return;
    }
    if (btn.dataset.confirmCancel !== undefined) {
      clearConfirming(workerKey);
      const cell = findActionsCell(row);
      if (cell) {
        delete row.dataset.confirming;
        cell.innerHTML = actionsCellHtml(workerKey, row.dataset.itemCount);
      }
      return;
    }
    if (btn.dataset.confirmAction) {
      runAction(row, btn.dataset.confirmAction, onAuthLost);
    }
  });

  body.addEventListener('keydown', e => {
    if (e.key !== 'Escape') return;
    const row = e.target.closest('tr[data-confirming]');
    if (!row) return;
    const workerKey = row.dataset.workerKey;
    clearConfirming(workerKey);
    const cell = findActionsCell(row);
    if (cell) {
      delete row.dataset.confirming;
      cell.innerHTML = actionsCellHtml(workerKey, row.dataset.itemCount);
    }
  });
}
