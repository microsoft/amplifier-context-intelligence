// identity-panel.js — P2, doc 17 §E.2 (mode-aware: API keys OR OID identities).
//
// Reads `status.auth.mode` (surfaced by queue-metrics-panel's /status fetch,
// wired by admin.js) to decide which store is LIVE. The other mode's store
// always 503s (admin.py:325-329,339-343) — rather than firing a doomed
// request, the inactive card renders the doc 06 B.6 "Not configured in this
// auth mode" state proactively (doc 17 §C consequence 1 / §E.2).
//
// No top-level DOM access — all DOM work lives inside exported functions.

import {
  listKeys,
  putKey,
  deleteKey,
  listIdentities,
  putIdentity,
  deleteIdentity,
} from '../admin-api.js';
import { refreshWithErrorState } from '../admin-refresh.js';

const GUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const ALL_ZEROS_GUID = '00000000-0000-0000-0000-000000000000';
const HASH_RE = /^[0-9a-f]{64}$/;

// ── Pure helpers ────────────────────────────────────────────────────────────

function escapeAttr(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// normalizeGuid / normalizeHash — doc 17 §G item 9: Azure Portal shows OIDs in
// UPPERCASE, Windows Get-FileHash emits UPPERCASE SHA-256. The server compares
// against lowercase patterns, so the client MUST normalize to lowercase before
// validating/submitting (accept uppercase, never falsely flag it invalid).
export function normalizeGuid(v) {
  return String(v ?? '').trim().toLowerCase();
}

export function isValidGuid(v) {
  const n = normalizeGuid(v);
  return GUID_RE.test(n) && n !== ALL_ZEROS_GUID;
}

export function normalizeHash(v) {
  return String(v ?? '').trim().toLowerCase();
}

export function isValidHash(v) {
  return HASH_RE.test(normalizeHash(v));
}

// ── Confirm state (module-level, mirrors deadletter-admin-panel.js) ────────
// Tracked in maps, not scraped back out of the DOM, so a wholesale tbody
// rebuild (after a mutation) cannot wipe an unrelated row's open confirm.

const keysConfirming = new Set(); // hash values currently confirming delete
const identitiesConfirming = new Set(); // oid values currently confirming delete

// ── Row builders (pure) ──────────────────────────────────────────────────────

// actionsButtonsHtml(kind, key, id, display) — the resting actions cell:
// [Edit] (.btn) + [Delete] (.btn-danger). Edit prefills the add/update form
// with this row's values (council fix 2 — spec §E.2 `.btn Edit + .btn-danger
// Delete`, and it removes the hand-transcribe-a-GUID hazard). Used both by the
// row builders AND by the confirm-cancel/Escape rebuild so cancelling a delete
// restores BOTH buttons, not just Delete.
function actionsButtonsHtml(kind, key, id, display) {
  const entity = kind === 'delete-key' ? 'key' : 'identity';
  const k = escapeAttr(key);
  const idAttr = escapeAttr(id ?? '');
  if (kind === 'delete-key') {
    return (
      `<button type="button" class="btn" data-action="edit-key" data-hash="${k}" data-id="${idAttr}">Edit</button> ` +
      `<button type="button" class="btn btn-danger" data-action="delete-key" data-hash="${k}">Delete</button>`
    );
  }
  const displayAttr = escapeAttr(display ?? '');
  return (
    `<button type="button" class="btn" data-action="edit-${entity}" data-oid="${k}" data-id="${idAttr}" data-display="${displayAttr}">Edit</button> ` +
    `<button type="button" class="btn btn-danger" data-action="delete-identity" data-oid="${k}">Delete</button>`
  );
}

export function keyRowHtml(entry) {
  const hash = escapeAttr(entry.hash ?? '');
  const id = escapeAttr(entry.id ?? '');
  const cell = keysConfirming.has(entry.hash)
    ? deleteConfirmHtml('delete-key', entry.hash, entry.hash)
    : actionsButtonsHtml('delete-key', entry.hash ?? '', entry.id ?? '');
  const confirmingAttr = keysConfirming.has(entry.hash) ? ' data-confirming="1"' : '';
  return (
    `<tr data-hash="${hash}" data-id="${id}"${confirmingAttr}>` +
    `<td class="mono dl-key" title="${hash}">${hash}</td>` +
    `<td>${id}</td>` +
    `<td class="actions" data-actions>${cell}</td>` +
    `</tr>`
  );
}

export function identityRowHtml(entry) {
  const oid = escapeAttr(entry.oid ?? '');
  const id = escapeAttr(entry.id ?? '');
  const display = escapeAttr(entry.display_name ?? '');
  const cell = identitiesConfirming.has(entry.oid)
    ? deleteConfirmHtml('delete-identity', entry.oid, entry.oid)
    : actionsButtonsHtml('delete-identity', entry.oid ?? '', entry.id ?? '', entry.display_name ?? '');
  const confirmingAttr = identitiesConfirming.has(entry.oid) ? ' data-confirming="1"' : '';
  return (
    `<tr data-oid="${oid}" data-id="${id}" data-display="${display}"${confirmingAttr}>` +
    `<td class="mono" title="${oid}">${oid}</td>` +
    `<td>${id}</td>` +
    `<td>${display || '—'}</td>` +
    `<td class="actions" data-actions>${cell}</td>` +
    `</tr>`
  );
}

// notConfiguredHtml(label) — doc 06 B.6 "Not configured in this auth mode"
// card. NOT an error state — the store is simply inactive for this mode.
export function notConfiguredHtml(label) {
  return (
    `<span class="pill admin">Not configured</span>` +
    `<p class="card-description">${escapeAttr(label)} are managed in the other auth mode — ` +
    `this server is not running with that mode active.</p>`
  );
}

function deleteConfirmHtml(kind, key, label) {
  const k = escapeAttr(key);
  return (
    `<span class="confirm-q">Delete ${escapeAttr(label)}?</span> ` +
    `<button type="button" class="btn btn-danger" data-confirm-action="${kind}" data-key="${k}">Confirm</button> ` +
    `<button type="button" class="btn" data-confirm-cancel>Cancel</button>`
  );
}

// ── Live card markup (built once when a mode becomes active) ───────────────

function keysLiveHtml() {
  return (
    `<div class="table-scroll"><table class="data-table">` +
    `<thead><tr><th>Hash</th><th>Contributor</th><th>Actions</th></tr></thead>` +
    `<tbody id="admin-keys-body"></tbody></table></div>` +
    `<div class="actions" style="margin-top:1rem;align-items:flex-start;">` +
    `<div><input type="text" class="input" id="key-hash-input" placeholder="sha256 hash (64 hex)" />` +
    `<div class="field-error" id="key-hash-error" style="display:none;">Not a valid sha256 hash (64 hex characters)</div></div>` +
    `<input type="text" class="input" id="key-id-input" placeholder="contributor id" />` +
    `<button type="button" class="btn btn-primary" id="key-add-btn">Add key</button>` +
    `</div>` +
    `<div class="card-description" style="margin-top:0.5rem;line-height:1.7;">` +
    `Paste the <strong>sha256 hash</strong>, not the raw key — the server never sees the raw key. Hash it yourself:<br>` +
    `Linux/macOS: <span class="mono">printf %s "&lt;key&gt;" | sha256sum</span><br>` +
    `PowerShell: <span class="mono">[BitConverter]::ToString([Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes("&lt;key&gt;"))).Replace("-","").ToLower()</span><br>` +
    `<em>Use <span class="mono">printf</span> (not <span class="mono">echo</span>) — a trailing newline changes the digest.</em>` +
    `</div>` +
    `<div class="result-error" id="key-form-error" style="display:none;"></div>`
  );
}

function identitiesLiveHtml() {
  return (
    `<div class="table-scroll"><table class="data-table">` +
    `<thead><tr><th>OID</th><th>Contributor</th><th>Display name</th><th>Actions</th></tr></thead>` +
    `<tbody id="admin-identities-body"></tbody></table></div>` +
    `<div class="actions" style="margin-top:1rem;align-items:flex-start;">` +
    `<div><input type="text" class="input" id="oid-input" placeholder="OID (GUID)" />` +
    `<div class="field-error" id="oid-error" style="display:none;">Not a valid GUID</div></div>` +
    `<input type="text" class="input" id="oid-id-input" placeholder="contributor id" />` +
    `<input type="text" class="input" id="oid-display-input" placeholder="display name (optional)" />` +
    `<button type="button" class="btn btn-primary" id="oid-add-btn">Add / update</button>` +
    `</div>` +
    `<div class="result-error" id="oid-form-error" style="display:none;"></div>`
  );
}

// ── Table body render (re-invoked on refresh) ───────────────────────────────
// Each row's confirm state comes from keysConfirming/identitiesConfirming
// (see keyRowHtml/identityRowHtml), so a wholesale rebuild triggered by ONE
// row's completed action cannot wipe ANOTHER row's independently-open
// confirm (doc 17 §G item 10 — same discipline as deadletter-admin-panel.js).

export function renderKeys(entries) {
  const body = document.getElementById('admin-keys-body');
  if (!body) return;
  const list = entries || [];
  body.innerHTML =
    list.length === 0
      ? `<tr class="table-empty"><td colspan="3">No API keys registered</td></tr>`
      : list.map(keyRowHtml).join('');
}

export function renderIdentities(entries) {
  const body = document.getElementById('admin-identities-body');
  if (!body) return;
  const list = entries || [];
  body.innerHTML =
    list.length === 0
      ? `<tr class="table-empty"><td colspan="4">No OID identities registered</td></tr>`
      : list.map(identityRowHtml).join('');
}

// Non-401 load-failure rows — a DISTINCT error state (not an empty table),
// mirroring deadletter's renderDeadLetterError discipline (council fix 3). An
// empty table would falsely imply "zero mappings"; this says "couldn't load".

export function renderKeysError() {
  const body = document.getElementById('admin-keys-body');
  if (!body) return;
  body.innerHTML =
    `<tr class="table-empty"><td class="result-error" colspan="3">` +
    `Couldn't load API keys — retrying…</td></tr>`;
}

export function renderIdentitiesError() {
  const body = document.getElementById('admin-identities-body');
  if (!body) return;
  body.innerHTML =
    `<tr class="table-empty"><td class="result-error" colspan="4">` +
    `Couldn't load OID identities — retrying…</td></tr>`;
}

// ── Refresh (fetch + render) ────────────────────────────────────────────────

// Both route through the shared refreshWithErrorState helper so they cannot
// diverge from the deadletter/status error discipline (council fix 3):
// success -> render; 401 -> onAuthLost; any other error -> visible error row.

export async function refreshKeys(onAuthLost) {
  return refreshWithErrorState({
    fetchFn: listKeys,
    onOk: data => renderKeys(data.keys || []),
    onError: () => renderKeysError(),
    onAuthLost,
  });
}

export async function refreshIdentities(onAuthLost) {
  return refreshWithErrorState({
    fetchFn: listIdentities,
    onOk: data => renderIdentities(data.identities || []),
    onError: () => renderIdentitiesError(),
    onAuthLost,
  });
}

// ── Wiring ───────────────────────────────────────────────────────────────

function wireDeleteDelegation(body, kind, deleteFn, refreshFn, confirmingSet, onAuthLost, onEdit) {
  if (!body || body.dataset.wired === '1') return;
  body.dataset.wired = '1';
  const rowAttr = kind === 'delete-key' ? 'data-hash' : 'data-oid';
  const entity = kind === 'delete-key' ? 'key' : 'identity';
  const dsKey = rowAttr === 'data-hash' ? 'hash' : 'oid';

  function findCell(row) {
    return row.querySelector('[data-actions]');
  }

  // Rebuild the resting Edit+Delete cell from the row's own data attributes
  // (data-id, data-display are stamped by the row builders) so cancelling a
  // delete restores BOTH buttons, not just Delete.
  function restingCell(row, key) {
    return actionsButtonsHtml(kind, key, row.dataset.id ?? '', row.dataset.display ?? '');
  }

  body.addEventListener('click', async e => {
    const btn = e.target.closest('button');
    if (!btn) return;
    const row = btn.closest(`tr[${rowAttr}]`);
    if (!row) return;
    const key = row.dataset[dsKey];

    // Edit — prefill the add/update form with this row's values (council fix 2).
    if (btn.dataset.action === `edit-${entity}`) {
      if (onEdit) onEdit(row);
      return;
    }

    if (btn.dataset.action === kind) {
      confirmingSet.add(key);
      const cell = findCell(row);
      if (cell) {
        cell.innerHTML = deleteConfirmHtml(kind, key, key);
        row.dataset.confirming = '1';
        const cancelBtn = cell.querySelector('[data-confirm-cancel]');
        if (cancelBtn) cancelBtn.focus();
      }
      return;
    }
    if (btn.dataset.confirmCancel !== undefined) {
      confirmingSet.delete(key);
      const cell = findCell(row);
      if (cell) {
        delete row.dataset.confirming;
        cell.innerHTML = restingCell(row, key);
      }
      return;
    }
    if (btn.dataset.confirmAction === kind) {
      // Disable the Confirm button SYNCHRONOUSLY before the await so a
      // double-click can't fire a second DELETE (council fix 4; mirrors
      // deadletter-admin-panel.js runAction).
      btn.disabled = true;
      const cell = findCell(row);
      try {
        await deleteFn(key);
        confirmingSet.delete(key);
        await refreshFn(onAuthLost);
      } catch (err) {
        if (err && err.status === 401) {
          confirmingSet.delete(key);
          if (onAuthLost) onAuthLost();
          return;
        }
        const msg = err && err.status === 409 ? 'Cannot delete (protected — see message).' : 'Failed — retry';
        if (cell) cell.innerHTML = `<span class="result-error">${escapeAttr(msg)}</span>`;
        confirmingSet.delete(key);
        delete row.dataset.confirming;
      }
    }
  });

  body.addEventListener('keydown', e => {
    if (e.key !== 'Escape') return;
    const row = e.target.closest('tr[data-confirming]');
    if (!row) return;
    const key = row.dataset[dsKey];
    confirmingSet.delete(key);
    const cell = findCell(row);
    if (cell) {
      delete row.dataset.confirming;
      cell.innerHTML = restingCell(row, key);
    }
  });
}

function wireKeysForm(onAuthLost) {
  const addBtn = document.getElementById('key-add-btn');
  const hashInput = document.getElementById('key-hash-input');
  const idInput = document.getElementById('key-id-input');
  const hashError = document.getElementById('key-hash-error');
  const formError = document.getElementById('key-form-error');

  if (addBtn) {
    addBtn.addEventListener('click', async () => {
      const normalized = normalizeHash(hashInput.value);
      if (!isValidHash(normalized)) {
        hashInput.setAttribute('aria-invalid', 'true');
        if (hashError) hashError.style.display = '';
        return;
      }
      hashInput.removeAttribute('aria-invalid');
      if (hashError) hashError.style.display = 'none';
      if (formError) formError.style.display = 'none';
      try {
        await putKey(normalized, { id: idInput.value });
        hashInput.value = '';
        idInput.value = '';
        await refreshKeys(onAuthLost);
      } catch (err) {
        if (err && err.status === 401 && onAuthLost) {
          onAuthLost();
          return;
        }
        if (formError) {
          formError.textContent = err && err.status === 409 ? 'Cannot register the admin key\u2019s own hash.' : 'Save failed — check the contributor id.';
          formError.style.display = '';
        }
      }
    });
  }
  // onEdit: prefill the add/update form from the row's data attributes so the
  // operator never hand-transcribes a 64-hex hash (council fix 2). PUT is an
  // upsert, so submitting the prefilled form updates the entry.
  function prefillKeyForm(row) {
    if (hashInput) hashInput.value = row.dataset.hash || '';
    if (idInput) idInput.value = row.dataset.id || '';
    hashInput?.focus?.();
  }

  wireDeleteDelegation(
    document.getElementById('admin-keys-body'),
    'delete-key',
    deleteKey,
    refreshKeys,
    keysConfirming,
    onAuthLost,
    prefillKeyForm
  );
}

function wireIdentitiesForm(onAuthLost) {
  const addBtn = document.getElementById('oid-add-btn');
  const oidInput = document.getElementById('oid-input');
  const idInput = document.getElementById('oid-id-input');
  const displayInput = document.getElementById('oid-display-input');
  const oidError = document.getElementById('oid-error');
  const formError = document.getElementById('oid-form-error');

  if (addBtn) {
    addBtn.addEventListener('click', async () => {
      const normalized = normalizeGuid(oidInput.value);
      if (!isValidGuid(normalized)) {
        oidInput.setAttribute('aria-invalid', 'true');
        if (oidError) oidError.style.display = '';
        return;
      }
      oidInput.removeAttribute('aria-invalid');
      if (oidError) oidError.style.display = 'none';
      if (formError) formError.style.display = 'none';
      const body = { id: idInput.value };
      if (displayInput.value.trim()) body.display_name = displayInput.value;
      try {
        await putIdentity(normalized, body);
        oidInput.value = '';
        idInput.value = '';
        displayInput.value = '';
        await refreshIdentities(onAuthLost);
      } catch (err) {
        if (err && err.status === 401 && onAuthLost) {
          onAuthLost();
          return;
        }
        if (formError) {
          formError.textContent = 'Save failed — check the contributor id.';
          formError.style.display = '';
        }
      }
    });
  }
  // onEdit: prefill the add/update form from the row's data attributes so the
  // operator never hand-transcribes a GUID (council fix 2). PUT is an upsert,
  // so submitting the prefilled form updates the mapping.
  function prefillIdentityForm(row) {
    if (oidInput) oidInput.value = row.dataset.oid || '';
    if (idInput) idInput.value = row.dataset.id || '';
    if (displayInput) displayInput.value = row.dataset.display || '';
    oidInput?.focus?.();
  }

  wireDeleteDelegation(
    document.getElementById('admin-identities-body'),
    'delete-identity',
    deleteIdentity,
    refreshIdentities,
    identitiesConfirming,
    onAuthLost,
    prefillIdentityForm
  );
}

// ── Mode switch (doc 17 §E.2) ──────────────────────────────────────────────

// setIdentityMode(mode, onAuthLost) — the load-bearing entry point. `mode` is
// `status.auth.mode` ('static' | 'entra'). The active store's card gets the
// live table+form (and an initial fetch); the inactive store's card gets the
// proactive "Not configured" state WITHOUT firing a doomed request.
export function setIdentityMode(mode, onAuthLost) {
  const isStatic = mode === 'static';
  const isEntra = mode === 'entra';

  const keysCard = document.getElementById('identity-keys-card');
  if (keysCard) {
    if (isStatic) {
      if (keysCard.dataset.mode !== 'static') {
        keysCard.innerHTML = keysLiveHtml();
        keysCard.dataset.mode = 'static';
        wireKeysForm(onAuthLost);
      }
      refreshKeys(onAuthLost);
    } else {
      keysCard.innerHTML = notConfiguredHtml('API keys');
      keysCard.dataset.mode = 'inactive';
    }
  }

  const idCard = document.getElementById('identity-identities-card');
  if (idCard) {
    if (isEntra) {
      if (idCard.dataset.mode !== 'entra') {
        idCard.innerHTML = identitiesLiveHtml();
        idCard.dataset.mode = 'entra';
        wireIdentitiesForm(onAuthLost);
      }
      refreshIdentities(onAuthLost);
    } else {
      idCard.innerHTML = notConfiguredHtml('OID identities');
      idCard.dataset.mode = 'inactive';
    }
  }
}
