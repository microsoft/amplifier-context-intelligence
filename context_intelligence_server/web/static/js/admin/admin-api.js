// admin-api.js — the admin auth helper + typed fetch wrappers (doc 17 §D.2).
//
// Distinct credential from the read-only dashboard: this module reads
// `ci_admin_key` (NOT `ci_api_key`, api.js:2). The admin credential must never
// be conflated with, or leak into, the dashboard's key — the two surfaces keep
// separate storage (doc 17 §D.1).
//
// Pure, node-importable: no top-level DOM access anywhere in this file.

export function adminAuthHeaders() {
  const token = localStorage.getItem('ci_admin_key');
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  return headers;
}

function _httpError(label, res) {
  const err = new Error(`${label} failed: ${res.status}`);
  err.status = res.status; // 401 | 403 | 404 | 409 | 422 | 503 ...
  return err;
}

async function _json(label, res) {
  if (!res.ok) throw _httpError(label, res);
  return res.json();
}

// --- Status / mode ---
export async function fetchStatus() {
  return _json('status', await fetch('/status', { headers: adminAuthHeaders() }));
}

// --- Dead letters ---
export async function fetchDeadLetters() {
  return _json(
    'dead-letter list',
    await fetch('/queues/dead-letter', { headers: adminAuthHeaders() })
  );
}

export async function replayWorker(workerKey) {
  const url = `/admin/queues/dead-letter/${encodeURIComponent(workerKey)}/replay`;
  return _json('replay', await fetch(url, { method: 'POST', headers: adminAuthHeaders() }));
}

export async function purgeWorker(workerKey) {
  const url = `/admin/queues/dead-letter/${encodeURIComponent(workerKey)}/purge`;
  return _json('purge', await fetch(url, { method: 'POST', headers: adminAuthHeaders() }));
}

// --- Identities (entra mode) ---
export async function listIdentities() {
  return _json('identities', await fetch('/admin/identities', { headers: adminAuthHeaders() }));
}

export async function putIdentity(oid, body) {
  // body: {id, display_name?}
  const url = `/admin/identities/${encodeURIComponent(oid)}`;
  return _json(
    'put identity',
    await fetch(url, {
      method: 'PUT',
      headers: adminAuthHeaders(),
      body: JSON.stringify(body),
    })
  );
}

export async function deleteIdentity(oid) {
  const url = `/admin/identities/${encodeURIComponent(oid)}`;
  return _json('delete identity', await fetch(url, { method: 'DELETE', headers: adminAuthHeaders() }));
}

// --- Keys (static mode) ---
export async function listKeys() {
  return _json('keys', await fetch('/admin/keys', { headers: adminAuthHeaders() }));
}

export async function putKey(hash, body) {
  // body: {id}
  const url = `/admin/keys/${encodeURIComponent(hash)}`;
  return _json(
    'put key',
    await fetch(url, { method: 'PUT', headers: adminAuthHeaders(), body: JSON.stringify(body) })
  );
}

export async function deleteKey(hash) {
  const url = `/admin/keys/${encodeURIComponent(hash)}`;
  return _json('delete key', await fetch(url, { method: 'DELETE', headers: adminAuthHeaders() }));
}
