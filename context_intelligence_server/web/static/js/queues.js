// queues.js - authenticated dead-letter fetch wrappers (C2)
// Wraps the /queues/dead-letter endpoints. Each wrapper attaches the HTTP
// status to thrown errors (err.status) so action handlers can branch on
// 401/400 honestly instead of guessing from the message (U2).

import { fetchStatus, authHeaders } from './api.js';

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
