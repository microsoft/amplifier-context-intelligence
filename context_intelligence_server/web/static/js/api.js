export function authHeaders() {
  const token = localStorage.getItem('ci_api_key');
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  return headers;
}

export async function fetchStatus() {
  const response = await fetch('/status');
  if (!response.ok) throw new Error('Status fetch failed: ' + response.status);
  return response.json();
}

export async function postCypher(query, params = {}, workspace = '*') {
  const response = await fetch('/cypher', {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify({ query, params, workspace })
  });
  if (!response.ok) throw new Error('Cypher failed: ' + response.status);
  return response.json();
}
