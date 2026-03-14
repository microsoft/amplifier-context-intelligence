export async function fetchStatus() {
  const r = await fetch('/status');
  if (!r.ok) throw new Error('Status fetch failed: ' + r.status);
  return r.json();
}

export async function postCypher(query, params, workspace) {
  const r = await fetch('/cypher', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, params: params || {}, workspace: workspace || '*' })
  });
  if (!r.ok) throw new Error('Cypher failed: ' + r.status);
  return r.json();
}
