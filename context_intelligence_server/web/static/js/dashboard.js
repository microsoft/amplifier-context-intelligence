import { fetchStatus, postCypher } from './api.js';
import { renderQueues, fetchDeadLetters, renderDeadLetters, renderDeadLetterError, wireDeadLetterActions } from './queues-panel.js';

function timeAgo(ts) {
  if (!ts) return '-';
  const diff = Math.floor(Date.now() / 1000 - ts);
  if (diff < 60) return diff + 's ago';
  if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
  if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
  return Math.floor(diff / 86400) + 'd ago';
}

function truncate(s, n) {
  if (!s) return '-';
  return s.length > n ? s.slice(0, n) + '…' : s;
}

function escapeAttr(s) {
  if (!s) return '';
  return String(s)
    .replace(/&/g, '&amp;').replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// Pure pipeline-health hint derivation. Pill text carries NO literal dot
// glyph — .pill::before draws the dot (fix C).
export function computeHint(metrics) {
  const m = metrics || {};
  const degraded = !!m.degraded;
  const dead = m.dead_letter_total ?? 0;
  return {
    degraded,
    pillText: degraded ? 'DEGRADED' : 'Pipeline OK',
    pillClass: degraded ? 'pill degraded' : 'pill',
    inQueue: m.in_queue_total ?? 0,
    deadVisible: dead > 0,
    deadText: `Dead-letter ${dead}`,
  };
}

// ── Neo4j row expand ──────────────────────────────────────────────────────
document.getElementById('completed-body')?.addEventListener('click', e => {
  const row = e.target.closest('tr.clickable');
  if (!row) return;
  toggleDetail(row.dataset.sessionId, row.dataset.workspace, row);
});

function toggleDetail(sessionId, workspace, row) {
  const next = row.nextElementSibling;
  if (next?.classList.contains('detail-row')) { next.remove(); return; }
  const tr = document.createElement('tr');
  tr.className = 'detail-row';
  const td = document.createElement('td');
  td.colSpan = 6; td.className = 'detail-cell'; td.textContent = 'Loading graph data…';
  tr.appendChild(td);
  row.parentNode.insertBefore(tr, row.nextSibling);
  postCypher(
    'MATCH (n {workspace: $workspace}) WHERE n.node_id CONTAINS $sid RETURN labels(n)[0] as type, count(n) as cnt ORDER BY cnt DESC',
    { workspace, sid: sessionId },
    '*'
  )
    .then(data => {
      const rows = data.results || [];
      td.textContent = rows.length
        ? rows.map(r => r.type + ': ' + r.cnt).join(' · ')
        : 'No graph nodes found.';
    })
    .catch(() => { td.textContent = 'Neo4j query failed.'; });
}

// ── Status polling ────────────────────────────────────────────────────────
let activeTab = 'overview';

function onAuthLost() {
  try { localStorage.removeItem('ci_api_key'); } catch { /* storage unavailable */ }
  const overlay = document.getElementById('auth-overlay');
  if (overlay) overlay.style.display = '';
}

function setTab(name) {
  activeTab = name;
  const queues = name === 'queues';
  const panelOverview = document.getElementById('panel-overview');
  if (panelOverview) panelOverview.hidden = queues;
  const panelQueues = document.getElementById('panel-queues');
  if (panelQueues) panelQueues.hidden = !queues;
  const tabOverview = document.getElementById('tab-overview');
  if (tabOverview) {
    tabOverview.classList.toggle('active', !queues);
    tabOverview.setAttribute('aria-selected', String(!queues));
  }
  const tabQueues = document.getElementById('tab-queues');
  if (tabQueues) {
    tabQueues.classList.toggle('active', queues);
    tabQueues.setAttribute('aria-selected', String(queues));
  }
  window.scrollTo(0, 0);
  if (queues) refresh();
}

document.getElementById('tab-overview')?.addEventListener('click', () => setTab('overview'));
document.getElementById('tab-queues')?.addEventListener('click', () => setTab('queues'));
document.getElementById('hint-go-queues')?.addEventListener('click', () => setTab('queues'));
wireDeadLetterActions({ onAuthLost });

async function refresh() {
  try {
    const data = await fetchStatus();

    document.getElementById('uptime').textContent = data.uptime_seconds?.toFixed(1) ?? '-';
    document.getElementById('active_sessions').textContent = data.active_sessions ?? '-';

    const ec = data.error_count_last_hour || 0;
    document.getElementById('error_count').textContent = ec;
    const badge = document.getElementById('error-badge');
    if (badge) badge.style.display = ec > 0 ? 'inline' : 'none';

    const neo4jStatus = document.getElementById('neo4j-status');
    if (neo4jStatus) {
      if (data.neo4j_connected) {
        neo4jStatus.textContent = '\u25cf Connected';
        neo4jStatus.style.color = 'var(--primary)';
      } else {
        neo4jStatus.textContent = '\u25cb Disconnected';
        neo4jStatus.style.color = 'var(--destructive)';
      }
    }

    const neo4jUrl = document.getElementById('neo4j-url');
    if (neo4jUrl) neo4jUrl.textContent = data.neo4j_url || '\u2014';
    const neo4jBrowserUrl = document.getElementById('neo4j-browser-url');
    if (neo4jBrowserUrl && data.neo4j_browser_url) {
      neo4jBrowserUrl.textContent = data.neo4j_browser_url;
      neo4jBrowserUrl.href = data.neo4j_browser_url;
    }

    const sb = document.getElementById('sessions-body');
    if (sb) sb.innerHTML = (data.sessions || []).map(s =>
      `<tr><td>${truncate(s.session_id, 20)}</td><td>${truncate(s.workspace, 28)}</td>` +
      `<td>${(s.last_event || '-')}</td><td>${s.events_processed}</td></tr>`
    ).join('');

    const cb = document.getElementById('completed-body');
    if (cb) cb.innerHTML = (data.completed_sessions || []).map(s => {
      const dur = s.duration_seconds != null ? s.duration_seconds.toFixed(1) + 's' : '-';
      return `<tr class="clickable" data-session-id="${escapeAttr(s.session_id)}" data-workspace="${escapeAttr(s.workspace)}">` +
        `<td>${truncate(s.session_id, 20)}</td><td>${truncate(s.workspace, 28)}</td>` +
        `<td>${dur}</td><td>${s.events_processed || 0}</td><td>${s.error_count || 0}</td>` +
        `<td>${timeAgo(s.ended_at)}</td></tr>`;
    }).join('');

    const eb = document.getElementById('events-body');
    if (eb) eb.innerHTML = (data.recent_events || []).map(e =>
      `<tr><td>${timeAgo(e.timestamp)}</td><td>${e.event}</td>` +
      `<td>${truncate(e.session_id, 20)}</td><td>${truncate(e.workspace, 28)}</td>` +
      `<td class="${e.result === 'ok' ? 'result-ok' : 'result-error'}">${e.result}</td></tr>`
    ).join('');

    const hint = computeHint(data.metrics || {});
    const hintPill = document.getElementById('hint-pill');
    if (hintPill) { hintPill.textContent = hint.pillText; hintPill.className = hint.pillClass; }
    const hintInQueue = document.getElementById('hint-inqueue');
    if (hintInQueue) hintInQueue.textContent = hint.inQueue;
    const hintDead = document.getElementById('hint-dead');
    if (hintDead) {
      hintDead.style.display = hint.deadVisible ? 'inline-flex' : 'none';
      hintDead.textContent = hint.deadText;
    }

    renderQueues(data);
    if (activeTab === 'queues') {
      try {
        const dl = await fetchDeadLetters();
        renderDeadLetters(dl.dead_letters || []);
      } catch (err) {
        if (err && err.status === 401) onAuthLost();
        else renderDeadLetterError();
      }
    }
  } catch (err) {
    console.error('Status refresh failed:', err);
  }
}
refresh();
setInterval(refresh, 3000);

// ── Log viewer (SSE) ──────────────────────────────────────────────────────
const logContainer = document.getElementById('log-container');
const logFilter = document.getElementById('log-filter');
const logToggle = document.getElementById('log-toggle');
const logErrorBadge = document.getElementById('log-error-badge');
let isPaused = false;
let pauseBuffer = [];
let autoScroll = true;
let logErrorCount = 0;

logContainer?.addEventListener('scroll', () => {
  autoScroll = (logContainer.scrollHeight - logContainer.scrollTop - logContainer.clientHeight) < 8;
});

function appendLogLine(text) {
  if (!logContainer) return;
  let level = 'INFO';
  try {
    const p = JSON.parse(text);
    level = p.level || p.levelname || 'INFO';
  } catch { /* raw text */ }

  if (level === 'ERROR') {
    logErrorCount++;
    if (logErrorBadge) {
      logErrorBadge.textContent = logErrorCount + ' error' + (logErrorCount === 1 ? '' : 's');
      logErrorBadge.style.display = 'inline';
    }
  }

  const div = document.createElement('div');
  div.className = `log-line log-${level}`;
  div.textContent = text;
  const ft = logFilter?.value.toLowerCase();
  if (ft && !text.toLowerCase().includes(ft)) div.style.display = 'none';
  logContainer.appendChild(div);

  while (logContainer.children.length > 2000) logContainer.removeChild(logContainer.firstChild);
  if (autoScroll) logContainer.scrollTop = logContainer.scrollHeight;
}

function filterLogs() {
  const ft = logFilter?.value.toLowerCase() || '';
  for (const el of logContainer?.getElementsByClassName('log-line') || []) {
    el.style.display = (!ft || el.textContent.toLowerCase().includes(ft)) ? '' : 'none';
  }
}

function togglePause() {
  isPaused = !isPaused;
  if (logToggle) logToggle.textContent = isPaused ? 'Resume' : 'Pause';
  if (!isPaused) { pauseBuffer.forEach(appendLogLine); pauseBuffer = []; }
}

const evtSource = new EventSource('/logs/stream');
evtSource.onmessage = e => isPaused ? pauseBuffer.push(e.data) : appendLogLine(e.data);

// wire up controls
logFilter?.addEventListener('input', filterLogs);
logToggle?.addEventListener('click', togglePause);
