// admin.js — entry point: overlay, tabs, mode-detect, polling (doc 17 §H.1 step 7).
// Glue only — no business logic lives here; panels own their own render/wire.

import { fetchStatus } from './admin-api.js';
import { wireAdminOverlay } from './admin-auth-overlay.js';
import { refreshWithErrorState } from './admin-refresh.js';
import { refreshDeadLetters, wireDeadLetterActions } from './panels/deadletter-admin-panel.js';
import { setIdentityMode } from './panels/identity-panel.js';
import { renderQueueMetrics, renderRefreshOk, renderRefreshError } from './panels/queue-metrics-panel.js';

// ── onAuthLost (mirrors dashboard.js:74-78, retargeted at ci_admin_key) ────
function onAuthLost() {
  try {
    localStorage.removeItem('ci_admin_key');
  } catch {
    /* storage unavailable */
  }
  const overlay = document.getElementById('admin-auth-overlay');
  if (overlay) overlay.style.display = '';
}

// ── Auth overlay (doc 17 §D.3) ─────────────────────────────────────────────
// The login gate lives ENTIRELY in wireAdminOverlay (admin-auth-overlay.js),
// which is the tested unit. admin.js merely hands it the DOM elements — there
// is NO hand-rolled probe or duplicate 200|503 allow-list here (council
// BLOCKER: the shipping path must be the tested path). The same wiring also
// owns the sign-out control (clear ci_admin_key + re-show the overlay).
wireAdminOverlay({
  overlay: document.getElementById('admin-auth-overlay'),
  input: document.getElementById('admin-auth-token-input'),
  submit: document.getElementById('admin-auth-submit-btn'),
  errMsg: document.getElementById('admin-auth-error'),
  signOutBtn: document.getElementById('admin-signout-btn'),
});

// ── Tabs ────────────────────────────────────────────────────────────────────
let activeTab = 'deadletters';

function setTab(name) {
  activeTab = name;
  for (const tabName of ['deadletters', 'identity', 'metrics']) {
    const panel = document.getElementById(`panel-${tabName}`);
    if (panel) panel.hidden = tabName !== name;
    const tabBtn = document.getElementById(`tab-${tabName}`);
    if (tabBtn) {
      tabBtn.classList.toggle('active', tabName === name);
      tabBtn.setAttribute('aria-selected', String(tabName === name));
    }
  }
  window.scrollTo(0, 0);
}

document.getElementById('tab-deadletters')?.addEventListener('click', () => setTab('deadletters'));
document.getElementById('tab-identity')?.addEventListener('click', () => setTab('identity'));
document.getElementById('tab-metrics')?.addEventListener('click', () => setTab('metrics'));

// ── Wiring (idempotent — safe even if refresh() runs before the DOM settles) ─
wireDeadLetterActions(document.getElementById('admin-dl-body'), onAuthLost);

// ── Polling ──────────────────────────────────────────────────────────────
let identityModeSeen = null;

async function refresh() {
  // doc 06 A.5: bail the WHOLE refresh cycle while any row across the surface
  // has an open inline confirm, so a 3s poll can never wipe an in-flight
  // Replay/Purge/Delete confirmation.
  if (document.querySelector('[data-confirming]')) return;

  // Status drives every panel (metrics + mode detection). Use the shared
  // refresh helper so a non-401 failure surfaces a VISIBLE "refresh failed"
  // signal instead of a silent console.error (council fix 3).
  const statusResult = await refreshWithErrorState({
    fetchFn: fetchStatus,
    onOk: status => {
      renderQueueMetrics(status);
      renderRefreshOk();
      const mode = status?.auth?.mode ?? null;
      if (mode && mode !== identityModeSeen) {
        identityModeSeen = mode;
        setIdentityMode(mode, onAuthLost);
      }
    },
    onError: () => renderRefreshError(),
    onAuthLost,
  });
  // If /status failed, everything downstream is stale — skip dependent fetches
  // this cycle (the visible refresh-failed signal already told the operator).
  if (!statusResult.ok) return;

  if (activeTab === 'deadletters') {
    // The deadletter panel owns its own fetch+render+error discipline
    // (renderDeadLetterError on non-401); reuse it rather than re-inlining.
    await refreshDeadLetters(onAuthLost);
  }
}

refresh();
setInterval(refresh, 3000);
