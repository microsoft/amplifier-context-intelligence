// admin-auth-overlay.js — the §D.3 allow-list probe, extracted as a pure-ish,
// independently-testable unit. admin.js wires this to the DOM overlay; tests
// exercise it directly with a mock fetch, without needing to execute the rest
// of admin.js (which has import-time side effects: polling, other panels).
//
// This is a deliberate structural addition beyond the spec's exact file list
// (doc 17 §B.1) — justified because it is the only way to unit-test the
// allow-list probe in isolation without either (a) importing all of admin.js
// (starts a real setInterval poll and touches many DOM ids at import time) or
// (b) resorting to source-text regex assertions instead of exercising real
// behavior. See the builder report for the full rationale.

// classifyAdminProbeStatus(status) — the explicit ALLOW-LIST (doc 17 §D.3):
// 200 or 503 accept; everything else (401/403/404/500/502/...) rejects. An
// earlier draft inverted this into a reject-list (reject only 401/403,
// silently accept anything else) — that regression is guarded by tests.
export function classifyAdminProbeStatus(status) {
  return status === 200 || status === 503;
}

// probeAdminKey(token, fetchImpl) — POSTs nothing; probes GET /admin/keys with
// the candidate token as a Bearer credential. Returns a plain result object
// rather than throwing, so callers never need a try/catch for the "reject"
// path — only a genuine network failure is distinguished (networkError=true).
export async function probeAdminKey(token, fetchImpl = fetch) {
  let res;
  try {
    res = await fetchImpl('/admin/keys', {
      headers: { Authorization: 'Bearer ' + token },
    });
  } catch (_networkErr) {
    // Network error => reject (NOT accept). Guards the same inverted-accept
    // regression class as the status allow-list above.
    return { accepted: false, networkError: true, status: null };
  }
  return { accepted: classifyAdminProbeStatus(res.status), networkError: false, status: res.status };
}

// wireAdminOverlay(opts) — THE shipping login gate. admin.js calls this with
// the real DOM elements; tests call it with mock elements + a mock fetch. This
// is the SINGLE implementation of the login flow: there is no hand-rolled
// duplicate probe anywhere. The 200|503 allow-list lives ONLY in
// classifyAdminProbeStatus (via probeAdminKey) — a future re-inversion breaks
// admin-auth-overlay.test.mjs, which exercises THIS function end-to-end.
//
// opts:
//   overlay, input, submit, errMsg — the overlay DOM elements.
//   signOutBtn (optional)          — clears ci_admin_key + re-shows the overlay
//                                    (credential rotation without editing
//                                    localStorage by hand; council fix 7).
//   storage (default localStorage) — credential store seam for tests.
//   fetchImpl (default fetch)      — network seam for tests.
// Returns { tryAuth, showOverlay, hideOverlay } so callers/tests can drive it.
export function wireAdminOverlay(opts = {}) {
  const {
    overlay,
    input,
    submit,
    errMsg,
    signOutBtn,
    storage = localStorage,
    fetchImpl = fetch,
  } = opts;
  if (!overlay || !input || !submit) return null;

  function showOverlay() {
    overlay.style.display = '';
    if (errMsg) errMsg.style.display = 'none';
    input.value = '';
    input.focus?.();
  }
  function hideOverlay() {
    overlay.style.display = 'none';
  }

  // If a credential is already stored, skip the gate on load.
  if (storage.getItem('ci_admin_key')) hideOverlay();

  async function tryAuth() {
    const token = input.value.trim();
    if (!token) return;
    if (errMsg) errMsg.style.display = 'none';
    const result = await probeAdminKey(token, fetchImpl);
    if (result.accepted) {
      storage.setItem('ci_admin_key', token);
      hideOverlay();
    } else {
      // 401/403/404/5xx/network => reject. The allow-list is enforced ONLY in
      // classifyAdminProbeStatus; this branch never re-decides it.
      if (errMsg) errMsg.style.display = '';
    }
  }

  submit.addEventListener('click', tryAuth);
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter') tryAuth();
  });

  if (signOutBtn) {
    signOutBtn.addEventListener('click', () => {
      try {
        storage.removeItem('ci_admin_key');
      } catch {
        /* storage unavailable */
      }
      showOverlay();
    });
  }

  return { tryAuth, showOverlay, hideOverlay };
}
