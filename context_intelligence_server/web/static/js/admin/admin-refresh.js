// admin-refresh.js — shared refresh-with-error-state discipline (council fix 3).
//
// One helper so no panel can silently swallow a non-401 error again. Every
// refresh path (status/metrics in admin.js, keys/identities in identity-panel)
// routes through this, so they cannot diverge:
//   - success        -> onOk(data)
//   - 401 (auth lost) -> onAuthLost()   (credential gone; re-show the overlay)
//   - any OTHER error -> onError(err)    (render a VISIBLE error/stale state,
//                                         NOT a silent console.error)
//
// Mirrors deadletter-admin-panel.js's renderDeadLetterError discipline and
// keeps every panel's refresh path uniform. Returns { ok, authLost } so the
// caller can decide whether to run dependent fetches this cycle.
export async function refreshWithErrorState({ fetchFn, onOk, onError, onAuthLost }) {
  try {
    const data = await fetchFn();
    if (onOk) onOk(data);
    return { ok: true, authLost: false };
  } catch (err) {
    if (err && err.status === 401) {
      if (onAuthLost) onAuthLost();
      return { ok: false, authLost: true };
    }
    if (onError) onError(err);
    return { ok: false, authLost: false };
  }
}
