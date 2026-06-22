# Design: Per-User API Keys & Node Provenance

Status: implemented (server version 5.0.0)
Scope: `context_intelligence_server/config.py`, `auth.py`, `credentials.py`,
`neo4j_store.py` / handlers (provenance), `start.sh`, `docker-entrypoint.sh`.

---

## Goal

Move authentication from a single shared `api_key` to **per-contributor keys**, so
the server can (a) attribute graph data to the contributor who produced it and
(b) revoke or rotate one peer without re-onboarding everyone — without breaking
existing single-key deployments.

Two coexisting modes feed one keystore (`Settings.build_keystore()` →
`{sha256_hex(token) -> contributor_id}`):

- **Legacy `api_key`** — one secret, folds to contributor id `owner`. Hashed at
  startup so the bearer token and the stored digest match.
- **`api_keys`** — a map of `sha256_hex(token) -> {id: <contributor>}`. The server
  stores only digests; the peer sends the raw token and the server hashes it
  (`auth.py` `_resolve_token`) to look up the contributor.

Auth is disabled (keystore empty) only when `api_keys` is omitted/`null` **and**
no `api_key` is set — backward-compatible with un-authed local dev.

---

## Load-bearing invariant: one contributor per session

**Each `session_id` is owned by exactly one contributor.** This is what makes node
provenance sound: every node derived from a session (the session node, its tool
calls, its events) can be stamped with that session's single contributor without
ambiguity. The invariant is now **test-enforced**, not merely assumed — a session
whose events arrive under two different contributors is a contract violation the
tests guard against, rather than a silent data-quality problem.

Edges break this invariant by nature (an edge can join nodes from different
sessions), which is why edge provenance has weaker semantics (D7).

---

## Decisions

### D1 — Reject empty `api_keys` (fail-closed)
`api_keys: {}` (an explicit empty map) raises at startup. An empty keystore is a
misconfiguration, not "auth off"; silently disabling auth on an empty map is the
dangerous reading. To disable auth, the operator must *omit* `api_keys` or set it
to `null` — an explicit, deliberate act. (`config.py` `_validate_api_keys`.)

### D2 — Write-once provenance (`ON CREATE` + `created_by` excluded from props)
`created_by` is set when a node is first created and **never overwritten**.
Implemented by writing it under `ON CREATE SET` only, and by **excluding
`created_by` from the normal property-update set** so a later `MERGE` from a
different (or spoofed) contributor cannot clobber the original stamp. This is the
anti-spoof guarantee: first writer wins, permanently.

### D3 — The invariant is observable, not silent
Rather than trust the one-contributor-per-session invariant implicitly, the system
makes a violation detectable (test-enforced, and the write-once stamp means a
second contributor's writes cannot quietly rewrite history). Surfacing beats
silent corruption.

### D4 — Digest validation & lowercase normalization
Each `api_keys` key must be exactly 64 hex characters. Keys are lowercased before
validation and stored lowercase, so an UPPERCASE digest in a config file still
matches the lowercase `hashlib.sha256(...).hexdigest()` the server computes.
Non-hex, wrong-length, non-dict values, and missing/blank/whitespace `id` are all
rejected at startup. (`config.py` `_validate_api_keys`.)

### D5 / O1 — Remove `init`; keep Docker bootstrap emitting `api_keys`
The `init` subcommand is **removed**. First-run setup is either the Docker
auto-bootstrap or the manual guide (`docs/managing-api-keys.md`). The bootstrap
scripts (`start.sh`, `docker-entrypoint.sh`) generate a raw token, write
`credentials.yaml` with `api_keys: { <digest>: { id: owner } }` (digest only), and
**print the raw token once** behind a "SAVE THIS TOKEN — it will NOT be shown
again" banner. Operators can no longer grep the token from `credentials.yaml`;
only the digest is persisted. (`credentials.py` `generate_credentials` returns the
raw token for the caller to display.)

### D7 — Edge / relationship provenance (v1 semantics)
Relationships also get a write-once `created_by`, but edges are inherently
cross-session, so it means **"first-asserter wins, race-determined for
cross-session edges, write-once-stable" — NOT ownership.** Consequences for v1:

- Edge `created_by` is **intentionally not indexed**; querying edges by
  contributor is unsupported for now.
- **Bare endpoint placeholder nodes** created as a side effect of asserting an
  edge are left `created_by = null` (not stamped), to avoid cross-session
  mis-attribution of a node the asserting contributor doesn't actually own.

### D8 — Version 5.0.0
The change ships as server version 5.0.0.

---

## Security model

- **Anti-spoof via write-once overwrite protection (D2).** `created_by` is set
  `ON CREATE` and excluded from update props; a later writer — honest or
  malicious — cannot overwrite an existing stamp.
- **Fail-closed (D1, D4).** A misconfigured keystore (empty map, bad digest,
  blank id) stops the server rather than silently disabling auth. Auth-off is only
  reachable by explicit omission/`null`.
- **Digest-at-rest.** Raw tokens are never stored — only `sha256` digests live in
  config/credentials files. A leaked config file does not leak usable tokens.
- **Back-compat.** Legacy `api_key` continues to work unchanged (folds to
  `owner`); existing single-key deployments need no migration and can adopt
  `api_keys` incrementally (both modes merge into one keystore).
- **Historical data.** Nodes created before this feature keep `created_by = null`;
  attribution is forward-looking, not retroactively fabricated.

`created_by` is provenance/attribution, **not** an authorization boundary — access
control is the keystore (who holds a valid token), not the stamp.

---

## See also

- `docs/managing-api-keys.md` — operator guide (setup, add/revoke/rotate, migrate).
- `server-config.example.yaml` — annotated `api_key` / `api_keys` reference.
- `context_intelligence_server/config.py` — keystore validation + `build_keystore`.
- `context_intelligence_server/auth.py` — token → sha256 → keystore lookup.
