# Dangling-Node Reader Audit

**Date:** 2026-06-18  
**Auditor:** Amplifier (automated audit, Phase-1 gate for #278)  
**Scope:** Every node→edge reader in `neo4j_store.py`, `services.py`, and `routers/` that could break during a chunked flush when nodes are committed but their edges are not yet committed.

---

## Background

The chunked-flush design commits nodes before edges within each flush transaction.  This creates a
transient window visible to concurrent readers where a node exists in Neo4j but its edges have not
yet been committed.  Every code path that walks from a node to its edges must tolerate this window
without asserting "node exists ⇒ its edges exist."

If any reader makes that assumption it is classified **NEEDS-FIX** and must be fixed (with a
failing-test-first cycle) before Phase-1 work can proceed.  This document records the audit.

---

## Step 1 — Grep Enumeration

The following three commands were run and every output line recorded.

### Command 1: `grep -nE "MATCH .*-\[|OPTIONAL MATCH|-->|<--" context_intelligence_server/neo4j_store.py`

```
616:                "MATCH ()-[r]->() "
```

### Command 2: `grep -nE "MATCH .*-\[|OPTIONAL MATCH|-->|<--|traverse|neighbors|edges" context_intelligence_server/services.py`

```
70:        self._edges: dict[tuple[str, str], dict[str, Any]] = {}
125:        if key not in self._edges:
126:            self._edges[key] = {}
127:            self._edges[key].update(data)
135:        edge = self._edges.get((src_id, dst_id))
143:        self._edges.pop((src_id, dst_id), None)
```

### Command 3: Recursive grep over `context_intelligence_server/routers/`

Pattern: `grep -rnE "MATCH .*-\[|OPTIONAL MATCH|-->|<--|traverse|neighbors|edges" context_intelligence_server/routers/`

```
(no output — zero hits)
```

---

## Step 2 — Audit Table

One row per grep hit.  Each hit is read in context, then classified.

| # | Target (file:line) | Context / enclosing function | Walks node→edge? | Assumes node⇒edge? | Verdict |
|---|---|---|---|---|---|
| 1 | `neo4j_store.py:616` | `Neo4jGraphStore.get_edge()` — Cypher fallback after buffer miss: `MATCH ()-[r]->() WHERE r.src_id = $src_id AND r.dst_id = $dst_id AND r.workspace = $workspace RETURN properties(r) AS props` | **No** — pattern starts from anonymous nodes `()`, then filters the relationship `[r]` by stored properties `src_id`/`dst_id`; never traverses from a known node to discover its edges | **No** — the query locates an edge by its own properties; src node need not be present in Neo4j at query time | **TOLERANT** |
| 2 | `services.py:70` | `GraphState.__init__` — in-memory dict declaration: `self._edges: dict[tuple[str, str], dict[str, Any]] = {}` | **No** — constructor initialisation; no graph traversal of any kind | **No** — write path; irrelevant to read-time assumptions | **TOLERANT** |
| 3 | `services.py:125` | `GraphState.upsert_edge()` — write guard: `if key not in self._edges:` | **No** — write path | **No** — write path | **TOLERANT** |
| 4 | `services.py:126` | `GraphState.upsert_edge()` — write: `self._edges[key] = {}` | **No** — write path | **No** — write path | **TOLERANT** |
| 5 | `services.py:127` | `GraphState.upsert_edge()` — write: `self._edges[key].update(data)` | **No** — write path | **No** — write path | **TOLERANT** |
| 6 | `services.py:135` | `GraphState.get_edge()` — direct dict lookup: `edge = self._edges.get((src_id, dst_id))` | **No** — keyed point-lookup by `(src_id, dst_id)` tuple; never enumerates edges reachable from a node | **No** — returns `None` when absent; caller is responsible for handling the absent case | **TOLERANT** |
| 7 | `services.py:143` | `GraphState.remove_edge()` — delete: `self._edges.pop((src_id, dst_id), None)` | **No** — mutation path; not a reader | **No** — mutation path | **TOLERANT** |
| 8 | `routers/` (all files) | Zero hits across `routers/__init__.py`, `routers/queues.py`, `routers/skills.py`, `routers/version.py` | **N/A** | **N/A** | **TOLERANT** (absent) |

---

## Step 3 — Independent Point-Lookup Confirmation

The two primary read operations used by handlers are point lookups that are structurally
independent of whether the looked-up entity's relationships have been committed:

| Function | Location | Cypher / mechanism | Dangling-node safe? |
|---|---|---|---|
| `get_node` | `neo4j_store.py:566` | `MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace RETURN properties(n) AS props, labels(n) AS lbls` — node-only lookup; no edge traversal in the query | **SAFE** |
| `get_edge` | `neo4j_store.py:601` | `MATCH ()-[r]->() WHERE r.src_id = $src_id AND r.dst_id = $dst_id AND r.workspace = $workspace RETURN properties(r) AS props` — edge-only lookup by stored properties; no dependency on src node being present | **SAFE** |

Neither function walks from a node to discover its edges.  Both return `None` when the target
entity is absent, which all callers handle (`existing.get(...)` with a default of `[]` or `{}`).

---

## Handler Survey (supplementary — not covered by the spec grep commands)

As a belt-and-suspenders check, the handler files were inspected for any `get_node` call that is
immediately followed by a `get_edge` call (the specific pattern that would imply "node exists ⇒
edges exist"):

- `handlers/data_layer_2/session.py:157, 222, 288` — calls `get_node(session_id)` to read the
  current type labels, then performs `upsert_node` / `upsert_edge` writes.  No subsequent
  `get_edge` call.  **TOLERANT.**
- `handlers/data_layer_3/delegation.py:115` — calls `get_node(parent_session_id)` solely to
  resolve `agent == "self"` to the parent's stored agent name.  No subsequent `get_edge` call.
  **TOLERANT.**

No handler reads a node and then assumes that node's edges are present.

---

## NEEDS-FIX Count

**Zero.**  All readers are TOLERANT of the transient dangling-node window.

No code changes are required as a result of this audit.

---

## Sign-Off

All grep hits from the three specified commands have been recorded.  Every reader is classified
**TOLERANT**.  The two independent point-lookup functions (`get_node`, `get_edge`) are confirmed
**SAFE**.  No fix cycles were required.

The dangling-node design premise is verified: the chunked-flush ordering (nodes before edges) does
not expose any reader to an unhandled "node exists ⇒ its edges exist" assumption.

**Phase-1 gate: PASSED.** Subsequent Phase-1 tasks may proceed.

> Signed off by Amplifier on 2026-06-18 (automated audit, session `amplifier-context-intelligence #278 Phase 1`).
