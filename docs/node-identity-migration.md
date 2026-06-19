# Migration: universal `:Node` identity (ships with the B′ silent-edge-drop fix)

This change makes node **identity** key on the universal `:Node(node_id, workspace)`
label instead of per-type labels (`:Session`, `:Event`, …). It is required by the
cross-session edge-write fix: the edge writer now `MERGE`s its endpoints (so it never
silently drops an edge), and a bare `:Node` placeholder it creates must **converge** with
the later typed write rather than fork identity. Convergence only holds if every node
writer keys on `:Node`, backed by a `:Node` uniqueness constraint.

> **Why a migration at all:** a fresh DB builds this schema before any data exists, so it
> is trivially safe (that is the path the test suite exercises). The **live graph already
> holds millions of nodes written under the old schema** — and #19's `:Node` backfill
> shipped as dead code, so many of them have **no `:Node` label**. Pointing the re-keyed
> writers at that graph before it is migrated would duplicate nodes. This runbook closes
> that window.

## What the code does automatically (`ensure_neo4j_schema`)

On schema init (startup / first flush), idempotently and **in this order**:

1. **Dedup** duplicate `(node_id, workspace)` nodes — **globally across all labels**
   (keeping the richest/most-typed node), plus the existing `:Session`/`:Event` passes.
   Required so the uniqueness constraints can be created on a graph the dead-backfill bug
   already dirtied.
2. Create the per-type indexes and `:Session`/`:Event` uniqueness constraints (unchanged).
3. **Backfill** the `:Node` label onto every untagged node (`CALL … IN TRANSACTIONS OF
   10 000 ROWS`).
4. **Verify** + log the remaining-untagged count (LOUD `WARNING` if `> 0`, `INFO` `= 0`).
5. **Drop** the legacy `idx_node_universal` plain index (a uniqueness constraint cannot
   coexist with a standalone index on the same key).
6. **Create** the `:Node(node_id, workspace)` uniqueness constraint (its backing index
   restores the NodeIndexSeek that #19's plain index provided).

`ensure_neo4j_schema` returns `True` (and the store latches `_schema_initialized`) **only
when every constraint is established**; otherwise it retries on the next flush.

## Required operator procedure (two-phase deploy)

The automatic path is correct on a healthy, single-startup DB. On the **live 1.3M-node
graph**, run it as an explicit, verified migration **before** the re-keyed writers take
production traffic — do not discover a partial migration via duplicate nodes.

**Phase A — migrate + verify (no behaviour change to ingest):**
The migration is purely additive under the OLD code (it only tags `:Node` + adds a
constraint; the old `:Session`-keyed writer still works with the constraint present).

1. Take a backup / snapshot of the Neo4j volume.
2. Drive `ensure_neo4j_schema` to completion (deploy this build to a single worker, or run
   the steps as a maintenance script) and **watch the logs** for:
   `:Node backfill complete (0 untagged nodes)` and successful creation of
   `node_node_id_workspace_unique`.
3. **Verify** (read-only) — both must hold before Phase B:
   ```cypher
   MATCH (n) WHERE NOT n:Node RETURN count(n) AS untagged;          // expect 0
   SHOW CONSTRAINTS YIELD name WHERE name = 'node_node_id_workspace_unique'
     RETURN count(*) AS present;                                    // expect 1
   ```
   Sizing (read-only, run first to estimate the backfill):
   ```cypher
   MATCH (n) WHERE NOT n:Node RETURN count(n);                      // backfill size
   MATCH (n) WITH n.node_id AS id, n.workspace AS ws, count(*) AS c
     WHERE c > 1 RETURN count(*);                                   // dup groups to clear
   ```

**Phase B — enable the re-keyed writers:** roll out the full build to all workers. With
the constraint present, the `:Node`-keyed Session writer and the MERGE-endpoint edge
writer behave correctly and concurrently-safely.

## Failure modes & rollback

| Symptom | Cause | Action |
|---|---|---|
| Log: `:Node backfill incomplete — N node(s) still lack :Node` | backfill interrupted (timeout/OOM/restart) | re-run schema init; it is idempotent (`WHERE NOT n:Node`). Do **not** proceed to Phase B until `N = 0`. |
| `node_node_id_workspace_unique` not created; `ConstraintValidationFailed` | residual duplicate `(node_id, workspace)` | the global dedup (Step 1) should clear it; if a dup persists, inspect and remove it, then re-run. |
| Constraint absent but writers live (Phase B ran too early) | mis-ordered deploy | re-keyed writers can fork identity. Stop writers, run Phase A to completion, dedup, then resume. |

**Rollback:** the change is backward-compatible — the OLD `:Session`-keyed writer operates
correctly on a graph that has the `:Node` label + constraint. To revert the code, redeploy
the previous build; leave the `:Node` label/constraint in place (harmless), or
`DROP CONSTRAINT node_node_id_workspace_unique` and recreate `idx_node_universal` if a
clean revert of the index is required.

## Reviewer checklist (what to scrutinise in the PR)

- Step ordering in `ensure_neo4j_schema`: dedup → backfill → verify → drop index →
  `:Node` constraint, **before** any write (`_flush_body` awaits `_ensure_schema` first).
- The Session writer now `MERGE (n:Node {…}) SET n:Session` (identity on `:Node`).
- The edge writer `MERGE`s both endpoints (never silently drops).
- Atomicity for concurrent MERGEs now comes from the `:Node` constraint (was `:Session`).
- Write-safety model: the two-phase procedure plus the LOUD incomplete-backfill warning
  (`ensure_neo4j_schema` logs `WARNING` while any node lacks `:Node` and only latches the
  store as initialized once every constraint is established) keep the migration
  never-silent, so re-keyed writers operate against a fully-migrated graph.
