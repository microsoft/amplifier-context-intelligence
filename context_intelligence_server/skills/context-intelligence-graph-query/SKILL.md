---
name: context-intelligence-graph-query
description: >
  Use when querying the context-intelligence property graph for session history,
  tool call traces, LLM iteration analysis, or execution scale metrics. Covers
  both data layers, cross-layer SOURCED_FROM joins, SST navigation, blob
  handling, and 8 verified Cypher patterns.
license: MIT
metadata:
  version: "2.0.0"
---

# Context Intelligence Graph Query

This skill equips you to navigate and extract insights from the context-intelligence
property graph using the `graph_query` tool. The graph holds a complete record of
every Amplifier session — what happened, when, how things connect, and at what scale.

---

## Section 1 — What the Graph Gives You (Two Layers)

The graph holds two complementary views of every session.

**Data layer 1** is the raw event stream. Every kernel event is preserved as a node,
queryable by type, field, and time. It answers: *what happened and when.* Complete
timeline, exact field values, every tool call and LLM exchange recorded as-is.

**Data layer 2** is the semantic layer. Events are assembled into meaningful runtime
entities — turns (OrchestratorRun), LLM iterations (Iteration), content blocks
(ContentBlock), tool calls (ToolCall), prompts (Prompt), and more. Connected by 15
typed relationships. It answers: *what ran, how, and at what scale.* Conversation
structure, execution scale, tool correlation, turn-level reasoning.

Both layers coexist in the same graph and are bridged by **SOURCED_FROM** edges — the
canonical cross-layer connection. Every data layer 2 entity carries one or more
SOURCED_FROM edges back to the raw data layer 1 events that produced it, giving every
semantic node a direct provenance link into the original event stream. Use data layer 1
when you need exact event fields or the raw timeline. Use data layer 2 when you need
structure, scale, or causation. Navigate between them with SOURCED_FROM.

**Layer identification signal:** The `node_id` separator tells you which layer a node
came from. `__` (double underscore) = data layer 1 node. `::` (double colon) =
data layer 2 node. A few data layer 2 types use plain identifiers (ToolCall uses
the provider's tool_call_id directly; Orchestrator uses the orchestrator name string).

---

## Section 2 — Schema Reference

### Data Layer 1 Nodes

| Node Label | Description | node_id Format |
|---|---|---|
| `:Session` | One Amplifier session. Sub-labels: `:RootSession`, `:ForkedSession`. | Raw UUID |
| `:Event` | Every kernel event. Triple-labeled: `:Event` + `:{Category}Event` + `:{Specific}Event`. | `{session_id}__{event_name}__{epoch_ms}` |

Key properties on `:Event` nodes:
- `occurred_at` — ISO 8601 timestamp
- `session_id` — owning session UUID
- `workspace` — workspace partition key
- `event_name` — raw event name (e.g. `tool:pre`)
- **`data`** — **JSON string** of the original kernel event payload. Not a Cypher map. Parse it before accessing sub-fields. May contain `ci-blob://` URI references for large payloads (see Section 5).
- Plus event-specific lifted properties (e.g. `tool_name`, `tool_call_id` on `:ToolPreEvent`; `model`, `provider` on `:LlmResponseEvent`).

Common event labels: `:ToolPreEvent`, `:ToolPostEvent`, `:ToolErrorEvent`, `:LlmRequestEvent`, `:LlmResponseEvent`, `:PromptSubmitEvent`, `:ExecutionStartEvent`, `:ExecutionEndEvent`, `:DelegateAgentSpawnedEvent`, `:SessionStartEvent`, `:SessionEndEvent`.

### Data Layer 1 Edges

| Edge | From → To | Meaning |
|---|---|---|
| `HAS_FORK` | Session → Session | Parent session forked a child |
| `HAS_TOOL_CALL` | Session → ToolCall | Session owns a data layer 1 tool call lifecycle node |
| `HAS_EVENT` | Session → Event | Session owns an event node |
| `HAS_EVENT` | ToolCall → Event | Tool call owns its lifecycle events |

### Data Layer 2 Entity Types

All data layer 2 nodes carry a `workspace` property and an SST type label.

| Entity | Labels | SST Type | node_id Format | Key Properties |
|---|---|---|---|---|
| Session | `:Session:SST_EVENT` (+ `:RootSession`/`:SubSession`/`:ForkedSession`) | Temporal | Raw UUID | `started_at`, `ended_at`, `status` |
| OrchestratorRun | `:OrchestratorRun:SST_EVENT` | Temporal | `{session_id}::orch_run::{started_at}` | `started_at`, `ended_at`, `orchestrator_name` |
| Iteration | `:Iteration:SST_EVENT` | Temporal | `{session_id}::iteration::{N}` | `iteration_number`, `started_at` |
| ContentBlock | `:ContentBlock:SST_EVENT` | Temporal | `{session_id}::block::{iteration_N}::{index}` | `block_type`, `block_index` |
| ToolCall | `:ToolCall:SST_EVENT` | Temporal | `{tool_call_id}` (provider UUID directly) | `tool_name`, `tool_call_id`, `result_success`, `result_error`, `result_output`, `started_at`, `ended_at`, `parallel_group_id` |
| Prompt | `:Prompt:SST_EVENT` | Temporal | `{session_id}::prompt::{timestamp}` | `prompt_text`, `started_at` |
| Cancellation | `:Cancellation:SST_EVENT` | Temporal | `{session_id}::cancellation::{timestamp}` | `occurred_at` |
| ContextCompaction | `:ContextCompaction:SST_EVENT` | Temporal | `{session_id}::compaction::{timestamp}` | `occurred_at` |
| MountPlan | `:MountPlan:SST_THING` | Resource | `{session_id}::mount_plan` | `mount_plan_data` |
| Orchestrator | `:Orchestrator:SST_CONCEPT` | Abstract | Orchestrator name string (e.g. `loop-streaming`) | `name` |

### Data Layer 2 Edge Types

All edges carry an `sst_semantic` property that expresses the relationship's meaning.

| Edge Type | `sst_semantic` | From → To | What It Means |
|---|---|---|---|
| `HAS_EXECUTION` | `CONTAINS` | Session → OrchestratorRun | Session contains this orchestrator run (one per user turn) |
| `FORKED` | `LEADS_TO` | Session → ForkedSession | Session forked a child session |
| `HAS_ATTRIBUTE` | `EXPRESSES` | Session → Orchestrator | Session describes its orchestrator type |
| `HAS_PART` | `CONTAINS` | Session → MountPlan/Prompt/Cancellation | Session contains these parts |
| `HAS_PART` | `CONTAINS` | OrchestratorRun → Iteration | Run contains these LLM iterations |
| `HAS_PART` | `CONTAINS` | Iteration → ContentBlock | Iteration contains these content blocks |
| `HAS_TOOL_CALL` | `CONTAINS` | Iteration → ToolCall | Iteration contains these tool calls |
| `HAS_COMPACTION` | `CONTAINS` | Session → ContextCompaction | Session contains this compaction event |
| `HAS_SUBSESSION` | `LEADS_TO` | Session → SubSession | Session leads to a sub-session |
| `CAUSED` | `LEADS_TO` | ContentBlock → ToolCall | This content block triggered this tool call |
| `PARALLEL_EXECUTION` | `NEAR` | ToolCall ↔ ToolCall | These tool calls ran concurrently in the same parallel group |
| `TRIGGERS` | `LEADS_TO` | Prompt → OrchestratorRun | This prompt started this orchestrator run |
| `ENABLES` | `LEADS_TO` | OrchestratorRun → Prompt | This run's completion enabled the next prompt |
| `SOURCED_FROM` | (none) | data_layer_2 entity → data_layer_1 Event | Cross-layer provenance bridge. Every data layer 2 entity has one SOURCED_FROM edge per contributing raw event. No `sst_semantic` — infrastructure, not SST model. |

---

## Section 3 — SST Navigation (Reasoning by Semantic Type)

The data layer 2 schema uses SST type labels to classify every node by its fundamental
character. These labels let you query across entity boundaries without knowing specific
node labels in advance.

### Querying by SST Type Label

Three SST type labels partition the semantic layer:

| SST Label | Meaning | Data Layer 2 Entities |
|---|---|---|
| `:SST_EVENT` | Temporal, bounded occurrence | Session, OrchestratorRun, Iteration, ContentBlock, ToolCall, Prompt, Cancellation, ContextCompaction |
| `:SST_THING` | Persistent resource or artifact | MountPlan |
| `:SST_CONCEPT` | Abstract, reusable identity | Orchestrator |

**Example — find all temporal events in the last session:**

```cypher
MATCH (s:Session {workspace: $workspace})
WITH s ORDER BY s.started_at DESC LIMIT 1
MATCH (s)-[:HAS_EXECUTION|HAS_PART*1..3]->(e:SST_EVENT)
RETURN labels(e) AS types, e.node_id, e.started_at
ORDER BY e.started_at
```

**Example — find all abstract concepts referenced by a session:**

```cypher
MATCH (s:Session {workspace: $workspace})-[:HAS_ATTRIBUTE]->(c:SST_CONCEPT)
RETURN c.name AS orchestrator_name
```

**Example — find all persistent resources (things) attached to sessions:**

```cypher
MATCH (s:Session {workspace: $workspace})-[:HAS_PART]->(t:SST_THING)
RETURN s.node_id AS session, labels(t) AS resource_type, t.node_id
```

### Querying by Edge Semantic

Every data layer 2 edge carries an `sst_semantic` property that expresses the relationship's
abstract meaning, independent of the concrete edge type. This lets you query causation,
containment, and concurrence uniformly.

| `sst_semantic` Value | Meaning | Concrete edges that carry it |
|---|---|---|
| `CONTAINS` | Part-of / containment relationship | `HAS_EXECUTION`, `HAS_PART`, `HAS_TOOL_CALL`, `HAS_COMPACTION` |
| `LEADS_TO` | Causal / sequential relationship | `FORKED`, `HAS_SUBSESSION`, `CAUSED`, `TRIGGERS`, `ENABLES` |
| `EXPRESSES` | Description / attribution relationship | `HAS_ATTRIBUTE` |
| `NEAR` | Concurrent / proximity relationship | `PARALLEL_EXECUTION` |

**Example — find all causal relationships emanating from a session:**

```cypher
MATCH (s:Session {workspace: $workspace, node_id: $session_id})-[r]->(target)
WHERE r.sst_semantic = 'LEADS_TO'
RETURN type(r) AS edge_type, r.sst_semantic, labels(target) AS target_type, target.node_id
```

**Example — find all concurrent tool calls in the session:**

```cypher
MATCH (tc1:ToolCall)-[r:PARALLEL_EXECUTION]-(tc2:ToolCall)
WHERE r.sst_semantic = 'NEAR'
  AND tc1.workspace = $workspace
RETURN tc1.tool_name, tc2.tool_name, tc1.parallel_group_id
```

### Hierarchical Traversal Pattern

Use variable-length paths with `HAS_EXECUTION|HAS_PART*` to traverse the full session
containment hierarchy in a single query. This pattern reaches any depth of the
Session → OrchestratorRun → Iteration → ContentBlock tree.

**Pattern — reach all descendants of a session:**

```cypher
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
      -[:HAS_EXECUTION|HAS_PART*]->(descendant)
RETURN labels(descendant) AS type, descendant.node_id
ORDER BY descendant.started_at
```

**Pattern — reach tool calls specifically (three-hop max):**

```cypher
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
      -[:HAS_EXECUTION|HAS_PART*1..3]->(iteration:Iteration)
      -[:HAS_TOOL_CALL]->(tc:ToolCall)
RETURN tc.tool_name, tc.started_at, tc.result_success
ORDER BY tc.started_at
```

The `*1..3` bound prevents runaway traversal on large sessions. Use `*` (unbounded) only
when the session hierarchy depth is known to be shallow.

### Turn Chain Pattern

The `TRIGGERS` and `ENABLES` edges form a chain that represents the conversation flow:
each user prompt triggers an orchestrator run, and each completed run enables the next
prompt. Traversing this chain reconstructs the turn-by-turn progression of a session.

**Pattern — walk the turn chain forward from the first prompt:**

```cypher
MATCH path = (p:Prompt {workspace: $workspace})
             -[:TRIGGERS]->(run:OrchestratorRun)
             -[:ENABLES]->(next_prompt:Prompt)
WHERE p.session_id = $session_id
RETURN [node IN nodes(path) | {type: labels(node), id: node.node_id, at: node.started_at}]
  AS turn_chain
ORDER BY p.started_at
```

**Pattern — count turns in a session:**

```cypher
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
      -[:HAS_PART]->(p:Prompt)
      -[:TRIGGERS]->(run:OrchestratorRun)
RETURN count(run) AS turn_count
```

---

## Section 4 — Cross-Layer Queries

Data layer 1 (raw events) and data layer 2 (semantic entities) coexist in the same graph.
The canonical way to move between them is the `SOURCED_FROM` edge. Two additional fallback
strategies cover cases where SOURCED_FROM edges are absent (older sessions ingested before
the SOURCED_FROM handler was deployed).

### Join 1 — SOURCED_FROM (Canonical)

Every data layer 2 entity is linked back to the raw data layer 1 event(s) that produced
it via `SOURCED_FROM` edges. This is the preferred join strategy because it is exact,
direction-aware, and does not require shared scalar keys.

```cypher
// Navigate from a ToolCall entity back to its source raw event
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
      -[:HAS_EXECUTION]->(run:OrchestratorRun)
      -[:HAS_PART]->(iter:Iteration)
      -[:HAS_TOOL_CALL]->(tc:ToolCall)
MATCH (tc)-[:SOURCED_FROM]->(pre:ToolPreEvent)
RETURN tc.tool_name          AS tool_name,
       tc.result_success      AS succeeded,
       pre.occurred_at        AS event_fired_at,
       pre.data               AS raw_payload
ORDER BY pre.occurred_at
```

```cypher
// Navigate in the reverse direction — from a raw event to the semantic entity it produced
MATCH (pre:ToolPreEvent {workspace: $workspace, session_id: $session_id})
MATCH (tc:ToolCall)-[:SOURCED_FROM]->(pre)
RETURN pre.tool_name          AS event_name,
       pre.occurred_at        AS fired_at,
       tc.result_success      AS succeeded,
       tc.result_output       AS output
ORDER BY pre.occurred_at
```

Use this join when you want to retrieve the raw event payload for a semantic entity, or
when you want the structured result for a raw event.

### Join 2 — ToolCall Direct Match (Fallback)

The `:ToolCall` data layer 2 node uses the provider's `tool_call_id` directly as its
`node_id`. The `:ToolPreEvent` data layer 1 node lifts the same identifier as its
`tool_call_id` property. This shared key is a direct join between the layers.

```cypher
// Find the semantic ToolCall entity for a given raw ToolPreEvent
MATCH (e:ToolPreEvent {workspace: $workspace, tool_call_id: $tool_call_id})
MATCH (tc:ToolCall {node_id: e.tool_call_id})
RETURN e.tool_name          AS event_tool_name,
       e.occurred_at        AS event_time,
       tc.result_success    AS succeeded,
       tc.result_output     AS output,
       tc.ended_at          AS completed_at
```

Use this join when SOURCED_FROM edges are absent (older sessions) and you have a
ToolPreEvent. It works only for ToolCall entities — other data layer 2 types do not
share a direct key with data layer 1.

### Join 3 — Session Containment (Fallback)

When you need to correlate raw events with the semantic structure of a session, join
through the shared `:Session` node. Data layer 1 uses `HAS_EVENT` to attach raw event
nodes. Data layer 2 uses `HAS_EXECUTION` and `HAS_PART` to attach semantic entities.

```cypher
// Correlate raw LLM response events with semantic iterations
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
MATCH (s)-[:HAS_EVENT]->(lre:LlmResponseEvent)          // data layer 1
MATCH (s)-[:HAS_EXECUTION]->(run:OrchestratorRun)
      -[:HAS_PART]->(iter:Iteration)                     // data layer 2
WHERE lre.iteration_number = iter.iteration_number
RETURN iter.iteration_number,
       lre.model            AS model,
       lre.occurred_at      AS responded_at,
       iter.started_at      AS iter_started
ORDER BY iter.iteration_number
```

The `s` (Session) node is the bridge: traverse `HAS_EVENT` to reach data layer 1 nodes,
traverse `HAS_EXECUTION`/`HAS_PART` to reach data layer 2 entities, then join on shared
scalar properties (`iteration_number`, `tool_call_id`, etc.).

### Workspace Scoping

Every query must be scoped to a workspace. The workspace is a partition key that prevents
results from bleeding across unrelated projects or users.

**Default workspace** — the `graph_query` tool automatically injects the configured
workspace as `$workspace`. Most queries use it without any explicit parameter:

```cypher
MATCH (s:Session {workspace: $workspace})
RETURN s.node_id, s.started_at
ORDER BY s.started_at DESC
LIMIT 10
```

**Explicit workspace parameter** — when the workspace differs from the default, pass it
explicitly in the `params` dict of the `graph_query` call:

```cypher
// Query with explicit workspace override
MATCH (s:Session {workspace: $workspace})
RETURN count(s) AS session_count
```

Invoke with `params: {"workspace": "my-other-project"}` to override the default.

**Cross-workspace queries** — pass `workspace: '*'` to query across all workspaces. Use
sparingly; cross-workspace queries skip the partition index and can be slow on large graphs:

```cypher
MATCH (s:Session)
WHERE s.workspace <> ''
RETURN s.workspace, count(s) AS sessions_per_workspace
ORDER BY sessions_per_workspace DESC
```

**Mandatory workspace placement** — always place `{workspace: $workspace}` on the anchor
node (the first `MATCH` pattern that establishes the starting point of the query). Do not
rely on downstream nodes or `WHERE` clauses alone to scope results. Placing the workspace
constraint on the anchor node allows the graph engine to use the workspace index and
avoids full graph scans:

```cypher
// CORRECT — workspace on anchor node Session
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
      -[:HAS_EXECUTION]->(run:OrchestratorRun)
RETURN run.orchestrator_name, run.started_at

// INCORRECT — workspace on a downstream node (misses the index)
MATCH (s:Session {node_id: $session_id})
      -[:HAS_EXECUTION]->(run:OrchestratorRun {workspace: $workspace})
RETURN run.orchestrator_name, run.started_at
```

---

## Section 5 — Blob Handling (Critical)

### The `data` Field Is a JSON String, Not a Cypher Map

The `data` property on every `:Event` node holds the original kernel event payload as a
**JSON-encoded string**. It is not a Cypher map. You cannot use dot notation (`e.data.tool_name`)
to access sub-fields from Cypher. The entire payload is stored as an opaque string and must
be parsed in application code or with a post-processing tool like `jq`.

```cypher
// Returns the raw JSON string — you must parse it outside Cypher
MATCH (e:ToolPreEvent {workspace: $workspace, tool_call_id: $tool_call_id})
RETURN e.data AS raw_payload
```

### ci-blob:// URI Replacement for Large Payloads

When an event payload exceeds the graph storage threshold, the server replaces the full
`data` string with a `ci-blob://` URI reference. The URI points to a blob store entry
that holds the original payload. The `data` field in this case looks like:

```
ci-blob://SESSION_ID/EVENT_KEY
```

The presence of a `ci-blob://` value in `data` means the full payload is too large to
store inline and must be retrieved separately using `blob_read`.

### Agent Workflow for Blob-Aware Data Extraction

When writing queries that access the `data` field, always follow this four-step workflow:

**Step 1 — Run the Cypher query and retrieve the `data` field:**

```cypher
MATCH (e:LlmResponseEvent {workspace: $workspace})
WHERE e.session_id = $session_id
RETURN e.node_id, e.data
ORDER BY e.occurred_at DESC
LIMIT 5
```

**Step 2 — Inspect each `data` value. If it starts with `ci-blob://`, it is a blob
reference. Do NOT try to parse it as JSON.**

**Step 3 — For blob references, call `blob_read` with the URI. `blob_read` returns a
file path on the local filesystem — it does NOT return the content directly:**

```python
# blob_read returns {"file_path": "/tmp/ci-blobs/SESSION_ID/EVENT_KEY.json"}
# The content is at the file_path, not in the return value
result = blob_read(uri="ci-blob://SESSION_ID/EVENT_KEY")
file_path = result["file_path"]
```

**Step 4 — Extract the fields you need with `jq`. Never load the full blob into the
agent's context — large blobs can be tens of thousands of tokens:**

```bash
# Extract a specific field from the blob file using jq
jq '.messages[-1].content' /tmp/ci-blobs/SESSION_ID/EVENT_KEY.json

# Extract just the top-level keys to understand the structure
jq 'keys' /tmp/ci-blobs/SESSION_ID/EVENT_KEY.json

# Extract a nested field safely with a fallback
jq '.response.usage // "no usage data"' /tmp/ci-blobs/SESSION_ID/EVENT_KEY.json
```

### Rules for Blob Handling

- **Never load the full blob** — always use `jq` to extract only the fields you need.
- **`blob_read` returns a file path**, not content — dereference the path, then read.
- **Check for `ci-blob://` before parsing** — treat any `data` value that starts with
  `ci-blob://` as a URI, not as JSON.
- **Lifted properties bypass blobs** — commonly needed fields (e.g. `tool_name`,
  `tool_call_id`, `model`, `provider`) are lifted onto the node as top-level properties
  during ingestion. Query lifted properties directly from Cypher rather than fetching
  blobs when the lifted field is sufficient.

---

## Section 6 — Discovery Patterns (Verified Cypher)

The following patterns are verified to work against the data layer 2 schema. All use
`$workspace` as the workspace parameter automatically injected by `graph_query`.

### Pattern 1 — Full Conversation Turn Trace

Reconstructs the complete turn-by-turn flow of a session: each prompt, the orchestrator
run it triggered, the iterations within that run, and the tool calls in each iteration.

```cypher
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
      -[:HAS_PART]->(p:Prompt)
      -[:TRIGGERS]->(run:OrchestratorRun)
      -[:HAS_PART]->(iter:Iteration)
      -[:HAS_TOOL_CALL]->(tc:ToolCall)
RETURN p.started_at           AS turn_start,
       run.orchestrator_name  AS orchestrator,
       iter.iteration_number  AS iteration,
       tc.tool_name           AS tool,
       tc.result_success      AS succeeded,
       tc.started_at          AS tool_start
ORDER BY p.started_at, iter.iteration_number, tc.started_at
```

### Pattern 2 — Tool Usage Per LLM Iteration

Counts and lists every tool call grouped by which LLM iteration fired it. Useful for
understanding how many tools each iteration invoked and what they were.

```cypher
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
      -[:HAS_EXECUTION]->(run:OrchestratorRun)
      -[:HAS_PART]->(iter:Iteration)
      -[:HAS_TOOL_CALL]->(tc:ToolCall)
RETURN iter.iteration_number  AS iteration,
       collect(tc.tool_name)  AS tools_called,
       count(tc)              AS tool_count
ORDER BY iter.iteration_number
```

### Pattern 3 — Parallel Tool Groups

Finds all tool calls that executed concurrently within the same parallel group. The
`parallel_group_id` property identifies the group; `PARALLEL_EXECUTION` edges connect
the members directly.

```cypher
MATCH (tc1:ToolCall {workspace: $workspace})
      -[:PARALLEL_EXECUTION]-(tc2:ToolCall)
WHERE tc1.node_id < tc2.node_id  // deduplicate undirected pairs
  AND tc1.session_id = $session_id
RETURN tc1.parallel_group_id   AS group_id,
       tc1.tool_name            AS tool_a,
       tc2.tool_name            AS tool_b,
       tc1.started_at           AS started_at
ORDER BY tc1.started_at
```

### Pattern 4 — ContentBlock → ToolCall Causation

Traces which content block in the LLM response caused each tool call to be issued.
The `CAUSED` edge from `ContentBlock` to `ToolCall` expresses this direct causation.

```cypher
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
      -[:HAS_EXECUTION]->(run:OrchestratorRun)
      -[:HAS_PART]->(iter:Iteration)
      -[:HAS_PART]->(block:ContentBlock)
      -[:CAUSED]->(tc:ToolCall)
RETURN iter.iteration_number   AS iteration,
       block.block_index        AS block_index,
       block.block_type         AS block_type,
       tc.tool_name             AS tool_triggered,
       tc.result_success        AS succeeded
ORDER BY iter.iteration_number, block.block_index
```

### Pattern 5 — Session Comparison

Compares two sessions side by side: total turns, total iterations, total tool calls,
and success rate. Useful for comparing agent behavior across sessions.

```cypher
MATCH (s:Session {workspace: $workspace})
WHERE s.node_id IN [$session_id_a, $session_id_b]
OPTIONAL MATCH (s)-[:HAS_PART]->(p:Prompt)-[:TRIGGERS]->(run:OrchestratorRun)
OPTIONAL MATCH (run)-[:HAS_PART]->(iter:Iteration)
OPTIONAL MATCH (iter)-[:HAS_TOOL_CALL]->(tc:ToolCall)
RETURN s.node_id                                          AS session,
       count(DISTINCT p)                                  AS turns,
       count(DISTINCT iter)                               AS iterations,
       count(DISTINCT tc)                                 AS tool_calls,
       sum(CASE WHEN tc.result_success THEN 1 ELSE 0 END) AS successful_tools
ORDER BY s.node_id
```

### Pattern 6 — Failed Tool Calls

Lists every tool call that failed (result_success is false), including the error message
and the session it belongs to. Useful for diagnosing error-prone sessions.

```cypher
MATCH (s:Session {workspace: $workspace})
      -[:HAS_EXECUTION]->(run:OrchestratorRun)
      -[:HAS_PART]->(iter:Iteration)
      -[:HAS_TOOL_CALL]->(tc:ToolCall)
WHERE tc.result_success = false
RETURN s.node_id              AS session,
       iter.iteration_number  AS iteration,
       tc.tool_name           AS tool,
       tc.result_error        AS error,
       tc.started_at          AS failed_at
ORDER BY tc.started_at
```

### Pattern 7 — Data Layer 1 / Data Layer 2 Cross-Layer Join

Joins raw `:ToolPreEvent` nodes (data layer 1) with semantic `:ToolCall` entities
(data layer 2) using the shared `tool_call_id` key. Returns both the raw event timestamp
and the structured result from the semantic layer.

```cypher
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
      -[:HAS_EVENT]->(pre:ToolPreEvent)
MATCH (tc:ToolCall {node_id: pre.tool_call_id})
RETURN pre.tool_name          AS tool_name,
       pre.occurred_at        AS event_fired_at,
       tc.result_success      AS succeeded,
       tc.result_error        AS error,
       tc.result_output       AS output_preview,
       tc.ended_at            AS completed_at
ORDER BY pre.occurred_at
```

### Pattern 8 — SOURCED_FROM Cross-Layer Navigation

Navigates from a semantic `:ToolCall` entity (data layer 2) through its `SOURCED_FROM`
edge to the originating `:ToolPreEvent` (data layer 1). Returns both the structured
result stored on the semantic entity and the raw event timestamp from the event stream.
Use this pattern as the canonical cross-layer join when SOURCED_FROM edges are present.

```cypher
MATCH (s:Session {workspace: $workspace, node_id: $session_id})
      -[:HAS_EXECUTION]->(run:OrchestratorRun)
      -[:HAS_PART]->(iter:Iteration)
      -[:HAS_TOOL_CALL]->(tc:ToolCall)
MATCH (tc)-[:SOURCED_FROM]->(pre:ToolPreEvent)
RETURN iter.iteration_number  AS iteration,
       tc.tool_name            AS tool,
       tc.result_success       AS succeeded,
       pre.occurred_at         AS event_fired_at,
       pre.data                AS raw_payload
ORDER BY pre.occurred_at
```

---

## Gotchas

**1. Data layer 2 nodes only exist if handlers ran.**
Semantic entities (OrchestratorRun, Iteration, ContentBlock, ToolCall, etc.) are
created by data layer 2 handlers during event ingestion. If a session was ingested
before data layer 2 was deployed, or if the handler for a specific event type is
disabled, those nodes will not exist. Always use `OPTIONAL MATCH` when joining
data layer 2 entities against unknown sessions.

**2. `result_success: false` signals the error path.**
A `:ToolCall` node with `result_success = false` means the tool returned an error.
The `result_error` property holds the error message. A missing `result_success`
property (null) means the `ToolPostEvent` or `ToolErrorEvent` has not been processed
yet — the tool call is still in-flight or the handler did not run.

**3. `data` is a JSON string, not a Cypher map.**
The `data` property on `:Event` nodes is a serialized JSON string. You cannot access
`e.data.tool_name` in Cypher. Lifted properties (`tool_name`, `tool_call_id`, `model`,
etc.) are your first resort. When you need raw payload fields not lifted, retrieve the
`data` string and parse it with `jq` outside Cypher (see Section 5).

**4. `ENABLES` edges are sparse.**
The `ENABLES` edge from `OrchestratorRun` to the next `Prompt` is only written when
the session has a multi-turn chain. Single-turn sessions and sessions where the run
ended without a follow-up prompt will have no `ENABLES` edge. For a session with N
prompts, there are exactly N−1 `ENABLES` edges (each run connects to the next prompt,
but the last run has no successor). Do not rely on `ENABLES` existing to determine if
a session ended cleanly.

**5. Workspace scoping is mandatory.**
Every query must include `{workspace: $workspace}` on the anchor node. Omitting the
workspace filter causes a full graph scan and may return results from unrelated projects
or users. The `graph_query` tool automatically injects `$workspace` — always include
it in the first `MATCH` pattern.

**6. The node MERGE key is `{node_id, workspace}`.**
Data layer 2 nodes are merged using the composite key `{node_id, workspace}`. This
means the same logical entity (e.g. an Orchestrator named `loop-streaming`) can exist
as separate nodes in different workspaces. Cross-workspace queries (passing `workspace:
'*'`) will return one node per workspace, not one node per unique `node_id`. Account
for this when aggregating across workspaces.

**7. `SOURCED_FROM` edges may be absent on older sessions.**
Sessions ingested before the SOURCED_FROM handler was deployed will not have any
cross-layer provenance edges. To check which data layer 2 nodes are missing their
source link, run:

```cypher
MATCH (n:SST_EVENT) WHERE NOT (n)-[:SOURCED_FROM]->() AND NOT n:Session RETURN labels(n), count(*)
```

If this returns results, fall back to Join 2 (ToolCall Direct Match) or Join 3
(Session Containment) for those sessions.
