# Resource Limits and Crash-Loop Guards

How to keep a host install from consuming the machine it runs on, and how to
notice when it is already happening.

Nothing here is speculative tuning. Every number below was measured during a
real two-day outage on a host install, and every remediation was verified to
work on that same host. The guards are cheap, they are a one-time edit to the
Neo4j container and the systemd unit, and without them the failure mode is
**silent** — the only user-visible symptom is a graph that quietly stops
updating.

| Goal | Use |
|------|-----|
| Install the service in the first place | [docs/service-setup.md](service-setup.md) |
| Run Neo4j locally for development | [docs/local-development.md](local-development.md) |
| Managed/production deployment | [docs/azure-deployment.md](azure-deployment.md) |
| **Stop a host install from eating the host** | **this guide** |

---

## 1. The incident

A host install with **121 GB RAM** and **3.7 TB disk** stopped updating its
graph. It took about four and a half hours before anyone noticed, and the
machine had by then been in a crash loop for two days.

| What | Measured |
|------|----------|
| Durable queue spool (`queues_path`) | **38 GB across 583 files**; largest single file **4.9 GB** |
| Neo4j process | **22 GB** — no JVM heap cap, no container memory limit |
| Server startup (`lifespan_startup`) | `crash recovery respawned 94/94 drainers`, **~4 minutes**, peak **43.9 GB RSS** |
| Outcome of that startup | **SIGKILL** from the kernel OOM killer |
| Restart behavior | `Restart=on-failure`, **no `StartLimitBurst`** → **16 worker boots / 30 min, for two days** (≈1,500 boots), serving zero requests |
| Cost | **9.5 CPU-hours** burned; graph silently **~4.5 hours stale** |

The loop could never have escaped on its own. Sixteen boots per thirty minutes
is a boot every **~112 seconds**, and the crash-recovery pass needed **~4
minutes** — the process was killed and restarted well before it could finish
draining, so every attempt redid the same work from the same 38 GB spool and
died at the same place.

### Why nothing caught it

Three independently unbounded things compounded. Any one of them alone is
survivable:

1. **The spool had no ceiling and no signal.** 38 GB of undrained events is
   not an error state — it is a queue doing exactly what a durable queue does.
2. **Neo4j auto-sized from host RAM.** With no container memory limit, the
   Neo4j startup script sizes the page cache and the JVM sizes its heap from
   what the *host* reports — 121 GB — not from what the operator intended.
3. **Startup cost scales with the spool.** Crash recovery respawns one drainer
   per session with an undrained line (94 of them here) and reads from disk to
   rebuild the conservation baseline. A big spool makes boot expensive; an
   expensive boot is where the memory peak lives.

Add a restart policy with no give-up condition and the result is a machine
that spends two days failing at exactly the same point, at 20% of a core,
with no alert anywhere.

---

## 2. Cap Neo4j

Set both an explicit heap/page-cache **and** a container memory limit. They do
different jobs: the explicit settings make the sizing deterministic, and the
container limit is the backstop that bounds anything they don't cover (JVM
off-heap, native allocations, transaction memory).

### New container

```bash
docker run -d \
  --name amplifier-context-intelligence-neo4j \
  --restart unless-stopped \
  --memory 24g --memory-swap 24g \
  -p 37474:7474 -p 37687:7687 \
  -e NEO4J_AUTH=neo4j/"${NEO4J_PASSWORD}" \
  -e 'NEO4J_PLUGINS=["apoc","graph-data-science"]' \
  -e 'NEO4J_dbms_security_procedures_unrestricted=apoc.*,gds.*' \
  -e NEO4J_server_memory_heap_initial__size=10g \
  -e NEO4J_server_memory_heap_max__size=10g \
  -e NEO4J_server_memory_pagecache_size=10g \
  -v "${DATA_DIR}/neo4j:/data" \
  neo4j:5.26.22-community
```

**Sizing rule:** `--memory` ≥ heap + page cache + ~3–4 GB of headroom for JVM
off-heap and native allocations. The `24g / 10g / 10g` split above is what the
incident host runs; scale it to your box, but keep the headroom — a container
limit set *equal* to heap + page cache turns a normal transaction spike into an
OOM kill.

> **The env-var spelling is not obvious.** The Neo4j image maps
> `NEO4J_<setting>` to a `neo4j.conf` key with `.` → `_` and `_` → `__`, so
> `server.memory.heap.max_size` becomes `NEO4J_server_memory_heap_max__size`
> (**double** underscore before `size`). Get it wrong and the variable is
> silently ignored. Verify after start:
> ```bash
> docker exec amplifier-context-intelligence-neo4j \
>   grep -E '^server\.memory' /var/lib/neo4j/conf/neo4j.conf
> # → server.memory.heap.max_size=10g
> #   server.memory.heap.initial_size=10g
> #   server.memory.pagecache.size=10g
> ```

> **Why explicit settings, not just `--memory`.** With a container limit and no
> heap env vars, Neo4j writes only an auto-computed
> `server.memory.pagecache.size` and passes **no `-Xmx` at all** — heap is left
> to JVM ergonomics as a percentage of the cgroup limit. That is survivable but
> not something you chose. Set both and the number is yours:
> ```bash
> docker exec amplifier-context-intelligence-neo4j \
>   sh -c "ps -eo args | tr ' ' '\n' | grep -E '^-X(mx|ms)'"
> # → -Xms10485760k
> #   -Xmx10485760k
> ```

### Already-running container

A limit can be applied without recreating the container, but **the JVM reads
its sizing once, at startup** — so the limit does nothing for the running
process until you restart it:

```bash
docker update --memory 24g --memory-swap 24g amplifier-context-intelligence-neo4j
docker restart amplifier-context-intelligence-neo4j
docker stats --no-stream amplifier-context-intelligence-neo4j
```

This is the step that reclaimed **~21 GB** on the incident host. Note it only
gives you the cgroup backstop; add the heap/page-cache env vars (which requires
recreating the container) to make the sizing explicit.

---

## 3. Cap the server, and make it give up

The unit in [service-setup.md §5](service-setup.md#5-linux--systemd-user-service)
ships with these guards. If you installed before they existed, your unit has
`MemoryMax=infinity` and no start limit — add all four:

```ini
[Unit]
# Refuse to start again after more than 5 starts within 10 minutes. Without
# this the unit restarts forever; the incident above burned two days and
# 9.5 CPU-hours failing at the same point every ~112 seconds.
StartLimitIntervalSec=600
StartLimitBurst=5

[Service]
# Attributable, bounded failure instead of a host-wide OOM event.
MemoryHigh=20G
MemoryMax=24G
```

> **`StartLimitIntervalSec` and `StartLimitBurst` go in `[Unit]`, not
> `[Service]`.** They are documented in `systemd.unit(5)`, not
> `systemd.service(5)` (older systemd releases took them in `[Service]`, which
> is where most stale copy-paste comes from), and are **silently ignored** in
> the wrong section. There is no runtime error — the unit starts fine and simply
> restarts forever. Check for it:
> ```bash
> systemd-analyze verify --user ~/.config/systemd/user/context-intelligence-server.service
> # A misplaced directive prints:
> #   Unknown key name 'StartLimitIntervalSec' in section 'Service', ignoring.
> ```
> `systemd-analyze verify` **exits 0 either way** — the warning line is the
> result, not the exit code.

`MemoryHigh` throttles and reclaims at 20 GB (a slowdown, recoverable);
`MemoryMax` is the hard wall at 24 GB where the cgroup OOM killer fires. Both
are cgroup-v2 directives.

Apply and verify:

```bash
systemctl --user daemon-reload
systemctl --user restart context-intelligence-server
systemctl --user show context-intelligence-server -p MemoryHigh -p MemoryMax
# → MemoryHigh=21474836480
#   MemoryMax=25769803776     (NOT "infinity")
```

> **User services need the memory controller delegated.** On a `systemctl --user`
> unit, `MemoryMax` is enforced only if the memory controller reaches your user
> slice. Confirm before you trust it:
> ```bash
> cat /sys/fs/cgroup/user.slice/user-$(id -u).slice/cgroup.controllers
> # must include: memory
> ```
> If `memory` is absent, the directives parse cleanly and enforce nothing. Run
> the server as a **system** unit instead (same directives, `sudo systemctl`),
> or enable delegation for the user slice.

> **`StartLimitBurst` bounds *unit* restarts only.** The server runs
> `gunicorn` (master) + exactly one `UvicornWorker`. If the OOM killer takes the
> **worker** and the master survives, gunicorn respawns it internally and
> systemd never counts a restart. To tell which is happening, compare
> `NRestarts` (unit restarts) against the number of startup log lines (process
> boots) — see §6.

---

## 4. Bound the drain burst — `write_concurrency`

`write_concurrency` (default **8**) caps how many Neo4j write flushes run
concurrently across all session drainers. It is a starvation guard, but it is
also the knob that decides **peak Neo4j transaction memory during a backlog
drain** — precisely the situation a restart-with-backlog creates.

A sibling deployment carries this in its config, and it is the best evidence
available in the wild:

```yaml
# Bound peak Neo4j transaction memory during backlog drains (default 8 thundered
# neo4j's ~20.6GiB tx-memory ceiling on a restart-with-backlog -> 307 OOM/hr).
write_concurrency: 4
```

Eight concurrent drains of a large backlog were enough to hit that instance's
~20.6 GiB transaction-memory ceiling and produce **307 OOMs per hour**. Halving
it to 4 stopped that.

**Set `write_concurrency: 4` on any host install with a Neo4j instance whose
transaction memory you have not explicitly sized.** The cost is drain
throughput, which matters only while a backlog exists; the benefit is that the
backlog drain cannot be the thing that kills the database it is draining into.
A second host still running the default carries this hazard today.

---

## 5. Keep the spool out of the boot path

`queues_path` holds one durable append-log per session (`.log` / `.offset` /
`.dead.jsonl`). It is meant to be transient — drained and deleted. When
draining stalls, it is not.

There is **no size cap and no alert**. Boot cost scales with it: crash recovery
respawns one drainer per session with an undrained line and reads from disk to
rebuild the conservation baseline, so a large spool converts every restart into
a multi-minute, memory-heavy operation. That is what turned a recoverable stall
into a two-day loop.

If a boot is already failing on an oversized spool, move it out of the way so
the server can start, then triage it separately:

```bash
DATA_DIR="$HOME/amplifier-context-intelligence-server-data-store"
systemctl --user stop context-intelligence-server
mv "${DATA_DIR}/queues" "${DATA_DIR}/queues.archived-$(date +%F-%H%M)"
mkdir -p "${DATA_DIR}/queues"
systemctl --user start context-intelligence-server
```

> **This is data movement, not cleanup.** Every undrained line in that archive
> is an accepted event that never reached the graph. Archive it — do not delete
> it — and decide deliberately whether to replay or discard once the server is
> healthy again. Keep an eye on disk: the archive is still 38 GB.

---

## 6. How to tell this is happening to you

The failure is silent by construction: `POST /events` returns `202` the moment
the event is durably appended, so **clients see success while nothing reaches
the graph**. The only organic symptom is someone noticing the graph is stale.
Check these directly.

| Symptom | Command | What bad looks like |
|---------|---------|---------------------|
| Graph stopped updating | `curl -s http://localhost:8000/status \| python3 -c "import json,sys;[print(s['session_id'],s['last_successful_flush'],s['events_processed']) for s in json.load(sys.stdin)['sessions']]"` | `last_successful_flush` (unix seconds) hours old while `events_processed` keeps climbing |
| Backlog / loss accounting | `curl -s http://localhost:8000/status \| python3 -c "import json,sys; print(json.load(sys.stdin)['metrics'])"` | `in_queue_total` climbing and not falling; `degraded: true`; non-zero `dead_letter_total` |
| Dead drainers | `curl -s http://localhost:8000/status \| python3 -c "import json,sys; print(json.load(sys.stdin)['orphaned_sessions'])"` | non-zero — drain tasks that completed and stopped |
| Spool growth | `du -sh ~/amplifier-context-intelligence-server-data-store/queues` | GB, not MB |
| Spool file count / worst offender | `find ~/amplifier-context-intelligence-server-data-store/queues -name '*.log' \| wc -l` then `ls -lhS ~/amplifier-context-intelligence-server-data-store/queues/*.log \| head -5` | hundreds of files; any single file over ~1 GB |
| Crash-looping | `systemctl --user show context-intelligence-server -p NRestarts -p Result -p ActiveEnterTimestamp` | `NRestarts` climbing; `Result=oom-kill`; `ActiveEnterTimestamp` only minutes ago on a service you started days ago |
| Boots (incl. gunicorn-internal worker respawns) | `journalctl --user -u context-intelligence-server --since '-30min' --no-pager \| grep -c 'crash recovery respawned'` | more than 1 — that line is emitted once per successful startup |
| Startups that never finish | `journalctl --user -u context-intelligence-server --since '-2h' --no-pager \| grep 'crash recovery respawned'` | **nothing**, while the unit keeps restarting — every boot dies before finishing recovery |
| OOM kills in the server's cgroup | `CG=$(systemctl --user show context-intelligence-server -p ControlGroup --value); cat "/sys/fs/cgroup${CG}/memory.events"` | `oom_kill` non-zero (also `max`/`high` counters rising) |
| Host-level OOM kills | `journalctl -k --since '-2h' --no-pager \| grep -i 'out of memory'` | the kernel naming your process |
| Neo4j size | `docker stats --no-stream amplifier-context-intelligence-neo4j` | `MEM USAGE / LIMIT` showing a limit of the host's total RAM |

For a **system** unit, drop `--user` from the `systemctl`/`journalctl` commands
and use `sudo systemctl`; the cgroup path from
`systemctl show … -p ControlGroup --value` works the same way.

Two of these are worth a periodic check even when nothing looks wrong: `du -sh`
on the spool, and `NRestarts`. Both are cheap, and both would have caught this
on day one.

---

## 7. What these guards buy you — and what they don't

Be clear about the claim. `MemoryMax` does **not** guarantee the server never
runs out of memory. It guarantees the failure is **bounded and attributable**:

| Guard | Prevents | Does **not** prevent |
|-------|----------|----------------------|
| Neo4j heap + `--memory` | Neo4j auto-sizing from host RAM and crowding out everything else | Neo4j OOMing *inside* its limit if a transaction genuinely needs more |
| `MemoryMax` / `MemoryHigh` | The server taking the host down with it; an unattributable kernel OOM kill | The server being killed — it just gets killed **in its own cgroup**, with `memory.events` naming it |
| `StartLimitIntervalSec` / `StartLimitBurst` | An infinite restart loop burning CPU for days | The underlying failure — the unit stops in `failed` state and **stays down** until you fix it |
| `write_concurrency: 4` | A backlog drain thundering Neo4j's transaction-memory ceiling | A backlog from forming in the first place |

The point of every one of them is the same: convert a silent, unbounded,
self-perpetuating failure into a loud, bounded, one-shot one that shows up in
`systemctl status` as `failed`. **After a start-limit trip the service is down
and will not come back on its own** — `systemctl --user reset-failed
context-intelligence-server` clears the counter once the cause is fixed. That
is the intended behavior, and it is strictly better than what the incident did.

Verified on the incident host after all four changes: **14/14 status probes
returned 200 over 10 minutes, memory plateaued at 1.2 GB, 1 worker, 0
restarts.**

---

## See also
- [docs/service-setup.md](service-setup.md) §5 — the systemd unit these guards live in.
- [docs/service-setup.md](service-setup.md) §9 — troubleshooting table.
- [docs/local-development.md](local-development.md) §1 — the local Neo4j container.
- [docs/remote-access-sharing.md](remote-access-sharing.md) §7 — graph backup, and the
  matching warning that ingested data grows unbounded with no ingress cap.
