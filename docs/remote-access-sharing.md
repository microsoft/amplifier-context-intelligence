# Sharing the Server with Trusted Peers over Tailscale

How to let a few trusted people on **other networks** send their Amplifier sessions
to your context-intelligence server, exposing the **minimum possible**: a single
endpoint, over an encrypted overlay, with nothing else reachable and no public
internet exposure.

This guide complements the existing options — it is the path for **sharing with a
handful of external people** without standing up a public endpoint:

| Goal | Use |
|------|-----|
| Lock a public/LAN port to trusted source IPs | README → "Network Access and Security" (firewall rule) |
| Local/dev HTTPS in front of the server | [docs/service-setup.md](service-setup.md) §10 (Caddy) |
| Production, public, managed TLS | [docs/azure-deployment.md](azure-deployment.md) |
| **Share with a few external peers, privately** | **this guide** |

> **Note:** This guide uses [Tailscale](https://tailscale.com) as the overlay
> network. The same principles (bind to loopback, expose one path, scope per peer)
> apply to any WireGuard-based mesh.

---

## 1. What a peer's client actually needs

The Amplifier client hook sends exactly one kind of request:

```
POST /events     Authorization: Bearer <API_KEY>
```

It never connects to Neo4j (bolt `7687` / browser `7474`) and uses no other
endpoint. So a peer needs reachability to **one path on one port** — `POST /events`
— and nothing else. The entire design below follows from that fact.

The bearer token is the server's `api_key` (see the auth model in the README). Keep
it set; it is your application-layer gate even inside the tunnel.

---

## 2. Architecture (least access)

```
peer's Amplifier ──WireGuard──► tailscale serve (HTTPS :443)
                                  mount: ONLY /events ──► http://127.0.0.1:8000/events
                                  (every other path → 404, never proxied)
server (gunicorn) bound 127.0.0.1:8000 only   ·   Neo4j bound to loopback, never shared
```

A single `tailscale serve` mount does **two** jobs: it terminates TLS (with an
automatic MagicDNS certificate) **and** acts as a path allowlist. The second job
matters because several server endpoints are intentionally unauthenticated
(`/status`, `/version`, `/`, `/dashboard`, `/docs`, `/openapi.json`, `/logs/stream`,
`/static/*`, `/skills/*`). That exempt set is defined in code and is **not**
configurable, so the only way to keep those endpoints away from peers is to not
route them — which scoping `serve` to `/events` does cleanly.

---

## 3. Lock the host bindings

By default the server binds `0.0.0.0:8000` and the bundled `docker-compose.yml`
publishes Neo4j's `7474`/`7687` on **all interfaces**. For a shared deployment,
close both so Tailscale is the *only* ingress.

In `docker-compose.yml`:

```yaml
  context-intelligence-server:
    ports:
      - "127.0.0.1:8000:8000"   # was "8000:8000" — loopback only

  neo4j:
    # remove the host "ports:" mapping entirely (7474/7687).
    # The server reaches Neo4j over the internal compose network (bolt://neo4j:7687).
    # For local browser/debug access use: docker compose exec neo4j cypher-shell
```

(If you run the server standalone rather than via Compose, set
`server_host: 127.0.0.1` in your config instead.)

> **Gate — prove it before continuing.** From another machine on your LAN:
> `curl http://<this-host-lan-ip>:8000/status` must be **refused**, and
> `nc -vz <this-host-lan-ip> 7474 7687` must be **refused**. Only `127.0.0.1`
> should answer. A loopback bind also means no sibling Docker container can reach
> the server via the bridge gateway.

---

## 4. Expose only `/events` over the tailnet

```bash
tailscale serve --bg --https=443 --set-path=/events http://127.0.0.1:8000/events
tailscale serve status
```

`tailscale serve` matches paths like Go's `ServeMux`: mounting **only** `/events`
leaves every other path unserved (404). It cannot filter by HTTP method, which is
fine — `/events` requires the bearer token, and other verbs return 405 from the app.

> **Important — verify the path rewrite.** Tailscale's docs don't specify whether
> `--set-path` strips the prefix before forwarding. Confirm with a request from a
> tailnet device:
> `curl -s -o /dev/null -w '%{http_code}' https://<node>.<tailnet>.ts.net/events`
> must return **401** (reached the app's `/events`, correctly demanding a token).
> A **404** means it double-prefixed to `/events/events` — drop the path from the
> target (`…:8000`) or adjust `--set-path`, then re-test.

> **Important — `serve` exposes the whole `:443` listener.** Tailscale ACLs are
> port-level, not path-level. Any **other** `tailscale serve` mount on this node
> (e.g. a different local app at `/`) is *also* reachable by anyone you share the
> node with. Only share a node whose entire `serve` surface you're comfortable
> exposing to those peers. To check: `tailscale serve status` should show only the
> `/events` mount (or only mounts you intend peers to reach).

---

## 5. Share the node and scope access — read this section carefully

There are two independent gates: **sharing** controls *which machine* a peer can
reach; **ACL grants** control *which ports* on it.

### 5a. Share the machine, per peer
In the Tailscale admin console: **Machines → (your node) → Share** → invite each
peer by the email they sign into Tailscale with (the share must show **Accepted**).
Each accepts with their own (free) account. Sharing grants access to **only that one
machine**, and is revocable per person.

### 5b. The ACL lesson that makes or breaks this

> **The single most important lesson in this guide.** A default tailnet policy
> contains an allow-all grant:
> ```json
> { "src": ["*"], "dst": ["*"], "ip": ["*"] }
> ```
> **`*` includes shared/invited external users** (it expands to include
> `autogroup:shared`), and **grants are additive — a more-specific grant never
> overrides a broader one; the policy engine applies the union.** Therefore, if you
> leave `src: ["*"]` in place and add a narrow `tcp:443` grant for your peers, the
> peers still get **every port** on the shared machine. Your narrow grant is
> cosmetic.

The fix is to make the broad rule **not match shared users**. Change its source
from `*` to `autogroup:member` (all *direct* members of your own tailnet, which
**excludes** shared users) — or to an explicit group of your own users. Then the
*only* grant a shared peer matches is your narrow one.

```jsonc
{
  "hosts": { "ci-server": "100.x.y.z" },   // your node's stable Tailscale IP (or use a tag)

  "grants": [
    // Your own members keep full access. NOTE: src is autogroup:member, NOT "*".
    { "src": ["autogroup:member"], "dst": ["*"], "ip": ["*"] },

    // Shared peers: ONLY tcp:443 on the server, nothing else. autogroup:shared
    // auto-includes every accepted-share user — no email string to mistype.
    { "src": ["autogroup:shared"], "dst": ["ci-server"], "ip": ["tcp:443"] }
  ]
}
```

Notes:
- **Prefer `autogroup:shared`** for the shared grant (above): it covers every
  current and future shared peer with no email to get wrong. To restrict to
  *specific* peers, list their exact emails instead — but see the testing caveat
  below. (`autogroup:shared` is valid only as a `src`, never a `dst`.)
- Reference the node in `dst` via a `hosts` alias or a tag — MagicDNS device names
  are not a documented grant selector.
- Your existing `ssh` and Funnel (`nodeAttrs`) rules that key on
  `autogroup:member` already exclude shared users — peers get neither.

> **The ACL Tests feature cannot validate `autogroup:shared`.** Tailscale's `tests`
> block evaluates a *static* policy, but `autogroup:shared` membership is *dynamic* —
> it's populated by who has actually accepted a share. A test such as
> `{ "src": "peer@example.com", "accept": ["ci-server:443"] }` reports **Drop**
> against an `autogroup:shared` grant **even when everything is correct** — a false
> negative; don't chase it. Verify instead via the Machines page (the share shows
> **Accepted**) and the peer's real request (§5c). For a *testable* rule, temporarily
> set the grant `src` to the peer's explicit email (a literal match the test *can*
> evaluate), confirm green, then switch back to `autogroup:shared`.

### 5c. Verify from the peer's side (and read these symptoms)
A Tailscale ACL deny **drops packets silently**, so a wrong or missing grant shows
up as a **connection timeout, not "refused."** And a shared node is **remapped to a
different `100.x` IP in the peer's tailnet** (1-1 NAT) — so the peer must connect by
the **owner-tailnet MagicDNS name** `https://<node>.<tailnet>.ts.net`, **never** a
raw IP, with MagicDNS enabled on their side.

From a **shared** peer's machine:
```bash
curl -m 10 -o /dev/null -w '%{http_code}\n' https://<node>.<tailnet>.ts.net/events
```
- `401` → reachable and correctly token-gated. The other ports (`8000`/`7474`/`7687`)
  and every *other* tailnet host stay refused; a non-shared account is refused entirely.
- timeout / `000` → ACL deny, or the share isn't **Accepted** under the peer's exact
  identity (see §5a) — check the Machines page.

---

## 6. Verification gates (run in order)

| # | Gate | Pass |
|---|------|------|
| 0 | serve path rewrite | `…/events` (no token) → **401**, not 404 |
| 1 | host lockdown | LAN/bridge curl to `:8000`, `:7474`, `:7687` → refused |
| 2 | auth | `POST /events` no token → 401; `OPTIONS`/`HEAD /events` → 401 (no CORS/405 leak); with token → accepted |
| 3 | reboot persistence | after reboot, `tailscale serve status` still shows the `/events` mount and gate 1 still holds |
| 4 | ACL isolation | shared account reaches `:443` only; non-shared account refused (verify operationally from a shared account — the ACL Tests feature can't validate `autogroup:shared`) |
| 5 | end to end | a real Amplifier session via the client lands events in Neo4j (`/cypher` count increases) |

Onboard real peers only after gates 0–5 pass.

---

## 7. Back up the graph (and prove the restore)

The shared graph is the whole value; Neo4j Community ships no backup agent. A
minimal cold backup:

```bash
docker compose stop neo4j        # graceful flush; do NOT use `docker pause` (SIGSTOP can corrupt the WAL)
tar czf neo4j-$(date +%F).tgz -C <data-store-dir> neo4j
docker compose start neo4j
```

> **Important — restore must run as root inside a container.** Neo4j's data files
> are owned by the container's uid, so a host-side `rm`/overwrite fails with
> *Permission denied* and the restore silently does nothing. Do the destructive
> replace in a throwaway root container:
> ```bash
> docker compose stop neo4j
> docker run --rm -v <data-store-dir>:/data -v "$PWD/neo4j-YYYY-MM-DD.tgz":/backup.tgz:ro \
>   alpine sh -c 'rm -rf /data/neo4j && tar xzf /backup.tgz -C /data'
> docker compose start neo4j
> ```
> Test one restore before you trust it. Also monitor disk — ingested data grows
> unbounded and there is no ingress size/rate cap.

---

## 8. Trust model — decide consciously

This design relies on the peers being trusted. Known, accepted properties for a
small group:

| Property | Implication | Harden later by |
|----------|-------------|-----------------|
| Single shared API key | Any key holder can read all data via `/cypher`; revoking one peer = un-share the node; rotating the key = re-onboard everyone | Per-peer keys (server change) |
| Client sets the `workspace` field | A peer can post events tagged with any workspace, including yours; no server-side validation | Server-side workspace validation |
| No ingress rate/size cap | A key holder can fill disk | A reverse-proxy allowlist (e.g. Caddy) or server change |

For a handful of close collaborators these are usually acceptable. Make the call
deliberately rather than by default.

---

## See also
- README → "Network Access and Security" — `server_host` binding and the
  firewall-to-trusted-IPs option.
- [docs/service-setup.md](service-setup.md) §10 — self-hosted HTTPS with Caddy
  (an alternative/complement to `tailscale serve` for TLS).
- [docs/peer-onboarding.md](peer-onboarding.md) — the guide to give your peers.
- [docs/azure-deployment.md](azure-deployment.md) — the managed, public path.
