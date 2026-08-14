# Connecting to a Shared Context-Intelligence Server

For people who've been **invited** to send their Amplifier sessions to someone
else's context-intelligence server. The server owner will give you a URL and a
**bearer token that is unique to you** (not a key shared with other peers); this
guide gets you connected in a few minutes.

> If you are the owner setting up sharing, see
> [docs/remote-access-sharing.md](remote-access-sharing.md) instead.

---

## 1. What you're agreeing to

Once connected, your Amplifier **session context-intelligence** — session and event
metadata and tool-call traces, which can include prompts, file paths, and tool
arguments — is uploaded to a **shared graph that the server owner can query**. You
contribute data; you don't query the shared graph yourself.

> **Contribute-only — you upload, you don't query.** Your sessions upload, and the
> *owner* queries them. That one-way property comes from the owner's **network
> configuration** — they route only the `/events` path, so the query endpoints aren't
> reachable — and **not** from your token. The server has no write-only credential:
> at the application layer any data token that can `POST /events` is also read-capable.
> The path allowlist is what makes this one-way, so verify it yourself in **step 5C**
> rather than assuming it. If a query *does* succeed, you are reading the owner's
> sessions too — tell them; that is almost certainly not what either of you intended.

Traffic is end-to-end encrypted (over the owner's overlay network plus HTTPS), and if
the owner set sharing up as documented it reaches only the upload endpoint — step 5C
confirms that. **You can opt out at any time** by removing the
destination below (or stopping the bundle). Data already uploaded remains in the
graph unless the owner deletes it.

You also choose **which** of your sessions are shared — the `include`/`exclude`
patterns in step 4 let you contribute everything or just a scoped subset (e.g. only
work sessions, never personal ones). By default this guide contributes everything.

If that's acceptable, continue.

---

## 2. Join the owner's network

This example uses [Tailscale](https://tailscale.com); your owner will tell you if
they use something else.

1. Create a free account and install Tailscale: https://tailscale.com/download
2. Sign in. The owner sends you a **device share invitation** for one machine.
   **Accept it** (via the email link, or under *Machines* in your Tailscale admin
   console). You do not join their network — you just get access to that one machine.
3. **Confirm the share is live before going further.** The shared machine should now
   appear in your Tailscale *Machines* list. Until it does, you have no network path to
   the server and the connection test in step 5 returns `000` (not `401`) — that's the
   share not being active yet, **not** a server problem. Give it a moment after
   accepting, and re-open the invite if the machine doesn't show up.

---

## 3. Get your credentials from the owner

Over a secure channel, the owner gives you two values:

| Value | Example |
|-------|---------|
| Server URL | `https://<their-node>.<their-tailnet>.ts.net` |
| API token | a long random token, **unique to you** |

This is a **raw token** — you send it verbatim as the bearer credential. The
server keeps only its SHA-256 digest and revokes it by removing that digest, so
the owner can cut off your access (or rotate your token) without disturbing other
peers. Treat the token as a secret; if it leaks, ask the owner to rotate it.

---

## 4. Configure Amplifier

The client hook sends your sessions to one or more named **destinations**. You'll add
the owner's server as a destination — keeping the secret token in `keys.env` and
referencing it from `settings.yaml`, so the secret never lives in your config file.

**a. Put your token in `~/.amplifier/keys.env`**, then lock the file's permissions:

```bash
cat >> ~/.amplifier/keys.env <<'EOF'
CI_TEAM_KEY=<paste-the-token-from-the-owner>
EOF
chmod 600 ~/.amplifier/keys.env
```

**b. Add the destination in `~/.amplifier/settings.yaml`:**

```yaml
overrides:
  hook-context-intelligence:
    config:
      destinations:
        team:
          url: "https://<their-node>.<their-tailnet>.ts.net"
          api_key: "${CI_TEAM_KEY}"     # expanded from keys.env — secret stays out of this file
          include: ["**"]               # contribute ALL your sessions
          # Prefer to share only SOME sessions? Scope by working directory instead:
          #   include: ["**/work/**"]       # only sessions under a "work" directory
          #   exclude: ["**/secret/**"]     # ...minus anything under "secret"
          # IMPORTANT: a destination with no `include` sends NOTHING — you must opt in.
```

Each session's events are sent to **every** destination whose `include` matches the
session's working directory and isn't caught by an `exclude`. With `include: ["**"]`
that's all of them; narrow it to contribute selectively. Sessions that match no
destination stay local only. You can also add more destinations here later (e.g. your
own personal server) — they fan out independently.

**c. Add the client bundle to your Amplifier app:**

```bash
amplifier bundle add git+https://github.com/microsoft/amplifier-bundle-context-intelligence@main --app
```

> **Simplest alternative (contribute everything, no YAML):** instead of the
> `destinations` block you can set two env vars in `~/.amplifier/keys.env` —
> `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL=<url>` and
> `AMPLIFIER_CONTEXT_INTELLIGENCE_API_KEY=<token>` — which sends *all* your sessions
> to that one server. The `destinations` form above is preferred because it lets you
> choose what to share and add more servers later.

---

## 5. Verify it works (do this yourself)

**A. Network reachable** — before adding your key, confirm the tunnel works:

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://<their-node>.<their-tailnet>.ts.net/events
```

| Result | Meaning |
|--------|---------|
| `401`  | Reachable — the server is there and (correctly) wants a token. You're good. |
| `000` / hang | The Tailscale share isn't active yet (the most common first-run snag) — confirm the shared machine appears in your *Machines* list and Tailscale is connected. **Not** a server problem. |
| `404`  | Wrong URL — re-check it exactly. |

**B. Your key is accepted:**

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST https://<their-node>.<their-tailnet>.ts.net/events \
  -H "authorization: Bearer $(grep CI_TEAM_KEY ~/.amplifier/keys.env | cut -d= -f2)" \
  -H 'content-type: application/json' -d '{}'
```

- `422` — authenticated (the empty test body is rejected, which is expected).
- `401` — key is wrong; re-copy it from the owner.

**C. The query endpoints really are closed** — this is the check that proves the
connection is contribute-only. Run it with your own key:

```bash
BASE=https://<their-node>.<their-tailnet>.ts.net
KEY=$(grep CI_TEAM_KEY ~/.amplifier/keys.env | cut -d= -f2)

curl -s -o /dev/null -w 'cypher %{http_code}\n' -X POST "$BASE/cypher" \
  -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
  -d '{"query":"MATCH (s:Session) RETURN count(s)","workspace":"*"}'
curl -s -o /dev/null -w 'blobs  %{http_code}\n' "$BASE/blobs/any" -H "authorization: Bearer $KEY"
curl -s -o /dev/null -w 'status %{http_code}\n' "$BASE/status"
```

- **`404` on all three** — correct. The owner routed only `/events`; nothing else is
  reachable, so the connection really is contribute-only.
- **Anything other than `404`** (`200`, `500`, `401`…) — that path *is* reachable, which
  means the allowlist is not in place. **Stop and tell the owner**: read endpoints are
  exposed to peers, so your token can read *their* entire graph across all workspaces.
  That is a misconfiguration on their side (see
  [remote-access-sharing.md](remote-access-sharing.md) §4), not something you can fix.

**D. Real run:** start a normal Amplifier session from a directory your `include`
matches. Your events upload automatically in the background. Ask the owner to confirm
your workspace appears.

---

## 6. Troubleshooting

| Symptom | Fix |
|---------|-----|
| `000` / timeout on step 5A | Open Tailscale; confirm you're connected and accepted the device share |
| `404` in step 5A or 5B | Server URL typo — must match exactly what the owner gave you (in step 5C, `404` is the *expected* result) |
| `401` in step 5B | Wrong or stale API key — request it again |
| Anything but `404` in step 5C | Owner-side misconfiguration — the path allowlist isn't in place and your token can read their whole graph. Report it; don't work around it |
| Events don't appear | Confirm the bundle is added (`--app`), that `~/.amplifier/keys.env` is loaded, and that your `include` actually matches the directory you ran from (no `include` ⇒ nothing is sent) |

To stop sharing at any time, remove the `team` destination from
`~/.amplifier/settings.yaml` (or delete `CI_TEAM_KEY` from `~/.amplifier/keys.env`).
