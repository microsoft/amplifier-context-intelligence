# Connecting to a Shared Context-Intelligence Server

For people who've been **invited** to send their Amplifier sessions to someone
else's context-intelligence server. The server owner will give you a URL and an API
key; this guide gets you connected in a few minutes.

> If you are the owner setting up sharing, see
> [docs/remote-access-sharing.md](remote-access-sharing.md) instead.

---

## 1. What you're agreeing to

Once connected, your Amplifier **session context-intelligence** — session and event
metadata and tool-call traces, which can include prompts, file paths, and tool
arguments — is uploaded to a **shared graph that the server owner can query**. You
contribute data; you don't query the shared graph yourself.

Traffic is end-to-end encrypted (over the owner's overlay network plus HTTPS) and
reaches only the upload endpoint. **You can opt out at any time** by removing the two
settings below or stopping the bundle. Data already uploaded remains in the graph
unless the owner deletes it.

If that's acceptable, continue.

---

## 2. Join the owner's network

This example uses [Tailscale](https://tailscale.com); your owner will tell you if
they use something else.

1. Create a free account and install Tailscale: https://tailscale.com/download
2. Sign in. The owner sends you a **device share invitation** for one machine.
   **Accept it** (via the email link, or under *Machines* in your Tailscale admin
   console). You do not join their network — you just get access to that one machine.

> **Accept while signed in to the exact account the owner invited.** Tailscale
> matches your identity as an exact string — and **Gmail dots count** (`a.b@gmail.com`
> ≠ `ab@gmail.com`). If your login differs from the address they invited, tell them
> so they can re-share to the right one. Also: reach the server by the **URL the
> owner gives you** (its MagicDNS name), never a raw `100.x` IP — a shared machine
> has a different IP in your tailnet.

---

## 3. Get your credentials from the owner

Over a secure channel, the owner gives you two values:

| Value | Example |
|-------|---------|
| Server URL | `https://<their-node>.<their-tailnet>.ts.net` |
| API key | a long random token |

---

## 4. Configure Amplifier

Put both values in `~/.amplifier/keys.env`, then lock the file's permissions:

```bash
cat >> ~/.amplifier/keys.env <<'EOF'
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL=https://<their-node>.<their-tailnet>.ts.net
AMPLIFIER_CONTEXT_INTELLIGENCE_API_KEY=<paste-the-key-from-the-owner>
EOF
chmod 600 ~/.amplifier/keys.env
```

Add the client bundle to your Amplifier app:

```bash
amplifier bundle add git+https://github.com/microsoft/amplifier-bundle-context-intelligence@main --app
```

---

## 5. Verify it works (do this yourself)

**A. Network reachable** — before adding your key, confirm the tunnel works:

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://<their-node>.<their-tailnet>.ts.net/events
```

| Result | Meaning |
|--------|---------|
| `401`  | Reachable — the server is there and (correctly) wants a token. You're good. |
| `000` / hang | Tailscale isn't connected, or you haven't accepted the share. |
| `404`  | Wrong URL — re-check it exactly. |

**B. Your key is accepted:**

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST https://<their-node>.<their-tailnet>.ts.net/events \
  -H "authorization: Bearer $(grep API_KEY ~/.amplifier/keys.env | cut -d= -f2)" \
  -H 'content-type: application/json' -d '{}'
```

- `422` — authenticated (the empty test body is rejected, which is expected).
- `401` — key is wrong; re-copy it from the owner.

**C. Real run:** start a normal Amplifier session. Your events upload automatically
in the background. Ask the owner to confirm your workspace appears.

---

## 6. Troubleshooting

| Symptom | Fix |
|---------|-----|
| `000` / timeout on step 5A | Open Tailscale; confirm you're connected and accepted the device share |
| `404` | Server URL typo — must match exactly what the owner gave you |
| `401` in step 5B | Wrong or stale API key — request it again |
| Events don't appear | Confirm the bundle is added (`--app`) and `~/.amplifier/keys.env` is loaded |

To stop sharing at any time, remove the two lines from `~/.amplifier/keys.env`.
