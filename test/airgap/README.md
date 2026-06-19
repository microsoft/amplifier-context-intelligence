# Air-gapped APOC validation

Reproducible checks that the Neo4j **APOC** plugin loads and runs when the Neo4j
container is **cut off from the internet** — i.e. it must be provisioned locally,
never downloaded. These exercise the air-tight path defined by
[`../../neo4j.Dockerfile`](../../neo4j.Dockerfile) and
[`../../docker-compose.airgap.yml`](../../docker-compose.airgap.yml).

Run these whenever the Neo4j base image version changes, to confirm APOC still
loads offline before shipping.

## Two checks — pick by how much isolation you need

| | `verify-airgap-apoc.sh` | `dtu-profile.yaml` |
|---|---|---|
| Needs | Docker only | `amplifier-digital-twin` (Incus) |
| Isolation | Docker `--internal` network (no egress) | Fresh Incus container **+** Docker `--internal` network |
| Speed | ~1 min | several minutes (installs Docker in the twin) |
| Use for | CI / quick local regression | deeper, host-isolated proof |

Both build the air-tight image (APOC jar baked in at build time), run it on a
Docker network with **no NAT / no internet route**, prove egress is blocked, and
assert APOC is functional.

## 1. Docker-only (primary)

```bash
test/airgap/verify-airgap-apoc.sh
```

Exit code `0` = PASS. Env overrides: `NEO4J_VERSION` (default `2026.05.0`),
`IMAGE_TAG`, `KEEP=1` (leave container/network/image up for inspection).

What it asserts:
- the APOC jar is **baked into the image layer** (present before any run);
- the run network is **internal** and outbound to `8.8.8.8:53` is **blocked**;
- `apoc.version()` matches the Neo4j version, `apoc.meta.stats()` runs;
- built-in `db.labels()` still works (proves we did **not** over-restrict with an
  `allowlist`);
- **no** plugin install/download lines appear in the startup logs.

## 2. Digital Twin Universe (deeper isolation)

```bash
amplifier-digital-twin up test/airgap/dtu-profile.yaml
# ... validation runs as part of provisioning ...
amplifier-digital-twin destroy neo4j-apoc-airgap-validate
```

Provisioning **is** the validation: the profile builds the air-tight image inside
a fresh Incus container, runs it on an internal Docker network, and fails the
launch if APOC does not load offline. Paths in the profile resolve relative to
the profile file, so it can be launched from anywhere in the repo.

## Note on a truly disconnected host

These checks use build-time internet only to pull the base `neo4j` image and (for
the DTU path) install Docker. On a **fully** disconnected host you must pre-load
the base image too — `docker save neo4j:2026.05.0-community -o neo4j.tar` on a
connected machine, then `docker load -i neo4j.tar` on the air-gapped host (or use
an internal registry mirror). The base image carries the bundled APOC Core jar
the build copies, so everything else then runs with zero internet access.
