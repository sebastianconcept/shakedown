# Adoption — Troubleshooting

**Purpose:** Symptom-first fixes for common Shakedown runs. Use as a **refresher** when something breaks after months of calm.

**Audience:** Any engineer running scenarios locally or reading CI logs.

**When to read this:** When red, not when green.

---

## How to use this page

1. Find your **symptom** below.
2. Apply the **first checks** — they fix most issues.
3. If still broken, collect **logs**: scenario name, `docker ps`, Shakedown version, host OS.

---

## Docker / runtime

| Symptom | First checks |
|---------|----------------|
| Cannot connect to Docker | Is Docker Desktop / daemon running? `docker info` |
| Pull failures | Registry auth, rate limits, corporate proxy |
| No space left on device | `docker system prune` (careful in shared envs); expand disk |
| Port already in use | Another test or service; change port mapping or stop conflicting process |

---

## Kafka

| Symptom | First checks |
|---------|----------------|
| Connection refused | Broker not ready — harness should wait; check readiness timeout |
| Unknown topic | Topic creation / isolation — confirm scenario seed step |
| Consumer lag / no messages | Producer acks, wrong topic name, serialization mismatch |

---

## Postgres / MariaDB

| Symptom | First checks |
|---------|----------------|
| Auth failed | User/password from harness vs migration |
| Schema missing | Migrations ran in scenario setup? |
| Deadlock / timeout | Ordering of operations; avoid shared DB across parallel scenarios without isolation |

---

## Toxiproxy / faults

| Symptom | First checks |
|---------|----------------|
| All traffic dropped | Toxic still active — ensure teardown between tests |
| Flaky only under fault | May be real — assert ordering and timeouts explicitly |

---

## CI-specific

| Symptom | First checks |
|---------|----------------|
| Passes locally, fails in CI | Resource limits; time; parallelism; different env vars |
| Exit 137 | OOM — reduce parallelism or increase runner size |
| Intermittent | See [Team playbook](team-playbook.md) flake policy |

---

## Refresher card

- **Order:** Docker OK → images pulled → ports free → logs for scenario name + version.
- **Still stuck:** Open issue with **repro command**, **OS**, **Shakedown version**, **CI vs local**.
