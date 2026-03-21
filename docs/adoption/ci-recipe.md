# Adoption — CI recipe

**Purpose:** Run the same scenarios in CI as on laptops, with artifacts teams and agents can consume.

**Audience:** Engineer owning GitHub Actions, GitLab CI, or similar.

**When to read this:** After [Quickstart](quickstart.md) is green locally.

**Prerequisites:** Docker-in-CI available; Rust toolchain pinned (same as local ideally).

---

## Glance — goals

- **Deterministic enough:** Same commit → same scenarios; flakes have an owner (see [Team playbook](team-playbook.md)).
- **Artifacts:** JUnit (or equivalent) for dashboards; **JSON** from CLI when you run scenarios outside `cargo test`.
- **Exit codes:** Semantic codes for automation (documented in crate / CLI `--help`).

---

## Steps

### 1. Pin Rust

Use `rust-toolchain.toml` or CI image with a **fixed** toolchain. Match [workspace MSRV](../contributing/mvp-scope.md) when applicable.

### 2. Cache dependencies

Use **cargo-chef**, `sccache`, or your org standard so integration tests are not dominated by cold compiles every run.

### 3. Enable Docker

Examples:

- **GitHub Actions:** `runs-on: ubuntu-latest` + `docker` available; consider larger runners if Kafka is heavy.
- **GitLab:** Docker executor with enough disk for images.

Document **resource limits** if OOM kills containers.

### 4. Prefer cargo-nextest

```bash
cargo nextest run --profile ci
```

Configure profiles in `.config/nextest.toml` (JUnit output, retries policy). **Retries:** use sparingly — only for known infra noise, not product bugs.

### 5. Wire JUnit

Point `nextest` (or `cargo test` JSON harness) at a JUnit path your CI uploads as an artifact. Name includes crate and scenario identifiers.

### 6. CLI + JSON (optional path)

When using `shakedown` CLI:

```text
shakedown run … --format json
```

Parse JSON in a thin wrapper for gates. Align **exit codes** with your promotion docs so “green” is unambiguous.

### 7. Secrets

Integration tests against **local containers** usually need no cloud secrets. If you add external sandboxes, use CI secret stores — never embed in YAML.

---

## Verify

- [ ] A clean CI run completes without manual steps.
- [ ] Artifacts attach to the workflow run (JUnit and/or JSON).
- [ ] Failed runs show **which scenario** failed in the report.

---

## If something fails

- **OOM / exit 137:** Increase runner memory or shrink parallel jobs.
- **Timeouts:** Separate **slow** scenarios into a nightly job if needed — document the split in the [Team playbook](team-playbook.md).
- **Docker not found:** Fix runner image or service flags.

See [Troubleshooting](troubleshooting.md) for container-specific issues.

---

## Refresher card

- **Pin** Rust; **cache** deps; **Docker** in CI; **nextest** + JUnit.
- **CLI:** `--format json` for automation; respect semantic exit codes.
- **Flakes:** policy in team playbook — retries are not a substitute for fixes.
