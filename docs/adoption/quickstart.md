# Adoption — Quickstart

**Purpose:** Shortest path to a **first green scenario** on a machine with Docker.

**Audience:** Backend engineer integrating Shakedown into an existing service repo.

**When to read this:** Day one. Re-read the Refresher card after you have something running.

**Prerequisites:** Rust workspace, Docker running, crate `shakedown` available (path/git/crates.io per your project). **Where to put tests and migrations:** [Walkthrough hub — repository layout](walkthrough-orders-payments.md#recommended-repository-layout).

---

## Glance — what you will have

- A test that Shakedown drives end-to-end against **real** Kafka (and optionally DB) in containers.
- One place to run it: `cargo test` (or `cargo nextest run` once configured).
- A baseline you can extend with faults, YAML scenarios, and CI.

---

## Steps

### 1. Add dependencies

In your **integration test crate** (or main crate’s `tests/`), add the Shakedown crates your version ships (`shakedown`, `shakedown-macros`, and any resource features). Follow the released README for exact feature flags.

### 2. Choose one vertical slice

Pick the **smallest** behavior that proves wiring:

- Produce a message → consume → assert side effect (or DB row), **or**
- Inbound HTTP → outbox → Kafka message visible.

Avoid testing the whole system on day one. For a **narrated** Orders + Payments example, open the [walkthrough hub](walkthrough-orders-payments.md) and pick **database**, **Kafka**, or **Kafka + consumer DB** — whichever matches how you assert.

### 3. Wire `#[system_test]` (or equivalent)

Annotate one async test. Ensure the macro and `TestContext` match the version you use. If the API differs pre-1.0, follow the crate docs — this page stays **pattern-stable**, not API-stable.

### 4. Declare resources

Enable **Kafka** (and **Postgres** / **MariaDB** if your slice needs them). Let the harness provision containers and expose connection strings via `TestContext`.

### 5. Write one assertion that matters

State **one** observable outcome: offset advanced, record in DB, metric stub called — whatever your slice proves.

### 6. Run locally

```bash
cargo test --test <your_integration_test> -- --nocapture
```

Prefer `cargo nextest run` once [CI recipe](ci-recipe.md) is in place.

### 7. Commit the scenario

Commit **only** what is needed: test code, any small fixture SQL, and config. No secrets in repo — use env or CI secrets for real keys (usually not needed for local containers).

---

## Verify

- [ ] Test passes twice in a row locally (catches obvious flakes).
- [ ] Teardown leaves no orphaned containers (`docker ps` check).
- [ ] Another engineer can run it with only Docker + Rust — document gaps in the [Team playbook](team-playbook.md).

---

## If something fails

Go to [Troubleshooting](troubleshooting.md). Most early issues are Docker resources, ports, or pulling images.

---

## Understand (optional)

**Why containers:** Real protocol behavior beats mocks for Kafka, TCP, and timing. **Why one slice:** Proves the harness before you invest in full scenarios. **Why macros:** Keeps test code small and consistent with CLI/YAML entrypoints.

---

## Refresher card

- **Order:** deps → one slice → `#[system_test]` → resources → one assertion → `cargo test`.
- **Verify:** passes twice, clean `docker ps`, teammate can run.
- **Stuck:** [Troubleshooting](troubleshooting.md).
