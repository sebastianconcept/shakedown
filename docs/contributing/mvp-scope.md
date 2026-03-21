# Contributing — MVP scope (v0.1 card)

**Purpose:** Single-page **in / out** scope for the first shippable version. Read this before planning or debating features.

**When to read this:** Milestone planning, RFCs, and “can we add X?” questions.

**Status:** Aligns with product docs such as `MVP.md` / `DECISIONS.md` when those exist outside the repo — update this file when the team agrees a change.

---

## Glance — v0.1 promise

Deliver a Rust harness teams can adopt for **realistic** system tests: library + macros + CLI, first-party **Kafka**, **Postgres**, **MariaDB**, **Toxiproxy**, **outbox helpers**, basic **YAML** scenarios, and **crates.io** publish under MIT.

---

## In scope (v0.1)

| Area | Includes |
|------|-----------|
| **Crates** | `shakedown` (lib), procedural macros, `shakedown-cli` |
| **Macros** | `#[system_test]`, `#[faulty_test]` (names per crate) |
| **Resources** | Kafka, Postgres, MariaDB via testcontainers (Docker) |
| **Faults** | Toxiproxy integration owned by Shakedown |
| **Patterns** | Outbox testing helpers on Kafka + relational |
| **UX** | Typed produce / consume / assert helpers via `TestContext` |
| **Scenarios** | Basic YAML runner aligned with same engine as tests + CLI |
| **CI story** | cargo-nextest, JUnit-friendly output; cargo-chef Dockerfile |
| **CLI** | `run`, `list`, `--format json`, semantic exit codes |
| **Publish** | MIT on crates.io |

---

## Out of scope (v0.1)

| Item | Notes |
|------|--------|
| **MCP server** | Post–v0.1 optional; CLI + JSON covers agents |
| **Soak / load / perf** | Do not block types for future work; no perf product in v0.1 |
| **Redis, RabbitMQ, …** | Roadmap after v0.1 |
| **Full IDE toxics UX, schema registry depth** | Longer horizon |

---

## Done when (release checklist — high level)

- [ ] Public API documented and semver plan stated.
- [ ] Examples run with Docker on a clean machine.
- [ ] CI recipe documented ([CI recipe](../adoption/ci-recipe.md)).
- [ ] Licensing and headers MIT-compliant.

---

## Refresher card

- **In:** lib + macros + CLI + Kafka + PG + MariaDB + Toxiproxy + outbox + YAML + nextest/JUnit story.
- **Out:** MCP, perf harness, extra brokers/queues in v0.1.
- **Argue scope?** Re-read this page first.
