# Adoption — Overview

**Purpose:** Decide whether Shakedown fits, what to budget, and how the adoption ladder works.

**Audience:** Tech lead, architect, or senior backend engineer evaluating or rolling out the harness.

**When to read this:** Before the [Quickstart](quickstart.md). Skip to Quickstart if someone already chose Shakedown.

---

## Problem → implication → outcome (SPIN, short)

**Situation:** You ship event-driven services with Kafka and databases; unit tests miss **wiring**, **ordering**, **faults**, and **outbox** behavior.

**Problem:** Hand-rolled integration tests accrete bash, Compose, and sleeps. Reviews cannot replay “what good means.” After incidents, “we fixed it” lacks a **repeatable** proof.

**Implication:** Slower promotion, opaque refactors, and regressions that tests should have caught.

**Need-payoff:** One harness that runs **the same** scenarios locally and in CI, with room to grow from **transfer** (ship change) to **preserve** (post-incident regression).

---

## What Shakedown is (one paragraph)

Shakedown is a Rust-first **system test** harness: real containers where it matters, procedural macros for test entrypoints, optional YAML scenarios, Toxiproxy for faults, and a CLI with machine-readable output. You still own domain logic — Shakedown owns **lifecycle**, **isolation**, and **evidence**.

---

## Prerequisites (honest)

| Requirement | Notes |
|-------------|--------|
| Docker (or compatible runtime) | Kafka, databases, Toxiproxy run as containers. |
| Rust toolchain | For the **test crate** and macros; your service may be started as a subprocess if needed. |
| CI with Docker | Parity with laptops; see [CI recipe](ci-recipe.md). |
| Time | Budget roughly **half a day to a day** for first wiring and first green scenario on a new codebase; less on the next service. |

---

## Adoption ladder (read in order)

1. **Overview** — This page.
2. **[Quickstart](quickstart.md)** — Smallest path to first green scenario.
3. **[Walkthrough — Orders + Payments](walkthrough-orders-payments.md)** — Hub with **three** focused paths: [database](walkthrough-orders-payments-database.md), [Kafka producer](walkthrough-orders-payments-kafka.md), [Kafka consumer + DB](walkthrough-orders-payments-kafka-consumer-database.md) (Orders under test, Payments **HTTP stub** — [Glossary](glossary.md)).
4. **[Team playbook](team-playbook.md)** — Who owns what, definition of green, flakes.
5. **[CI recipe](ci-recipe.md)** — nextest, JUnit, JSON, exit codes.
6. **[Troubleshooting](troubleshooting.md)** — Symptom → fix.

Optional **concept** reads (fit / scenario design):

- **[Bring your own clients](bring-your-own-clients.md)** — Proprietary Kafka/outbox/DB stacks; `TestContext` bindings vs harness traits.
- **[Asymmetric scenarios](asymmetric-scenarios.md)** — Real handler + minimal producer, or real producer + thin verification; wire compatibility.

Deeper **stack-specific** examples (Kafka + MariaDB + outbound stubs) may live in external product docs; point your team at those when the API stabilizes.

---

## Refresher card

- **Adopt in order:** Quickstart → (optional [walkthrough](walkthrough-orders-payments.md)) → team rules → CI → troubleshooting.
- **Two modes:** Transfer (ship) vs preserve (incident) — both use scenarios; different questions.
- **Prereqs:** Docker + Rust test crate + CI Docker.
