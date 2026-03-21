# Adoption — Bring your own Kafka and DB clients

**Purpose:** Explain how Shakedown fits teams that already have **proprietary or bespoke** libraries for producers, consumers, outbox processors, and database access.

**Audience:** Engineers evaluating the harness when “our stack” is not plain `rdkafka` + `sqlx`.

**When to read this:** After [Adoption — Overview](README.md), alongside [Asymmetric scenarios](asymmetric-scenarios.md).

---

## Glance

- Shakedown’s value is **lifecycle, isolation, and evidence** (containers, readiness, scenario-scoped topics/DBs, teardown, CI-friendly runs).
- Your domain code stays yours: wire it with **`TestContext` bindings** (brokers, URLs, topic prefixes, bridge URLs) — the same idea as pointing staging at a cluster, but **local and isolated per scenario**.
- **Traits** in the design are mainly for **harness** pieces (resources, bridges), not for “every consumer must implement Shakedown.”
- **Outbound wire stubs** replace **HTTP/gRPC** dependencies at the **protocol** boundary; the **real** service in production may be Python, Go, Node, or anything else — the stub does not need to match that implementation language (see [below](#outbound-stubs-and-language-of-the-real-dependency)).
- If your library can be **configured** for test brokers and topics, you can use Shakedown; if it **hardcodes** production endpoints, fix that first — that is a configurability problem, not a harness limitation.

---

## How wiring works

**Bindings-first:** The harness exposes structured connection information — bootstrap servers, JDBC/connection URLs, scenario-scoped topic prefixes or database names, stub base URLs for outbound HTTP/gRPC, and so on. From there:

- **In-process tests:** Build your library’s clients from those bindings inside the test module, then run the same code paths you run in production.
- **Subprocess tests:** Inject env vars or config files so your binary starts with the harness-managed endpoints.

Optional Shakedown **typed helpers** (produce, consume, assert) are for convenience when you enable the matching client features. They are **not** required for correctness: you may use **your** Kafka and DB stack as long as it uses the **same bindings** so isolation stays consistent.

---

## What “traits” are for (and what they are not)

| Layer | Role |
|-------|------|
| **Resource providers** | How the harness brings up Kafka, Postgres, MariaDB, Toxiproxy, etc. |
| **Service bridges** | Stub or replay **external** HTTP/gRPC dependencies at the network boundary. |
| **Your application** | No requirement to implement Shakedown traits for **being** a producer or consumer. |

The contract is **configuration**: your handlers see the same bootstrap and topic layout the scenario declared.

---

## Outbound stubs and language of the real dependency

In production, an upstream may be **another team’s microservice** written in **Python, Go, Node**, or any language. In a Shakedown scenario you still **stub it at the network boundary**: your **Rust** service under test uses its **real** HTTP or gRPC client against a **local** [wire stub](../glossary.md) whose answers are **good enough for the scenario**.

What must match is the **contract your client actually uses**: base URL (or proxied host), paths or RPC names, status codes, and **payload shape** on the wire (JSON, protobuf, …). What **does not** matter is the runtime or language of the real dependency — the stub is not a port of their service, only a **protocol-level** stand-in.

**When tests need more:** If the real service has **unusual** behavior (custom auth flows, streaming quirks, idiosyncratic errors), the stub must **model those behaviors** for the test to be meaningful — that is still a **contract** problem, not a “same language as prod” requirement.

---

## When it is still awkward

- **Non-configurable clients** (fixed broker list, no test profile) block local scenarios until you add a seam.
- **Exotic contracts** (e.g. deep exactly-once proofs) may need you to keep assertions in **your** types while Shakedown still provides **infra + lifecycle**.
- **Wire format mismatches** are not fixed by “same topic name” — see [Asymmetric scenarios — wire compatibility](asymmetric-scenarios.md#wire-compatibility-not-just-rust-types).

---

## Refresher card

- **Bindings:** Use harness-provided endpoints and scoped names; build **your** clients from them.
- **Traits:** Harness extensibility; not a mandate for domain Kafka code.
- **Outbound stub:** Protocol-level; **language-agnostic** vs the real dependency’s implementation.
- **Next:** [Asymmetric scenarios](asymmetric-scenarios.md) for “real handler only” vs “real producer only” tests.
