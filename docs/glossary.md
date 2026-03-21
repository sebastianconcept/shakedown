# Glossary

**Purpose:** Shared vocabulary for docs, issues, and reviews. Link here instead of redefining terms on every page.

**When to read this:** Onboarding, writing ADRs, or arguing about what “green” means.

---

## Core concepts

| Term | Meaning |
|------|---------|
| **Shakedown** | The harness (library + macros + CLI) that runs **scenarios** against **real** infra (containers) with optional **fault injection** and typed helpers. |
| **Scenario** | A scripted flow: provision resources → seed → drive the system → assert → tear down. Runnable from tests, CLI, or YAML runner (as shipped). |
| **TestContext** | Typed handle the harness injects: brokers, DB URLs, produce/consume helpers, clocks, etc. (exact API follows the crate.) |
| **Bindings** | Structured connection info from the harness (bootstrap URLs, scenario-scoped topic prefixes, DB URLs, bridge base URLs). Teams may construct **their own** clients from bindings; see [Bring your own clients](adoption/bring-your-own-clients.md). |
| **Asymmetric scenario** | A test that exercises **one** real boundary (e.g. handler **or** producer) while the other side is minimal but **wire-compatible**; see [Asymmetric scenarios](adoption/asymmetric-scenarios.md). |
| **Resource** | A managed dependency: e.g. Kafka, Postgres, MariaDB, Toxiproxy — started by the harness, isolated per scenario where designed. |
| **Wire stub** | A **minimal process** that listens on the network (HTTP, gRPC, …) and returns **deterministic, scripted** responses so your **real** client code runs against a **real** socket — not an in-process fake. Same idea as “outbound stub” or “HTTP stub” when the protocol is HTTP. The **production** dependency may be implemented in **any language**; the stub only needs to satisfy the **protocol and wire shape** your client uses — see [Bring your own clients — outbound stubs](adoption/bring-your-own-clients.md#outbound-stubs-and-language-of-the-real-dependency). |
| **Fault injection** | Controlled degradation (e.g. via Toxiproxy): latency, partitions, timeouts — used in `#[faulty_test]`-style flows. |
| **Outbox** | Pattern under test: relational write + message publish with consistency guarantees; helpers assert end-to-end paths. |

---

## Terminology: wire stub vs other words

**Why not “bridge”?** In software, *bridge* usually means *connecting two systems* (message bridge, API gateway, DB bridge). It does **not** imply “thin deterministic responder that your service treats like the real dependency.” That ambiguity is why this doc set prefers **wire stub** (or **HTTP stub**).

| Term | Precision | Notes |
|------|-----------|--------|
| **Wire stub** / **HTTP stub** | **Preferred** | Matches classic test doubles: **stub** = canned answers. **Wire** = at the network boundary, not a mocked trait in Rust. |
| **Outbound stub** | Good | Stresses “dependency we call,” not inbound traffic. |
| **Fake** | Mixed | Often means a **working** simplified implementation (e.g. in-memory repo). Can be heavier than a stub. |
| **Mock** | Mixed | Often implies **verifying** calls, not just returning data — easy to confuse with stub. |
| **Test double** | Umbrella | Correct but vague unless qualified. |
| **Simulator** | Sometimes | Implies more behavior than a first-day stub. |

**Crate API names** should align with **stub** / **wire** vocabulary where possible (e.g. feature `stub-http`, `ctx.stub_http()`). If you encounter older `bridge_*` identifiers in legacy code, treat them as the same concept — **wire stub** in prose.

---

## Modes of confidence

| Term | Meaning |
|------|---------|
| **Transferring confidence** | Evidence for **accepting a change** — scenarios reviewers can replay; CI matches local. |
| **Preserving confidence** | Evidence after **failure** — reproduce the incident, land a fix, keep a regression scenario. |

---

## Operations

| Term | Meaning |
|------|---------|
| **Green** | All assertions for the scenario passed; exit code success for CLI in CI. Define **what green proves** in your team playbook. |
| **Flake** | Intermittent failure not caused by product logic — often timing, Docker, or shared state. Owned and triaged per [Team playbook](adoption/team-playbook.md). |
| **Artifact** | What you keep for audit: logs, JUnit, JSON report, scenario name, git SHA. |

---

## Refresher card

- **Scenario** = full lifecycle test with real resources.
- **Wire stub** = deterministic network responder; real client, scripted far side — not an in-process mock.
- **Transfer** = ship / change confidence; **preserve** = incident / regression confidence.
