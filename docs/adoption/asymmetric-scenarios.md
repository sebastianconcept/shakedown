# Adoption — Asymmetric scenarios (real handler vs real producer)

**Purpose:** Describe common **one-sided** integration tests: you care about **one** real boundary (handler or producer) and only need the other side to be **correct enough on the wire**.

**Audience:** Engineers designing scenarios around Kafka-heavy services.

**When to read this:** After [Bring your own clients](bring-your-own-clients.md) or the [walkthrough hub](walkthrough-orders-payments.md).

---

## Glance

- A **topic name** only routes messages; **compatibility** is about bytes on the wire (encoding, schema, keys, headers).
- **Handler-focused test:** Use a minimal or harness-controlled **producer**; assert on **handler outcomes** (database, outbox, outbound calls to **bridges**).
- **Producer-focused test:** Use a **real producer**; verify with a **thin consumer**, Shakedown consume helpers, or **downstream** assertions (another topic, DB).
- **Service bridges** mock **external** APIs — orthogonal to “fake vs real” *inside* your Kafka pipeline.
- **Cross-language dependencies:** The real Payments (or identity, …) service might be **not Rust** in production; the lab stub still speaks **HTTP/gRPC to your Rust** client — see [Bring your own clients — outbound stubs](bring-your-own-clients.md#outbound-stubs-and-language-of-the-real-dependency).

---

## Wire compatibility (not just Rust types)

“Same topic” does **not** imply “same crate types.” What must align is what the **consumer actually deserializes**:

- Payload format (JSON, Avro, Protobuf, …) and schema/version if applicable.
- Key and header conventions if your handler depends on them.
- Ordering or partition assumptions if the test relies on them.

Two different Rust structs can match if they serialize compatibly; two identical structs can fail if one side uses a different wire format. **Integration tests prove behavior on the real encoding path**, not only type identity in Rust.

---

## Scenario A — Real handler, minimal producer

**Goal:** Prove the **real consumer / handler** (and DB effects, outbox, retries) under realistic infra.

**Typical setup:**

- The **ingress** can be Shakedown’s produce helpers, a small test-only publisher, or your production producer — whichever is cheapest while staying **wire-compatible** with what the handler expects.
- You often **do not** need to assert on producer business logic; you need messages that are **valid inputs** for this handler on the **configured topic** (and isolation prefix, if any).
- **Assertions** lean on **final state**: rows, outbox tables, emitted events, **bridge** call counts — whatever defines “correct” for the handler.

This matches the “transferring confidence” story: promotion and refactors hinge on **observable** handler behavior.

---

## Scenario B — Real producer, minimal consumer

**Goal:** Prove the **real producer** emits the right events (or triggers the right downstream effects).

**Typical setup:**

- Run the **real producer code path** against harness-managed Kafka.
- The **verification** side can be:
  - Shakedown **consume** / assert helpers, when formats line up;
  - a **minimal** consumer that only checks payloads and metadata;
  - assertions **outside** Kafka (e.g. consumer you already trust, or effects in a DB written by a downstream service) if that is the contract you care about.

You do **not** need the full domain consumer if a thinner check answers your question.

---

## How this fits Shakedown

- **Lifecycle and bindings** let you treat **either** side as “the system under test” and the other as **test-controlled**, as long as the wire contract holds.
- **Produce / consume / assert** helpers are optional ergonomics; **bindings-first** means your own clients participate in the same scenario.
- **Bridges** stay at **system boundaries** (payment API, identity, …), not between your test harness and your Kafka topic — unless you deliberately model an internal hop that way.
- Those boundaries are **language-agnostic**: you stub **protocol behavior**, not “a Rust clone of their service.”

---

## Refresher card

- **Topic:** routing label; **wire:** encoding + schema + headers.
- **Handler test:** minimal compatible ingress; assert **outcomes**.
- **Producer test:** real producer; **thin** verification path.
- **Related:** [Bring your own clients](bring-your-own-clients.md), [Glossary — TestContext](../glossary.md).
