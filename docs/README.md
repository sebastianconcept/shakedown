# Shakedown documentation map

**Purpose:** Find the right doc in one jump. This repo holds **adoption** and **contributor** docs; high-level product specs may also live outside the repo (for example `~/docs/shakedown`).

**When to read this:** First visit, or when you forgot where something lives.

---

## I want to …

| Intent | Start here |
|--------|------------|
| Run my first scenario locally | [Adoption — Quickstart](adoption/quickstart.md) |
| Walkthrough hub (Orders + Payments) | [Overview](adoption/walkthrough-orders-payments.md) |
| Lay out **integration tests** and **migrations** in the repo | [Walkthrough hub — repository layout](adoption/walkthrough-orders-payments.md#recommended-repository-layout) |
| Example: assert on **database** only | [Orders + Payments — database](adoption/walkthrough-orders-payments-database.md) |
| Example: assert on **Kafka** (producer) | [Orders + Payments — Kafka](adoption/walkthrough-orders-payments-kafka.md) |
| Example: **consumer** writes **DB** after Kafka | [Orders + Payments — Kafka + DB](adoption/walkthrough-orders-payments-kafka-consumer-database.md) |
| Run scenarios in CI (nextest, JUnit, JSON) | [Adoption — CI recipe](adoption/ci-recipe.md) |
| Agree as a team on ownership, flakes, and “green” | [Adoption — Team playbook](adoption/team-playbook.md) |
| Fix a failure (containers, Kafka, DB, proxy) | [Adoption — Troubleshooting](adoption/troubleshooting.md) |
| Understand terms (`TestContext`, scenario, wire stub, …) | [Glossary](glossary.md) |
| Use **proprietary** Kafka/DB libs with the harness | [Adoption — Bring your own clients](adoption/bring-your-own-clients.md) |
| **Handler-only** vs **producer-only** realism (minimal other side) | [Adoption — Asymmetric scenarios](adoption/asymmetric-scenarios.md) |
| Stub an **HTTP/gRPC** dependency whose **production** service is **another language** | [Adoption — Bring your own clients — outbound stubs](adoption/bring-your-own-clients.md#outbound-stubs-and-language-of-the-real-dependency) |
| See adoption narrative and prerequisites | [Adoption — Overview](adoption/README.md) |
| Know what v0.1 includes and excludes | [Contributing — MVP scope](contributing/mvp-scope.md) |
| Author a milestone / feature doc | [Contributing — Milestone template](contributing/milestone-template.md) |

---

## Two modes of confidence (spine)

Everything here should align with how teams actually decide:

1. **Transferring confidence** — Can we accept this change? (promotion, review, bold refactors.)
2. **Preserving confidence** — After an incident, can we show it will not happen again? (reproduce → fix → regression guard.)

Same harness; different questions. See [Adoption — Team playbook](adoption/team-playbook.md) for how “green” maps to each.

---

## Doc layers (how to read)

| Layer | Use when |
|-------|----------|
| **Glance** | Top of each page: outcome + 5 bullets max. |
| **Do** | Numbered steps, copy-paste blocks. |
| **Understand** | “Why” and trade-offs — optional for completing a task. |

**Refresher rule:** Scroll to the **Refresher card** at the bottom of major pages.

---

## Structure

```
docs/
├── README.md                 ← you are here
├── glossary.md
├── adoption/
│   ├── README.md
│   ├── quickstart.md
│   ├── walkthrough-orders-payments.md
│   ├── walkthrough-orders-payments-database.md
│   ├── walkthrough-orders-payments-kafka.md
│   ├── walkthrough-orders-payments-kafka-consumer-database.md
│   ├── bring-your-own-clients.md
│   ├── asymmetric-scenarios.md
│   ├── ci-recipe.md
│   ├── team-playbook.md
│   └── troubleshooting.md
└── contributing/
    ├── README.md
    ├── mvp-scope.md
    └── milestone-template.md
```
