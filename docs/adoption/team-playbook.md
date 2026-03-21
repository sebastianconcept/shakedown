# Adoption — Team playbook

**Purpose:** Agree how your **team** uses Shakedown so reviews and incidents stay coherent.

**Audience:** Tech lead + senior engineers; whole team reads the “Definition of green” section.

**When to read this:** Right after first local green ([Quickstart](quickstart.md)).

---

## Glance — what to decide once

- Where scenario files live and how they are named.
- What **green** proves for **transfer** vs **preserve** modes.
- Who owns flakes, Docker updates, and CI timeouts.

---

## Roles (lightweight)

| Role | Owns |
|------|------|
| **Harness owner** | Bumps Shakedown, resolves breaking changes, keeps CI recipe working. |
| **Scenario owner** | Domain correctness of a scenario group (often feature team). |
| **CI owner** | Runners, caches, artifacts, alerting on failed pipelines. |

One person can wear multiple hats in small teams — still write it down.

---

## Definition of green

**Transferring confidence (ship / change):**

- Required scenarios pass on the PR branch in CI.
- Optional: same scenarios runnable locally with one command (documented in README).

**Preserving confidence (post-incident):**

- A **regression scenario** exists that failed before the fix and passes after.
- The scenario is named and linked from the incident ticket or postmortem.
- It runs on default CI — not only on a laptop.

**Do not** mix the two without labeling: “green for merge” can differ from “green for incident closure” if you explicitly allow that.

---

## Conventions

| Topic | Decide |
|-------|--------|
| **Repo layout** | One pattern for migrations + integration tests — see [Recommended repository layout](walkthrough-orders-payments.md#recommended-repository-layout). |
| **Naming** | Prefix scenarios with area: `billing_outbox_emits_payment`. |
| **Secrets** | No secrets in repo; document how CI provides them if ever needed. |
| **Flakes** | Open an issue with **scenario name**, **frequency**, **host**; fix or quarantine with a time box. |
| **Timeouts** | Default per scenario class; escalate to harness owner if infrastructure is slow. |

---

## Reviews

- **Scenario PRs** should say what **observable** behavior changed.
- Prefer **small** scenario additions over giant JSON dumps unless debugging.
- **Fault tests** (`#[faulty_test]`) require a one-line **intent** in the PR (what failure mode).

---

## Refresher card

- **Transfer** = merge bar; **preserve** = regression after incident — name both.
- **Owners:** harness, scenarios, CI — even if one person.
- **Flakes** = tracked issues, not silent retries forever.
