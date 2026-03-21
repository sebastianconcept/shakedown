# Milestone — `<slug>`

**Purpose:** One milestone, one shippable increment. Copy this file per milestone (for example `mvp-kafka-vertical.md`) and fill placeholders.

**Prerequisites:** Read [MVP scope](mvp-scope.md) and linked product docs.

---

## Glance

| Field | Value |
|-------|--------|
| **Slug** | `<slug>` |
| **Goal (one sentence)** | |
| **Depends on** | |
| **Status** | Planned / In progress / Done |

---

## Done when (exit criteria)

- [ ] Criterion 1 — observable, testable
- [ ] Criterion 2
- [ ] Docs or changelog updated if behavior is user-visible
- [ ] Quality checks pass (project-defined: `cargo fmt`, `clippy`, tests, …)

---

## Out of scope (this milestone)

- Bullet what you are **not** doing so reviewers do not expand the PR implicitly.

---

## Implementation notes

- Design links: ADRs, issues, diagrams.
- Risks and mitigations.
- Follow-ups filed as issues with labels.

---

## Verify

Commands or scenarios that prove this milestone:

```bash
# example
cargo test -p shakedown --all-features
```

---

## Refresher card

- **Exit:** checkboxes above all true.
- **Scope:** “Out of scope” section is part of the contract.
- **Proof:** Verify section green on CI.
