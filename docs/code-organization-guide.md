# Code organization guide

This document describes an organizational style used in the Shakedown workspace (especially [`crates/lib`](../crates/lib)) so you can **apply the same ideas** to other binaries and libraries—without treating it as a rigid template.

The goal is **predictable structure**: newcomers find extension points quickly, optional dependencies stay optional, and the public API stays small even when the implementation is large.

---

## 1. Principles

### 1.1 Domain modules, not “layer folders” everywhere

Prefer **vertical slices by domain** (`lifecycle`, `resources`, `outbox`, `error`) over scattering code only by technical layer (`models/`, `services/`, `utils/`). Layers still appear *inside* a domain when they help (e.g. `error/assertion.rs`, `error/infrastructure.rs`).

**Why:** Boundaries match how people reason about behavior (“where does provisioning live?”) and keep related types and tests together.

### 1.2 One façade at the crate root

Use **`lib.rs` or `main.rs`** as the **composition root**:

- Declare submodules with `mod` / `pub mod`.
- **Re-export** the stable, user-facing names at the crate root with `pub use` so call sites depend on `crate::ImportantType`, not deep paths—unless you intentionally want a nested module API.

**Why:** A thin, documented root file acts as a **table of contents** and hides internal layout changes.

### 1.3 Optional surface via features

When a crate can integrate many backends or heavy dependencies:

- Put **optional dependencies** behind **`[features]`** in `Cargo.toml`.
- Gate **`pub mod`**, **`impl` blocks**, and **`pub use`** with **`#[cfg(feature = "…")]`** so default builds stay lean.

**Why:** Compile time, clarity, and CI matrices stay manageable; each integration is an opt-in.

### 1.4 Traits and data, not implicit “objects”

Organization may *look* object-oriented (folders per “thing”), but implementation stays idiomatic Rust:

- **Behavior:** `trait` definitions in a small, stable module.
- **Variants:** `enum` for closed sets; **`dyn Trait`** only where you need open extension and accept the tradeoffs.
- **State:** `struct` + `impl`; avoid fake “base classes.”

---

## 2. Crate root checklist

| Practice | Description |
|----------|-------------|
| **Crate-level `//!` docs** | What the crate does, main types, how to extend it, links to deeper docs. |
| **Conditional modules** | `#[cfg(feature = "…")]` on `mod` / `pub mod` for optional domains. |
| **Re-exports** | `pub use submodule::Type` for the API you want consumers to use. |
| **`#[doc(inline)]`** | Use when re-exporting a submodule so docs read as if types live at the root. |
| **Macros** | If you have a proc-macro crate, re-export macros from the main library so dependents need one dependency. |

---

## 3. Layout patterns

### 3.1 “Engine” or integration folders

For each pluggable integration (database, queue, cloud API), use a **dedicated directory** with a **repeatable layout**:

- **`resource.rs` / `client.rs` / `adapter.rs`** — wiring and lifecycle for that system.
- **`helpers.rs`** — convenience APIs for tests or callers (optional).
- **`tests.rs`** or **`mod tests`** — unit tests next to the integration.

Extract **shared code** to a sibling module only when two integrations truly share it (e.g. `resp_family.rs` for two compatible drivers). Avoid a single giant `shared.rs` that becomes a junk drawer.

### 3.2 Small files under a domain umbrella

For cross-cutting concerns (errors, IDs, small value types), prefer **one file per concern** re-exported from a single `mod.rs`:

```text
error/
  mod.rs          # pub use …; pub type Result<T> = …
  assertion.rs
  infrastructure.rs
  scenario.rs
```

**Why:** Easier reviews and merges than monolithic `error.rs`.

### 3.3 Private glue

Keep **internal-only** modules private (`mod docker_labels;`) and expose only what the façade needs. Not everything deserves `pub`.

---

## 4. Errors

- Use a **small taxonomy** (e.g. user vs system vs internal) with a **top-level error type** and a **`Result` alias** at the crate or domain boundary.
- Prefer **explicit conversion** at boundaries (`.map_err(Outer::Variant)?`) over silent `From` chains where debugging “where did this come from?” matters—match your project’s error policy.
- Put **display / reporting** helpers next to **`instrumentation`** or **`diagnostics`** if they are about observability, not core logic.

---

## 5. Testing

| Layer | Placement | Notes |
|-------|-----------|--------|
| **Unit** | `#[cfg(test)]` in the same file or `tests.rs` submodule | Fast, no external services. |
| **Integration** | `tests/*.rs` at crate root | Full crate as dependency; use for multi-module flows. |
| **Heavy / optional** | Same `tests/` tree | Register with **`[[test]]`** + **`required-features`** so CI can skip Docker or slow deps unless needed. |

Name integration tests by **scenario** (`outbox_nats_publication.rs`) rather than only by module name when they document a user journey.

---

## 6. Documentation inside modules

For important modules, short **`//!` blocks** that answer:

- **What** the module is.
- **Why** it exists.
- **How** to use or extend it.

This mirrors Rust API guidelines and pays off when the crate is extended by people who do not live in the code daily.

---

## 7. Applying this to a binary (`main.rs`)

Binaries benefit from the same ideas with small shifts:

- Keep **`main` thin**: parse config, initialize runtime, delegate to a **`run()`** or library crate.
- Put domain logic in **`src/lib.rs`** (library) + **`src/main.rs`** (entry) when tests need to call logic without spawning a process—or keep modules under `src/` with `main` only wiring if the project is small.

Use **the same feature gates** if optional integrations exist (metrics backends, cloud providers).

---

## 8. Anti-patterns to avoid

- **Deep private modules with a flat `pub use` explosion** — re-exports are good; hiding every name behind five modules without documentation is not.
- **Feature interaction bugs** — every `#[cfg]` on `pub` items needs a matching story in `Cargo.toml` (features should imply dependencies consistently).
- **`utils` / `common` as junk drawers** — prefer naming by **job** (`sanitize`, `timeline`, `labels`).
- **Copy-paste integration folders** — extract shared helpers only when the second consumer appears; premature abstraction creates coupling.

---

## 9. Adoption checklist (new crate or refactor)

1. **List domains** — orchestration, integrations, errors, observability, CLI, etc.
2. **Draw the public API** — types and traits at the root re-exports; everything else private or nested on purpose.
3. **List optional deps** — one feature per heavy integration; document default features.
4. **Pick one folder convention** for integrations and stick to it.
5. **Add one integration test** that proves the façade works end-to-end.
6. **Write crate `//!` + key module `//!`** before the surface grows further.

---

## 10. Relationship to this repo

The [`shakedown`](../crates/lib) crate applies these patterns at full scale: many Cargo features, a large `lib.rs` façade, `resources/<engine>/` trees, and integration tests gated by `required-features`. Use this guide to **extract the habits**, not to duplicate file-for-file structure in smaller projects.
