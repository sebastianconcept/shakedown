//! Procedural macros for the `shakedown` test harness.
//!
//! Attributes and helpers that expand into `#[tokio::test]` async tests wired to
//! [`shakedown::run_scenario`](https://docs.rs/shakedown/latest/shakedown/fn.run_scenario.html),
//! plus optional compile-time resource registration.
//!
//! Tests should not duplicate `Context::new`, `EventLog::new`, and scenario wiring in every
//! function.
//!
//! **Macros (public API):**
//! - [`test`] — standard scenario tests; optional `resources = [...]` registers builtins when the
//!   matching `shakedown` crate features are enabled.
//! - [`faulty_test`] — same as [`test`], but sets [`shakedown::ScenarioKind::Faulty`](https://docs.rs/shakedown/latest/shakedown/enum.ScenarioKind.html)
//!   for fault-injection / degraded-mode scenarios.
//! - [`register_resource`] — reserved pass-through (no expansion yet); future: resource self-registration.
//! - [`default_greeting`] — tiny proc-macro demo for crate smoke tests.

use proc_macro::TokenStream as ProcMacroTokenStream;

mod default_greeting;
mod faulty_test;
mod register_resource;
mod resource_registration;
mod signature;
mod test;

/// Declarative entry point for a **normal** Shakedown scenario test.
///
/// An attribute you place on an `async fn` so the macro expands it into a `#[tokio::test]`
/// that constructs [`shakedown::Context`](https://docs.rs/shakedown/latest/shakedown/struct.Context.html),
/// an [`shakedown::EventLog`](https://docs.rs/shakedown/latest/shakedown/event/struct.EventLog.html),
/// optionally pre-registers builtin resources, then runs your body inside
/// [`shakedown::run_scenario`](https://docs.rs/shakedown/latest/shakedown/fn.run_scenario.html).
///
/// # Order of operations
///
/// End-to-end, a `#[shakedown::test]` function runs stages in this order:
///
/// 1. **`resources`** — builtins from `resources = [...]` are registered on a fresh context (macro expansion).
///    SQL resources can declare schema and static seed files here, e.g.
///    `Postgres(migrations = "./migrations", seed = "./seed.sql")` — paths are relative to the test crate's
///    `CARGO_MANIFEST_DIR` and are wired into [`shakedown::PostgresResource::with_migrations_path`](https://docs.rs/shakedown/latest/shakedown/struct.PostgresResource.html).
///    Migrations run inside each resource's [`Resource::apply_default_seed`](https://docs.rs/shakedown/latest/shakedown/trait.Resource.html#tymethod.apply_default_seed)
///    (before programmatic [`set_seed`](https://docs.rs/shakedown/latest/shakedown/struct.PostgresResource.html#method.set_seed) closures).
/// 2. **`prelude`** — optional `prelude = { ... }` runs next (register HTTP mocks, `ServiceUnderTest`, etc.).
///    This happens **before** [`run_scenario`](https://docs.rs/shakedown/latest/shakedown/fn.run_scenario.html)
///    so everything is wired on `ctx` when the harness starts.
/// 3. **`run_scenario`** then runs **provision** and **readiness** for registered resources, **seed**
///    (`apply_default_seed`), **mocks** (mock HTTP/gRPC listeners start), **services** (service-under-test
///    containers start and become healthy), your scenario **body**, the harness **assert** phase, and
///    finally **teardown**.
///
/// **Unlocks when used:**
/// - **No wiring boilerplate** — you write only the scenario body; the harness supplies `ctx` and
///   drives the lifecycle.
/// - **Scenario identity** — the Rust function name becomes the scenario id string passed to
///   `run_scenario` (for logs, isolation keys, diagnostics).
/// - **`resources = [ ... ]`** — compile-time registration of builtins (`Fake`, `Kafka`, `Postgres`,
///   …) on `ctx` before your code runs; each name requires the matching feature on the `shakedown`
///   dependency (e.g. `kafka` for `Kafka`). Any other identifier or expression (e.g.
///   `crate::my_tests::VaultDevResource::new()`) is passed to `ctx.register_extension` and must
///   evaluate to a type implementing [`shakedown::Resource`](https://docs.rs/shakedown/latest/shakedown/resources/trait.Resource.html).
/// - **`prelude = { ... }`** — statements run on `ctx` after resource registration and before
///   [`shakedown::run_scenario`](https://docs.rs/shakedown/latest/shakedown/fn.run_scenario.html)
///   (e.g. register HTTP/gRPC mock services, which must complete before the harness starts mock listeners).
/// - **`ignore = true | false`** — optional override for automatic `#[ignore]` on the generated
///   `#[tokio::test]`. When `resources = [...]` is non-empty, the macro emits `#[ignore]` by default
///   so Docker-dependent tests are not run by plain `cargo test`. Set `ignore = false` when your CI
///   always provides Docker and you want these tests to run without `cargo test -- --ignored`. When
///   `ignore` is omitted: non-empty `resources` implies `#[ignore]`; empty or absent `resources`
///   does not add `#[ignore]`. A redundant `#[ignore]` on the same function is merged with the
///   generated attribute (no duplicate `#[ignore]`).
/// - **Integration with the rest of the harness** — same path as manual `run_scenario` callers:
///   outbox assertions, Kafka/DB bindings, Docker diagnostics when features are on, etc.
///
/// The attributed function must be `async` and take `ctx: &mut shakedown::Context` as the first parameter.
///
/// # Resource registration
///
/// - `#[shakedown::test]` — no builtins; register manually on `ctx` if needed.
/// - `#[shakedown::test(resources = [Fake])]` — registers `shakedown::FakeResource`.
/// - `#[shakedown::test(resources = [Fake, Kafka])]` — also registers Kafka when the `shakedown`
///   dependency enables the `kafka` feature.
/// - `#[shakedown::test(resources = [Fake, crate::my_module::CustomResource::new()])]` — builtins
///   and custom [`Resource`](https://docs.rs/shakedown/latest/shakedown/resources/trait.Resource.html)
///   implementations in one list.
/// - `#[shakedown::test(prelude = { ctx.register_http_mock_service("api", stub).unwrap(); })]` —
///   runs the block before the scenario lifecycle (after `resources` registration).
///
/// # Context injection
///
/// The macro injects `ctx: &mut ::shakedown::Context` into your function using an absolute path.
/// **Do not add `use shakedown::Context;`** to your test file — the import is unnecessary and will
/// trigger an `unused_imports` warning from the compiler.
///
/// ```rust,no_run
/// // Correct — no `use shakedown::Context` needed
/// #[shakedown::test]
/// async fn my_scenario(ctx: &mut Context) {
///     // ctx is provided by the macro
/// }
/// ```
///
/// # Panics
///
/// Panics with the display string of `shakedown::ShakedownError` if the scenario fails.
#[proc_macro_attribute]
pub fn test(attr: ProcMacroTokenStream, item: ProcMacroTokenStream) -> ProcMacroTokenStream {
    test::expand(attr.into(), item.into()).into()
}

/// Declarative entry point for a **fault-oriented** Shakedown scenario test.
///
/// Same expansion as [`test`] — `#[tokio::test]`, fresh `Context` and `EventLog`, optional
/// `resources = [ ... ]`, then `run_scenario` — except the Shakedown sets
/// [`shakedown::ScenarioKind::Faulty`](https://docs.rs/shakedown/latest/shakedown/enum.ScenarioKind.html)
/// on the context **before** the scenario runs.
///
/// **Unlocks when used:**
/// - **Everything [`test`] unlocks** (lifecycle, builtins, scenario id, integrations).
/// - **Fault-injection signalling** — code under `ScenarioKind::Faulty` can branch (now or in future
///   milestones) to simulate failures, chaos, or degraded infrastructure without changing the macro
///   shape.
/// - **`fault_profile = "..."`** — expands to `ctx.set_fault_profile(...)` before the scenario runs.
/// - **Other `name = <expr>` pairs** — reserved for future wiring; parsed and ignored (no silent
///   failure for unknown keys beyond `resources` / `fault_profile`).
///
/// Use for scenarios that explicitly opt into fault injection or degraded-mode behaviour. Resource
/// lists match [`test`].
///
/// # Panics
///
/// Panics if `shakedown::run_scenario` returns `Err`, same as [`test`].
#[proc_macro_attribute]
pub fn faulty_test(attr: ProcMacroTokenStream, item: ProcMacroTokenStream) -> ProcMacroTokenStream {
    faulty_test::expand(attr.into(), item.into()).into()
}

/// Placeholder attribute for **resource self-registration** (not implemented yet).
///
/// A no-op attribute that expands to its input item unchanged. It exists so API shape can be
/// reserved early.
///
/// Use only if you want to annotate types or modules in preparation for future Shakedown integration.
///
/// Will unlock (planned): attaching metadata to `Resource` implementations so the Shakedown can
/// discover and register them without listing every type in `resources = [...]` at the test site.
#[proc_macro_attribute]
pub fn register_resource(
    _attr: ProcMacroTokenStream,
    item: ProcMacroTokenStream,
) -> ProcMacroTokenStream {
    register_resource::expand(_attr.into(), item.into()).into()
}

/// Minimal **function-like** proc-macro used for crate smoke tests and examples.
///
/// Expands to the string literal `"Hello, shakedown!"` (no configurable input).
///
/// Unlocks when used: a quick compile-time check that the `shakedown-macros` crate is linked
/// and procedural macros resolve correctly; not intended for production scenario code.
#[proc_macro]
pub fn default_greeting(_input: ProcMacroTokenStream) -> ProcMacroTokenStream {
    default_greeting::expand(_input.into()).into()
}
