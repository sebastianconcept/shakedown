//! shakedown-macros is the procedural-macro crate for Shakedown scenario attributes.
//!
//! This crate lets a test crate mark async functions as scenarios with [`test`] and [`faulty_test`].
//! This crate does not depend on the `shakedown` library. Generated code names `::shakedown`.

use proc_macro::TokenStream as ProcMacroTokenStream;

mod faulty_test;
mod register_resource;
mod resource_registration;
mod signature;
mod test;

/// test is the attribute for a normal Shakedown scenario test.
/// This attribute lets a test crate run an async function inside the scenario lifecycle.
/// This attribute does not supply `tokio`. The test crate must depend on `tokio` with `macros` and `rt-multi-thread`.
///
/// The attributed function must be `async` and take `ctx: &mut shakedown::Context` as the first parameter.
/// Non-empty `resources = [...]` emits `#[ignore]` unless `ignore = false`.
/// Optional `prelude = { ... }` runs on `ctx` after resource registration and before the lifecycle.
///
/// # Panics
///
/// Panics with the display string of `shakedown::ShakedownError` if the scenario fails.
///
/// ```rust,no_run
/// #[shakedown::test]
/// async fn my_scenario(ctx: &mut Context) {
///     let _ = ctx;
/// }
/// ```
#[proc_macro_attribute]
pub fn test(attr: ProcMacroTokenStream, item: ProcMacroTokenStream) -> ProcMacroTokenStream {
    test::expand(attr.into(), item.into()).into()
}

/// faulty_test is the attribute for a fault-oriented Shakedown scenario test.
/// This attribute lets a test crate mark a scenario as [`ScenarioKind::Faulty`](https://docs.rs/shakedown/latest/shakedown/enum.ScenarioKind.html) before the run.
/// This attribute does not apply network faults by itself. The `fault-injection` feature does that.
///
/// Resource lists and `ignore` match [`test`]. Optional `fault_profile = "..."` sets a profile string on the context.
///
/// # Panics
///
/// Panics if `shakedown::run_scenario` returns `Err`, same as [`test`].
#[proc_macro_attribute]
pub fn faulty_test(attr: ProcMacroTokenStream, item: ProcMacroTokenStream) -> ProcMacroTokenStream {
    faulty_test::expand(attr.into(), item.into()).into()
}

/// register_resource is a pass-through attribute on an item.
/// This attribute lets a crate keep the annotation without a compile error.
/// This attribute does not register a resource or change the item.
#[proc_macro_attribute]
pub fn register_resource(
    _attr: ProcMacroTokenStream,
    item: ProcMacroTokenStream,
) -> ProcMacroTokenStream {
    register_resource::expand(_attr.into(), item.into()).into()
}
