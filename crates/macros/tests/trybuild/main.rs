//! Integration test suite for the `shakedown-macros` procedural macros using `trybuild`.
//!
//! **What:** This module runs compile-pass and compile-fail tests against real usage of
//! `#[shakedown::test]`, `#[shakedown::faulty_test]`, and related attributes.
//!
//! **Why:** Unit tests on synthetic `proc_macro2::TokenStream`s are not enough. `trybuild` exercises
//! the **full macro pipeline** (real `proc_macro::TokenStream` → expansion → type checking by rustc)
//! exactly as users will experience it in their own crates. It also locks in the exact compiler
//! error messages for bad inputs.
//!
//! **Structure:**
//! - `pass/*.rs` — must compile successfully (happy-path scenarios)
//! - `fail/*.rs` + matching `.stderr` — must fail to compile with the expected error (regression tests for diagnostics)
//!
//! All paths are relative to the `shakedown-macros` crate root (`CARGO_MANIFEST_DIR`).

#[test]
fn macros_ui() {
    // Given the compile-pass and compile-fail fixtures under tests/trybuild
    let t = trybuild::TestCases::new();

    // When trybuild compiles each case
    t.pass("tests/trybuild/pass/plain_no_resources.rs");
    t.pass("tests/trybuild/pass/basic_test.rs");
    t.pass("tests/trybuild/pass/faulty_test.rs");
    t.pass("tests/trybuild/pass/faulty_test_deferred_args.rs");
    t.pass("tests/trybuild/pass/faulty_test_fault_profile.rs");
    t.pass("tests/trybuild/pass/resources_arg.rs");
    t.pass("tests/trybuild/pass/prelude_arg.rs");
    t.pass("tests/trybuild/pass/kafka_resources.rs");

    t.compile_fail("tests/trybuild/fail/sync_function.rs");
    t.compile_fail("tests/trybuild/fail/wrong_signature.rs");
    t.compile_fail("tests/trybuild/fail/invalid_resource.rs");

    // Then all expected outcomes match the checked-in .stderr snapshots
}
