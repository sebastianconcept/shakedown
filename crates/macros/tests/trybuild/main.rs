//! trybuild: compile-pass and compile-fail checks for `#[shakedown::test]` / `#[shakedown::faulty_test]`.
//!
//! Paths are relative to the `shakedown-macros` crate root (`CARGO_MANIFEST_DIR`).

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
    t.compile_fail("tests/trybuild/fail/sync_function.rs");
    t.compile_fail("tests/trybuild/fail/wrong_signature.rs");
    t.compile_fail("tests/trybuild/fail/invalid_resource.rs");
    // Then all expected outcomes match the checked-in .stderr snapshots
}
