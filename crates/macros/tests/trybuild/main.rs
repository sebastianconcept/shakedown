//! trybuild: compile-pass and compile-fail checks for `#[shakedown::test]` / `#[shakedown::faulty_test]`.
//!
//! Paths are relative to the `shakedown-macros` crate root (`CARGO_MANIFEST_DIR`).

#[test]
fn macros_ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/trybuild/pass/basic_test.rs");
    t.pass("tests/trybuild/pass/faulty_test.rs");
    t.pass("tests/trybuild/pass/resources_arg.rs");
    t.compile_fail("tests/trybuild/fail/sync_function.rs");
    t.compile_fail("tests/trybuild/fail/wrong_signature.rs");
    t.compile_fail("tests/trybuild/fail/invalid_resource.rs");
}
