//! Compile-fail tests for shakedown-macros.

use trybuild::TestCases;

mod sync_function;
mod wrong_signature;
mod invalid_resource;

// Configure trybuild to look for test files in the current directory
#[test]
fn run_fail_tests() {
    TestCases::from_env().suffixes(&["rs"]).run();
}
