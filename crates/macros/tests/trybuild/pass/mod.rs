//! Compile-pass tests for shakedown-macros.

use trybuild::TestCases;

mod basic_test;
mod faulty_test;
mod resources_arg;
mod kafka_resources;

// Configure trybuild to look for test files in the current directory
#[test]
fn run_pass_tests() {
    TestCases::from_env().suffixes(&["rs"]).run();
}
