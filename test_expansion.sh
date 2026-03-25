#!/bin/bash
set -e

# Create a temporary directory for the expansion test
TMPDIR=$(mktemp -d)
mkdir -p $TMPDIR/src
cd $TMPDIR

# Create a Cargo.toml that depends on shakedown
cat > Cargo.toml << 'CARGO_EOF'
[package]
name = "expansion_test"
version = "0.1.0"
edition = "2021"

[dependencies]
shakedown = { path = "/Users/seb/projects/shakedown/crates/lib" }
CARGO_EOF

# Create a test file
cat > src/main.rs << 'SRC_EOF'
use shakedown::Context;

#[shakedown::test]
async fn my_test(ctx: &mut Context) {
    assert!(true);
}

fn main() {}
SRC_EOF

# Try to build
cargo build 2>&1

# Cleanup
cd /
rm -rf $TMPDIR
