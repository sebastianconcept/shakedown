#!/usr/bin/env bash
# Build rustdoc for `shakedown` with the docs.rs feature set from Cargo.toml.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
features="$("$root/scripts/docsrs-features.sh")"
cargo doc -p shakedown --no-deps --features "$features"
