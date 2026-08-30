#!/usr/bin/env bash
# Print the docs.rs feature list from crates/lib/Cargo.toml [package.metadata.docs.rs].
# This script is the single reader of that list for local and CI rustdoc.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
awk '
    $0 == "[package.metadata.docs.rs]" { in_section = 1; next }
    in_section && /^\[/ { exit }
    in_section && $0 ~ /^features = \[/ { in_features = 1; next }
    in_features && $0 ~ /^\]/ { exit }
    in_features {
        gsub(/[", ]/, "")
        if (length($0) > 0) print
    }
' "$root/crates/lib/Cargo.toml" | paste -sd, -
