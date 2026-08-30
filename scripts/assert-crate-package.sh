#!/usr/bin/env bash
# Assert published crate tarballs include LICENSE and omit tests/ (and macros examples/).
# Usage: assert-crate-package.sh [--allow-dirty]
set -euo pipefail

allow_dirty=()
if [[ "${1:-}" == "--allow-dirty" ]]; then
    allow_dirty=(--allow-dirty)
fi

assert_license_and_no_tests() {
    local crate_name="$1"
    local list
    list="$(cargo package -p "$crate_name" --list --no-verify "${allow_dirty[@]}")"
    echo "$list" | grep -qx 'LICENSE' || {
        echo "$crate_name: LICENSE missing from package" >&2
        exit 1
    }
    if echo "$list" | grep -q '^tests/'; then
        echo "$crate_name: tests/ must not ship in the package" >&2
        exit 1
    fi
    echo "$crate_name: LICENSE present, tests/ excluded"
}

assert_license_and_no_tests shakedown-macros
assert_license_and_no_tests shakedown

if cargo package -p shakedown-macros --list --no-verify "${allow_dirty[@]}" | grep -q '^examples/'; then
    echo "shakedown-macros: examples/ must not ship in the package" >&2
    exit 1
fi
