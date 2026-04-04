#!/usr/bin/env bash
# Fail if a crates/lib/tests/*_vertical.rs file exists without a matching [[test]] name in
# crates/lib/Cargo.toml (catches forgotten test registration when adding a vertical smoke binary).
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
missing=0
for f in crates/lib/tests/*_vertical.rs; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f" .rs)"
  if ! grep -qE "^name = \"${name}\"$" crates/lib/Cargo.toml; then
    echo "error: no [[test]] name = \"${name}\" for ${f} (add to crates/lib/Cargo.toml)" >&2
    missing=1
  fi
done
exit "$missing"
