#!/usr/bin/env bash
# Wait until a crate version is visible on the crates.io API.
# Usage: wait-for-crates-io-crate.sh <crate-name> <version> [timeout-seconds]
set -euo pipefail

crate_name="${1:?crate name required}"
crate_version="${2:?crate version required}"
timeout_seconds="${3:-600}"
user_agent="shakedown-release-workflow (https://github.com/sebastianconcept/shakedown)"
api_url="https://crates.io/api/v1/crates/${crate_name}/${crate_version}"

echo "waiting for ${crate_name}@${crate_version} on crates.io"
deadline=$((SECONDS + timeout_seconds))
while (( SECONDS < deadline )); do
    if curl -fsS -A "$user_agent" "$api_url" >/dev/null; then
        echo "index has ${crate_name}@${crate_version}"
        exit 0
    fi
    echo "not yet in index; retrying in 15s"
    sleep 15
done

echo "timed out after ${timeout_seconds}s waiting for ${crate_name}@${crate_version}" >&2
exit 1
