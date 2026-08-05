#!/bin/sh
set -eu
root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
exec "$root/src/copilotd/ops/scripts/package-smoke.sh" "$root"
