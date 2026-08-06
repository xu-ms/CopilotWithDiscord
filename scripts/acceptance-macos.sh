#!/bin/sh
set -eu
root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
exec "$root/src/copilotd/ops/scripts/acceptance-macos.sh" "$@"
