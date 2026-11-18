#!/usr/bin/env bash
# Fails when the vendored hub.proto drifted from snodra-server's proto/hub.proto (the source of truth).
#
# The source copy is $SNODRA_PROTO_SOURCE, else ../snodra-server/proto/hub.proto next to this repo. When it does not
# exist the check is skipped (exit 0), unless SNODRA_PROTO_SYNC_REQUIRED=true (CI jobs that clone snodra-server).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
copy="$root/snodra-messaging/src/main/proto/hub.proto"
source="${SNODRA_PROTO_SOURCE:-$root/../snodra-server/proto/hub.proto}"

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

if [ ! -f "$source" ]; then
  if [ "${SNODRA_PROTO_SYNC_REQUIRED:-false}" = "true" ]; then
    echo "proto sync: snodra-server proto not found at $source" >&2
    exit 1
  fi
  echo "proto sync: snodra-server proto not found at $source, skipping"
  exit 0
fi

want="$(sha256 "$source")"
have="$(sha256 "$copy")"
if [ "$want" != "$have" ]; then
  echo "proto sync: snodra-messaging/src/main/proto/hub.proto (sha256 $have) drifted from $source (sha256 $want)" >&2
  echo "proto sync: run: cp $source $copy" >&2
  exit 1
fi
echo "proto sync: snodra-messaging/src/main/proto/hub.proto matches $source (sha256 $want)"
