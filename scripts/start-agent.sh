#!/usr/bin/env bash
# Start one demo agent: scripts/start-agent.sh NAME [--resource chronos.dev/slot=1 ...]
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/demo-env.sh
name="${1:?usage: start-agent.sh NAME [extra chronos-agent options]}"
shift
exec uv run chronos-agent run --name "$name" --cpu 4 --mem 8Gi \
  --server "$CHRONOS_SERVER" --data-dir "$CHRONOS_DEMO_DIR/agents/$name" \
  --heartbeat-interval "$HEARTBEAT" --poll-interval 0.5 \
  --fence-timeout "$FENCE" --termination-grace "$GRACE" --drift-margin "$DRIFT" "$@"
