#!/usr/bin/env bash
# The demo cluster (PLAN.md A.7): chronosd plus plain-1, slot-a and slot-b, logs in this
# terminal. Ctrl-C stops the agents gracefully (they stop their workloads), then chronosd.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/demo-env.sh

rm -rf "$CHRONOS_DEMO_DIR"
mkdir -p "$CHRONOS_DEMO_DIR"
pids=()
cleanup() {
  trap - EXIT INT TERM
  for pid in "${pids[@]:1}"; do kill -TERM "$pid" 2>/dev/null || true; done
  wait "${pids[@]:1}" 2>/dev/null || true
  kill -TERM "${pids[0]}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

uv run chronosd run --port "$CHRONOS_PORT" --data-dir "$CHRONOS_DEMO_DIR/server" \
  --heartbeat-interval "$HEARTBEAT" \
  --node-grace "$NODE_GRACE" --eviction-timeout "$EVICTION" &
pids+=($!)
for _ in $(seq 100); do
  curl -sf "$CHRONOS_SERVER/healthz" >/dev/null && break
  sleep 0.1
done
curl -sf "$CHRONOS_SERVER/healthz" >/dev/null || { echo "chronosd didn't start" >&2; exit 1; }

scripts/start-agent.sh plain-1 & pids+=($!)
scripts/start-agent.sh slot-a --resource chronos.dev/slot=1 & pids+=($!)
scripts/start-agent.sh slot-b --resource chronos.dev/slot=1 & pids+=($!)
echo "chronosd on $CHRONOS_SERVER, agents plain-1 slot-a slot-b, data in $CHRONOS_DEMO_DIR"
wait
