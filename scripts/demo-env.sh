# Shared settings for the demo scripts (sourced, not run).
CHRONOS_DEMO_DIR="${CHRONOS_DEMO_DIR:-${TMPDIR:-/tmp}/chronos-demo}"
CHRONOS_PORT="${CHRONOS_PORT:-7070}"
export CHRONOS_SERVER="http://127.0.0.1:${CHRONOS_PORT}"
# Demo timings: heartbeat 0.5 s, Unknown after 2 s, NotReady after 5 s. Fencing (ADR-0012):
# an agent with no accepted heartbeat for 2 s stops its workloads within 1 s more, and
# 2 + 1 + 1 (drift margin) = 4 < 5, so they're dead before any replacement starts.
HEARTBEAT=0.5
NODE_GRACE=2
EVICTION=5
FENCE=2
GRACE=1
DRIFT=1
