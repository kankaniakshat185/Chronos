# Shared settings for the demo scripts (sourced, not run).
CHRONOS_DEMO_DIR="${CHRONOS_DEMO_DIR:-${TMPDIR:-/tmp}/chronos-demo}"
CHRONOS_PORT="${CHRONOS_PORT:-7070}"
export CHRONOS_SERVER="http://127.0.0.1:${CHRONOS_PORT}"
# Demo timings (PLAN.md A.3): heartbeat 0.5 s, Unknown after 2 s, NotReady after 5 s.
HEARTBEAT=0.5
NODE_GRACE=2
EVICTION=5
