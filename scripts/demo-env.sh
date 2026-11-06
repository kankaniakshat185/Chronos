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
# chronosctl logs reads pod output from here (local mode).
export CHRONOS_AGENTS_DIR="$CHRONOS_DEMO_DIR/agents"
# Metrics (ADR-0024): each agent serves /metrics on its own port; Prometheus (in Docker)
# reaches them through SCRAPE_HOST. On Linux, set CHRONOS_BIND=0.0.0.0 so it can.
METRICS_PORT_BASE="${METRICS_PORT_BASE:-9101}"
CHRONOS_BIND="${CHRONOS_BIND:-127.0.0.1}"
SCRAPE_HOST="${SCRAPE_HOST:-host.docker.internal}"
TARGETS_FILE="deploy/observability/targets/chronos.json"
