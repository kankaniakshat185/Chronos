#!/usr/bin/env bash
# The demo (PLAN.md A.7), unattended: starts the demo cluster, runs the key steps
# through chronosctl and the scripts, and fails loudly if any step looks wrong.
set -euo pipefail
cd "$(dirname "$0")/.."
export CHRONOS_PORT="${CHRONOS_PORT:-7079}"
export CHRONOS_DEMO_DIR="${CHRONOS_DEMO_DIR:-${TMPDIR:-/tmp}/chronos-demo-smoke}"
source scripts/demo-env.sh
ctl() { uv run --quiet chronosctl "$@"; }

scripts/demo-cluster.sh > "${CHRONOS_DEMO_DIR%/}.log" 2>&1 &
cluster=$!
agent=""
trap 'kill -TERM ${agent:-} $cluster 2>/dev/null || true; wait 2>/dev/null || true' EXIT

until_ok() {  # until_ok SECONDS DESCRIPTION COMMAND...: poll until COMMAND succeeds
  local deadline=$((SECONDS + $1)) what=$2; shift 2
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS > deadline)); then echo "FAIL: $what" >&2; ctl get pods -o wide >&2 || true; exit 1; fi
    sleep 0.25
  done
  echo "ok: $what"
}
count() { ctl get pods -o wide | awk -v n="$1" -v s="$2" -v node="${3:-}" \
  'index($1, n"-")==1 && $2==s && (node=="" || $3==node) {c++} END {print c+0}'; }
is() { [ "$("${@:2}")" = "$1" ]; }

until_ok 60 "three nodes Ready" is 3 sh -c "uv run --quiet chronosctl get nodes | grep -c ' Ready '"
ctl apply -f examples/web.yaml
until_ok 30 "web: 3 Running" is 3 count web Running
ctl apply -f examples/slot-demo.yaml
until_ok 30 "slot-demo: 2 Running" is 2 count slot-demo Running
until_ok 10 "slot-demo: 1 Pending" is 1 count slot-demo Pending
is 0 count slot-demo Running plain-1 || { echo "FAIL: slot pod on plain-1" >&2; exit 1; }
ctl scale deployment slot-demo --replicas 2
until_ok 10 "scale-down removed the Pending pod" is 0 count slot-demo Pending
scripts/kill-node.sh slot-a
until_ok 15 "slot-a NotReady" sh -c "uv run --quiet chronosctl get nodes | grep -q 'slot-a *NotReady'"
until_ok 15 "web: 3 Running again" is 3 count web Running
until_ok 15 "slot-demo replacement waits" is 1 count slot-demo Pending
is 0 count slot-demo Running plain-1 || { echo "FAIL: slot pod on plain-1" >&2; exit 1; }
scripts/start-agent.sh slot-a --resource chronos.dev/slot=1 > "${CHRONOS_DEMO_DIR%/}-slot-a.log" 2>&1 &
agent=$!
until_ok 20 "slot-demo: waiting pod placed on restarted slot-a" is 1 count slot-demo Running slot-a
ctl delete deployment web
ctl delete deployment slot-demo
until_ok 15 "all pods gone" is 0 sh -c "uv run --quiet chronosctl get pods | tail -n +2 | wc -l | tr -d ' '"
echo "demo smoke: PASS"
