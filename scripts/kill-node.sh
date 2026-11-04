#!/usr/bin/env bash
# Crash-stop a demo node like a machine losing power: SIGKILL its agent and every pod
# process group it started. Same kill_node() the integration tests use.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/demo-env.sh
name="${1:?usage: kill-node.sh NAME}"
exec uv run python -m chronos.testing.cluster kill-node "$CHRONOS_DEMO_DIR/agents/$name"
