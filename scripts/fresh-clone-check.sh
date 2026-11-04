#!/usr/bin/env bash
# What a fresh clone sees: copy exactly the files git would ship (tracked + untracked,
# minus .gitignore'd ones) into an empty directory, install from the lockfile, and run
# the unattended demo there. Pass a git URL to test a real clone instead.
set -euo pipefail
src="$(cd "$(dirname "$0")/.." && pwd)"
dest="$(mktemp -d "${TMPDIR:-/tmp}/chronos-fresh.XXXXXX")"
if [ $# -gt 0 ]; then
  git clone --quiet "$1" "$dest/chronos"
else
  mkdir "$dest/chronos"
  (cd "$src" && git ls-files -co --exclude-standard -z) | (cd "$src" && xargs -0 tar cf -) \
    | tar xf - -C "$dest/chronos"
fi
cd "$dest/chronos"
echo "fresh copy in $PWD"
uv sync --locked --quiet
CHRONOS_DEMO_DIR="$dest/demo" scripts/demo-smoke.sh
