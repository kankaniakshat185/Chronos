"""`python -m chronos.shim --lease PATH ... -- COMMAND...` (ADR-0012).

The shim leads the pod's process group; the workload is its child. Every `--poll`
seconds it reads the agent's lease file (the monotonic send time of the agent's last
accepted heartbeat):

* lease age > eviction timeout E: SIGKILL the group now. The shim itself must have
  been frozen, and replacements may already be running elsewhere.
* lease age > fence timeout F: SIGTERM the group, SIGKILL it after the grace K.
* a missing or unreadable lease counts as infinitely old (fail safe).

So a dead, paused or partitioned agent still fences its workloads. On its own
SIGTERM (the agent stopping the pod) the shim waits for the workload and exits with
its status; the agent SIGKILLs the group after the grace if needed.

Standard library and `chronos.core` only: one shim per pod, so it stays small.
"""

from __future__ import annotations

import argparse
import math
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from chronos.core.fencing import FENCED_EXIT_CODE, ShimAction, shim_action

EXIT_NOT_STARTABLE = 127


def lease_age(path: Path) -> float:
    try:
        return time.monotonic() - float(path.read_text())
    except (OSError, ValueError):
        return math.inf


def _exit_like(code: int) -> int:
    """Exit the way the workload did, so the agent sees the same status."""
    if code < 0:
        if -code not in (signal.SIGKILL, signal.SIGSTOP):  # those can't have handlers
            signal.signal(-code, signal.SIG_DFL)
        os.kill(os.getpid(), -code)
    return code if code >= 0 else 128 - code


def _fence(child: subprocess.Popen[bytes], grace: float) -> int:
    os.killpg(0, signal.SIGTERM)  # our own handler ignores it; the workload gets it
    try:
        child.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        os.killpg(0, signal.SIGKILL)  # takes the shim down too
    return FENCED_EXIT_CODE


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="chronos-shim")
    parser.add_argument("--lease", type=Path, required=True)
    parser.add_argument("--pod-uid", required=True)  # lets orphan cleanup recognize us
    parser.add_argument("--fence-timeout", type=float, required=True)
    parser.add_argument("--eviction-timeout", type=float, required=True)
    parser.add_argument("--grace", type=float, required=True)
    parser.add_argument("--poll", type=float, default=0.1)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("missing command after --")

    # A group-wide SIGTERM is meant for the workload; the shim outlives it to report.
    signal.signal(signal.SIGTERM, lambda *_: None)
    try:
        child = subprocess.Popen(command)
    except OSError as exc:
        print(f"chronos-shim: can't start {command[0]!r}: {exc}", file=sys.stderr)
        return EXIT_NOT_STARTABLE

    while True:
        code = child.poll()
        if code is not None:
            return _exit_like(code)
        match shim_action(lease_age(args.lease), args.fence_timeout, args.eviction_timeout):
            case ShimAction.KILL:
                print("chronos-shim: lease past eviction timeout: SIGKILL", file=sys.stderr)
                os.killpg(0, signal.SIGKILL)
            case ShimAction.FENCE:
                print("chronos-shim: lease past fence timeout: fencing", file=sys.stderr)
                return _fence(child, args.grace)
            case ShimAction.RUN:
                pass
        time.sleep(args.poll)


if __name__ == "__main__":
    sys.exit(main())
