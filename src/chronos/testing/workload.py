"""A controllable workload process for tests and demos (P2.8).

    python -m chronos.testing.workload [--label X] [--crash-after S] [--exit-code N]
                                       [--ignore-sigterm]

Prints one line with its label, pod name and pid, then runs until killed, or
until --crash-after seconds pass and it exits with --exit-code.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="chronos-workload")
    parser.add_argument("--label", default="workload")
    parser.add_argument("--crash-after", type=float, default=None)
    parser.add_argument("--exit-code", type=int, default=1)
    parser.add_argument("--ignore-sigterm", action="store_true")
    args = parser.parse_args(argv)

    if args.ignore_sigterm:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    pod = os.environ.get("CHRONOS_POD_NAME", "-")
    node = os.environ.get("CHRONOS_NODE_NAME", "-")
    print(f"{args.label} pod={pod} node={node} pid={os.getpid()}", flush=True)

    deadline = None if args.crash_after is None else time.monotonic() + args.crash_after
    while deadline is None or time.monotonic() < deadline:
        time.sleep(0.05)
    print(f"{args.label}: exiting with {args.exit_code}", flush=True)
    return int(args.exit_code)


if __name__ == "__main__":
    sys.exit(main())
