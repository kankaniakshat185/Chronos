"""`python -m chronos.sim`: run, replay and minimize simulated seeds (P5.5).

python -m chronos.sim --seed 1234                # one seed; the last 200 events if it fails
python -m chronos.sim --seeds 0:200              # a range
python -m chronos.sim --seed 7 --trace t.txt     # the whole trace, for reading
python -m chronos.sim --seed 7 --canary disable_fencing --minimize
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from chronos.sim.canaries import CANARIES
from chronos.sim.run import minimize, run_seed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m chronos.sim")
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--seed", type=int)
    which.add_argument("--seeds", help="START:STOP, e.g. 0:200")
    parser.add_argument("--canary", choices=sorted(CANARIES))
    parser.add_argument("--trace", type=Path, help="write the full trace of --seed here")
    parser.add_argument("--minimize", action="store_true", help="shrink a failing --seed")
    args = parser.parse_args(argv)

    if args.seed is not None:
        result = run_seed(args.seed, args.canary, keep_trace=args.trace is not None)
        if args.trace is not None:
            args.trace.write_text("\n".join(result.tail) + "\n")
            result.tail = result.tail[-200:]
        print(result.report())
        if result.violation is not None and args.minimize:
            smallest = minimize(args.seed, args.canary, result.violation.invariant)
            print(f"\nminimal schedule still breaking {result.violation.invariant}:")
            for entry in smallest.entries:
                print(f"  {entry.describe()}")
        return 0 if result.ok else 1

    start, _, stop = args.seeds.partition(":")
    failed = 0
    for seed in range(int(start), int(stop)):
        result = run_seed(seed, args.canary)
        if not result.ok:
            failed += 1
            print(result.report().splitlines()[0])
    print(f"{int(stop) - int(start) - failed} ok, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
