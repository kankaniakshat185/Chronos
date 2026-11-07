"""A configurable test workload (the demo and the integration tests run it).

    python -m chronos.testing.workload [--label X] [--crash-after S] [--exit-code N]
        [--ignore-sigterm] [--serve PORT [--ready-after S] [--unhealthy-after S]]
        [--leak-mb-per-s N]

Prints one line (`<label> pod=... node=... pid=...`) once its signal handlers are set,
then sleeps until --crash-after seconds pass and it exits with --exit-code.

* --serve PORT: an HTTP server for probes (P10.1). GET /ready answers 200 once
  --ready-after seconds have passed (503 before); GET /healthz answers 200 until
  --unhealthy-after seconds have passed (500 after).
* --leak-mb-per-s N: allocate N MiB of memory every second and keep it (P10.3).
"""

import argparse
import os
import signal
import socketserver
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def serve(port: int, started: float, ready_after: float, unhealthy_after: float | None) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            age = time.monotonic() - started
            if self.path == "/ready":
                code = 200 if age >= ready_after else 503
            elif self.path == "/healthz":
                code = 500 if unhealthy_after is not None and age >= unhealthy_after else 200
            else:
                code = 404
            self.send_response(code)
            self.end_headers()

        def log_message(self, *args: object) -> None:
            return None

    class Server(ThreadingHTTPServer):
        def server_bind(self) -> None:  # skip HTTPServer's reverse DNS lookup (getfqdn)
            socketserver.TCPServer.server_bind(self)
            self.server_name, self.server_port = "127.0.0.1", port

    server = Server(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="chronos-workload")
    parser.add_argument("--label", default="workload")
    parser.add_argument("--crash-after", type=float, default=None)
    parser.add_argument("--exit-code", type=int, default=1)
    parser.add_argument("--ignore-sigterm", action="store_true")
    parser.add_argument("--serve", type=int, default=None)
    parser.add_argument("--ready-after", type=float, default=0.0)
    parser.add_argument("--unhealthy-after", type=float, default=None)
    parser.add_argument("--leak-mb-per-s", type=float, default=0.0)
    args = parser.parse_args(argv)

    started = time.monotonic()
    if args.ignore_sigterm:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if args.serve is not None:
        serve(args.serve, started, args.ready_after, args.unhealthy_after)
    pod = os.environ.get("CHRONOS_POD_NAME", "-")
    node = os.environ.get("CHRONOS_NODE_NAME", "-")
    print(f"{args.label} pod={pod} node={node} pid={os.getpid()}", flush=True)

    leaked: list[bytearray] = []
    deadline = None if args.crash_after is None else started + args.crash_after
    last_leak = started
    while deadline is None or time.monotonic() < deadline:
        time.sleep(0.05)
        if args.leak_mb_per_s and time.monotonic() - last_leak >= 0.1:
            last_leak = time.monotonic()
            leaked.append(bytearray(int(args.leak_mb_per_s * 2**20 / 10)))  # touched: resident
    print(f"{args.label}: exiting with {args.exit_code}", flush=True)
    return int(args.exit_code)


if __name__ == "__main__":
    sys.exit(main())
