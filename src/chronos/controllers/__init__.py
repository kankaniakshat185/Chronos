"""Controllers that run inside `chronosd`, on the shared runtime (ADR-0009).

Importing this package registers the built-in controllers (X-6); `chronosd` starts
whatever is registered.
"""

from chronos.controllers import deployment, node_lifecycle, scheduler

__all__ = ["deployment", "node_lifecycle", "scheduler"]
