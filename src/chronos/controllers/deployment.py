"""Deployment controller (ADR-0009; MVP: 250 ms polling with authoritative reads, ADR-0022).

Each tick reads every deployment and pod straight from the store and applies
`core.reconcile`. Because reads are authoritative and the actions finish within
the tick, there's no stale cache to over-create from, so no expectations are
needed yet (they arrive with informers in PM-3). Any failed write is just retried
on the next tick from fresh state.
"""

from __future__ import annotations

import logging
import secrets
from collections import defaultdict

from chronos.api import Deployment, Pod
from chronos.client.local import LocalStoreClient
from chronos.common.clock import Clock
from chronos.core.reconcile import CreatePod, DeletePod, UpdateStatus, orphans, pod_for, reconcile
from chronos.store import StoreError

log = logging.getLogger(__name__)

_SUFFIX_CHARS = "bcdfghjklmnpqrstvwxz2456789"  # no vowels: no accidental words


def pod_name(deployment: str) -> str:
    suffix = "".join(secrets.choice(_SUFFIX_CHARS) for _ in range(5))
    return f"{deployment[:57]}-{suffix}"


class DeploymentController:
    def __init__(self, client: LocalStoreClient, clock: Clock, tick: float) -> None:
        self._client = client
        self._clock = clock
        self._tick = tick

    async def tick(self) -> None:
        deployments, _ = await self._client.list(Deployment)
        pods, _ = await self._client.list(Pod)
        owned: dict[str, list[Pod]] = defaultdict(list)
        for pod in pods:
            if pod.metadata.owner is not None:
                owned[pod.metadata.owner].append(pod)

        for dep in deployments:
            for action in reconcile(dep, owned[dep.metadata.uid]):
                await self._apply(dep, action)

        for pod in orphans(pods, {d.metadata.uid for d in deployments}):
            await self._delete(pod.metadata.name, pod.metadata.resource_version, "orphaned")

    async def _apply(self, dep: Deployment, action: CreatePod | DeletePod | UpdateStatus) -> None:
        name = dep.metadata.name
        try:
            match action:
                case CreatePod(count=count):
                    for _ in range(count):
                        created = await self._client.create(pod_for(dep, pod_name(name)))
                        log.info("deployment %s: created pod %s", name, created.metadata.name)
                case DeletePod(name=pod, resource_version=rv):
                    await self._delete(pod, rv, f"deployment {name}")
                case UpdateStatus(status=status):
                    dep.status = status
                    await self._client.update_status(dep)
        except StoreError as exc:  # Conflict, AlreadyExists (name clash): next tick retries
            log.info("deployment %s: %s deferred: %s", name, type(action).__name__, exc)

    async def _delete(self, pod: str, rv: int, why: str) -> None:
        try:
            await self._client.delete(Pod, pod, expected_rv=rv)
        except StoreError as exc:
            log.info("pod %s: delete (%s) deferred: %s", pod, why, exc)
            return
        log.info("pod %s: deleted (%s)", pod, why)

    async def run(self) -> None:
        while True:
            await self.tick()
            await self._clock.sleep(self._tick)
