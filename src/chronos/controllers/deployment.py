"""Deployment controller (ADR-0009): informer caches, a work queue, expectations.

Keys are deployment **uids**, the value pods carry in `metadata.owner`, so a pod
change maps straight to its key and a deleted deployment's key still finds its
pods. `reconcile(uid)` reads the deployment and its pods from the caches and
applies `core.reconcile`, the same pure function the MVP polling loop used.

The caches lag the store, and creating a pod conflicts with nothing (CAS guards
updates of one existing object, not "make a 3rd one"), so a reconcile on a cache
that hasn't seen its own creates yet would create them again. Expectations close
that gap: before acting, the controller records how many pods it is creating and
which it is deleting; the pod informer's handler ticks them off as they show up;
until then the key's reconcile does nothing (ADR-0009 3.C, the `disable_expectations`
canary).

Pods whose owner is gone are garbage-collected, after a live read confirms the
owner really is gone and the cache isn't just behind.
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Callable

from chronos.api import Deployment, Pod
from chronos.client.local import LocalStoreClient
from chronos.common.clock import Clock
from chronos.controllers.runtime import Controller, Expectations, Informers, register_controller
from chronos.core.reconcile import (
    CreatePod,
    DeletePod,
    UpdateStatus,
    is_active,
    pod_for,
    reconcile,
)
from chronos.store import StoreError

log = logging.getLogger(__name__)

_SUFFIX_CHARS = "bcdfghjklmnpqrstvwxz2456789"  # no vowels: no accidental words
UID = "uid"
OWNER = "owner"


def pod_name(deployment: str) -> str:
    suffix = "".join(secrets.choice(_SUFFIX_CHARS) for _ in range(5))
    return f"{deployment[:57]}-{suffix}"


class DeploymentController(Controller):
    name = "deployment"

    def __init__(
        self,
        client: LocalStoreClient,
        clock: Clock,
        informers: Informers | None = None,
        *,
        resync_period: float = 30.0,
        expectations_ttl: float = 30.0,
        new_name: Callable[[str], str] = pod_name,
    ) -> None:
        informers = informers or Informers(client, clock)
        self._client = client
        self._new_name = new_name
        self._deployments = informers.get(Deployment)
        self._pods = informers.get(Pod)
        self._deployments.add_index(UID, lambda d: d.metadata.uid)
        self._pods.add_index(OWNER, lambda p: p.metadata.owner)
        self.expectations = Expectations(clock, expectations_ttl)
        super().__init__(clock, [self._deployments, self._pods], resync_period=resync_period)
        self._deployments.add_handler(self._on_deployment)
        self._pods.add_handler(self._on_pod)

    def _on_deployment(self, old: Deployment | None, new: Deployment | None) -> None:
        dep = new or old
        assert dep is not None
        self.queue.add(dep.metadata.uid)

    def _on_pod(self, old: Pod | None, new: Pod | None) -> None:
        pod = new or old
        assert pod is not None
        owner = pod.metadata.owner
        if owner is None:
            return
        if old is None:
            self.expectations.created(owner)
        if new is None or new.metadata.deletion_timestamp is not None:
            self.expectations.deleted(owner, pod.metadata.name)
        self.queue.add(owner)

    def keys(self) -> list[str]:
        owners = {p.metadata.owner for p in self._pods.list() if p.metadata.owner}
        return sorted(owners | set(self._deployments_by_uid()))

    def _deployments_by_uid(self) -> dict[str, Deployment]:
        return {d.metadata.uid: d for d in self._deployments.list()}

    async def reconcile(self, key: str) -> None:
        pods = self._pods.by_index(OWNER, key)
        found = self._deployments.by_index(UID, key)
        if not found:
            await self._collect(key, pods)
            return
        if not self.expectations.satisfied(key):
            return  # the cache hasn't seen our last creates/deletes yet: wait for them
        dep = found[0]
        actions = reconcile(dep, pods)
        active = {p.metadata.name for p in pods if is_active(p)}
        creates = sum(a.count for a in actions if isinstance(a, CreatePod))
        deletes = [a.name for a in actions if isinstance(a, DeletePod) and a.name in active]
        self.expectations.expect(key, creates, deletes)
        failed: StoreError | None = None
        for action in actions:
            try:
                await self._apply(dep, action)
            except StoreError as exc:
                failed = failed or exc
        if failed is not None:
            raise failed  # requeued with backoff; expectations were already lowered

    async def _apply(self, dep: Deployment, action: CreatePod | DeletePod | UpdateStatus) -> None:
        name, uid = dep.metadata.name, dep.metadata.uid
        match action:
            case CreatePod(count=count):
                for _ in range(count):
                    try:
                        created = await self._client.create(pod_for(dep, self._new_name(name)))
                    except StoreError:
                        self.expectations.created(uid)  # it will never be observed
                        raise
                    log.info("deployment %s: created pod %s", name, created.metadata.name)
            case DeletePod(name=pod, resource_version=rv):
                try:
                    await self._client.delete(Pod, pod, expected_rv=rv)
                except StoreError:
                    self.expectations.deleted(uid, pod)
                    raise
                log.info("pod %s: deleted (deployment %s)", pod, name)
            case UpdateStatus(status=status):
                dep.status = status
                await self._client.update_status(dep)

    async def _collect(self, uid: str, pods: list[Pod]) -> None:
        """Garbage-collect the pods of an owner that no longer exists."""
        self.expectations.forget(uid)
        pods = [p for p in pods if p.metadata.deletion_timestamp is None]
        if not pods:
            return
        live, _ = await self._client.list(Deployment)
        if any(d.metadata.uid == uid for d in live):
            return  # the cache is behind: its ADDED event will queue the key again
        for pod in pods:
            await self._client.delete(
                Pod, pod.metadata.name, expected_rv=pod.metadata.resource_version
            )
            log.info("pod %s: deleted (orphaned)", pod.metadata.name)


register_controller(
    DeploymentController.name,
    lambda ctx: DeploymentController(
        ctx.client, ctx.clock, ctx.informers, resync_period=ctx.timings.resync_period
    ),
)
