# ADR-0004: HTTP/JSON (FastAPI) transport; gRPC deferred

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P1 | — | — |

## 1. What
The control-plane API is **REST/JSON over HTTP/1.1**, served by **FastAPI + uvicorn**, with **Pydantic v2** models shared by server, agent and CLI. Clients use **httpx**. All access goes through a `StoreClient` protocol so the transport can be swapped (gRPC = stretch M3) without touching agent or controller logic.

## 2. Why
### Problem
Agents, the CLI and controllers need a wire protocol to the control plane. The wrong choice either eats the schedule (learning gRPC during the hardest phase) or couples logic to transport, which makes later changes expensive.
### Forces & constraints
- The author knows FastAPI; gRPC would take an estimated 4–6 days to use well.
- **Fidelity:** in real Kubernetes, **kubelet ↔ kube-apiserver is HTTPS REST** (JSON or protobuf bodies; watch over chunked HTTP). gRPC is used for **etcd** and the **CRI** (kubelet ↔ container runtime). REST is the faithful choice for this link.
- Fault injection needs a transport that's easy to proxy and inspect (curl, logs).
- Scale: ≤ 100 real agents at 1 Hz heartbeats. Wire performance doesn't matter.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: REST/JSON via FastAPI** | Known, curl-able, OpenAPI, faithful to kubelet | One request per heartbeat; long-poll overhead | ✅ Chosen |
| A. gRPC + protobuf | Streaming, typed, HTTP/2 | Learning cost; second schema language; harder to debug | ⏸ Deferred (Backlog P12.4) |
| B. WebSockets | Bidirectional | Pushes towards push-based design; connection-state complexity | ❌ |
| C. Raw TCP + custom framing | Full control | All yak-shaving, no insight | ❌ |

**A** has a real technical advantage (streaming heartbeats/watch), but it doesn't change correctness, and performance doesn't matter at this scale; it's a clean retrofit later behind `StoreClient`. **B** favours a server-push model, which ADR-0010 rejects. **C** spends days on framing and reconnection logic that teaches nothing about orchestration.

## 4. How
### Components & files
- `src/chronos/api/`: Pydantic models (single source of schema).
- `src/chronos/server/app.py`, `routes/*.py`: FastAPI app.
- `src/chronos/client/protocol.py`: `StoreClient` protocol.
- `src/chronos/client/local.py`: `LocalStoreClient` (in-process, used by controllers in `chronosd`).
- `src/chronos/client/http.py`: `HttpStoreClient` (httpx; used by agent and CLI).
### Mechanism
```python
class StoreClient(Protocol):
    async def get(self, kind: Kind, name: str) -> Obj: ...
    async def list(self, kind: Kind, **filters) -> ListResult: ...          # items + revision
    async def create(self, obj: Obj) -> Obj: ...
    async def update(self, obj: Obj) -> Obj: ...                            # CAS on obj.metadata.resource_version
    async def update_status(self, obj: Obj) -> Obj: ...
    async def bind(self, pod: str, node: str, expected_rv: Revision) -> Obj: ...
    async def delete(self, kind: Kind, name: str, expected_rv: Revision | None) -> None: ...
    async def watch(self, kind: Kind, since: Revision, timeout: float, **filters) -> WatchBatch: ...
    async def heartbeat(self, node: str, epoch: str) -> HeartbeatReply: ...
```
- Error mapping (server → HTTP → client exception): `Conflict` ↔ 409, `NotFound` ↔ 404, `AlreadyExists` ↔ 409 (with a distinct `reason`), `Compacted` ↔ 410, `ValidationError` ↔ 422, `NotLeader` ↔ 503 (reserved, ADR-0018).
- Bodies: `{"kind": ..., "metadata": {...}, "spec": {...}, "status": {...}}`. Errors: `{"reason": "Conflict", "message": ..., "current_resource_version": ...}`.
- httpx: one `AsyncClient` per process, connection pooling on, **explicit timeouts on every call** (no defaults).
### Configuration
| Param | Default |
|-------|---------|
| `client.request_timeout` | 2.0 s (heartbeat: 0.8 s; watch: `timeout + 5 s`) |
| `client.endpoints` | `["http://127.0.0.1:7070"]` (list, for ADR-0018) |
### Edge cases & failure handling
- Timeouts and connection errors surface as `Unavailable`; callers decide whether to retry (the agent heartbeat loop does, with backoff; the CLI prints an error).
- Non-idempotent retries: `create` retried after a timeout may return `AlreadyExists`. The caller treats that as success if the uid matches.

## 5. Consequences
**Positive:** fast to build; debuggable with curl; free OpenAPI docs at `/docs`; transport-agnostic logic.
**Negative / accepted costs:** request-per-heartbeat overhead; watch latency includes request setup.
**Risks & mitigations:** contract drift between the two clients → the same contract test suite runs against `LocalStoreClient` and `HttpStoreClient`.

## 6. Verification
- P1.6 contract tests (both clients pass the same suite).
- Integration: every endpoint's status codes (409/410/422/404) against a live server.
- B2 breakdown shows network time is a negligible share of recovery time.

## 7. Revisit when
Heartbeat or watch overhead shows up in B1/B2 profiles, or you want gRPC experience. Then implement `GrpcStoreClient` (Backlog P12.4) and compare.

## 8. Related & references
ADR-0008 (watch), ADR-0010 (pull model), ADR-0018 (`NotLeader`); Kubernetes docs "API concepts" (watch, resourceVersion); CRI spec.
