# ADR 0003: Enforce binding-scoped tokens for workload-cluster QueueTopic bindings

- **Status**: Proposed (implementation of platform ADR-0022, accepted by the owner on 2026-10-07; independent review pending)
- **Date**: 2026-10-07
- **Deciders**: Osvaldo Andrade
- **Tracks**: Code Foundry CFP-070, customer issue #33; platform ADR-0022 contracts C1.1-C1.5 (revision after architecture review 1)

## Context

Workloads on a workload cluster need to publish to and consume from one tenant
QueueTopic served by the central codeQ. Before this change the producer routes
checked only authentication and tenant, `codeq:admin` or `role: ADMIN` reached
every administrator route, `GET /v1/codeq/tasks/:id[/result]` returned any task
to any authenticated token, `webhook` accepted any URL, and the worker owner
check compared only `task.WorkerID` with the token subject. The only credentials
available to such workloads were installation-wide static tokens.

Tikti will issue short-lived tokens scoped to one tenant and one topic. codeQ
must enforce that scope; it must not trust the request body over the token.

## Decision

A validated token is *binding-scoped* when it carries the `codeq_binding`
object claim. Every token that carries `codeq:publish` must be binding-scoped.
`internal/authclaims.ResolveBindingScope` accepts it only when all of these
hold, else every route answers 403 `{"error":"binding_scope_denied"}`:

- `codeq_binding` has exactly `uid` (1..128 printable characters), `generation`
  (integer >= 1), `policy` (`Publish` or `Subscribe`) and `topicId`;
- `tid` is the only tenant claim and is a valid tenant label;
- `eventTypes` has exactly one entry, a DNS label (never `*`), and
  `topicId == tid + "." + eventTypes[0]`;
- there is no `role`, `tenant_epoch` or `topic_controller` claim;
- `Publish`: `aud` is exactly `codeq-producer` and the scope set is exactly
  `{codeq:publish}`; `Subscribe`: `aud` is exactly `codeq-worker` and the scope
  set is exactly `{codeq:abandon, codeq:claim, codeq:heartbeat, codeq:nack,
  codeq:result}`;
- the subject starts with `codefoundry:workload:`;
- `exp` and `iat` are present and `exp - iat <= 300s`.

The producer, worker and any-token middlewares run the check right after
authentication and before tenant resolution, then apply one route allow-list
keyed on the registered route template and method. The status codes below are
the ones a client receives, given which group authenticates each route (the
producer validator accepts only `aud=codeq-producer`, the worker validator only
`aud=codeq-worker`, and `/admin` authenticates with the producer validator):

| Route | Publish | Subscribe |
|---|---|---|
| `POST /v1/codeq/tasks` | `command == eventTypes[0]`, no `webhook` | 401 (audience) |
| `POST /v1/codeq/tasks/batch` | every item checked before any enqueue; 403 with `index` | 401 |
| `GET /v1/codeq/tasks/:id`, `/:id/result` | tenant and command match, else 404 | same |
| `POST /v1/codeq/tasks/claim`, `/claim/batch` | 401 | commands subset of `eventTypes` |
| `POST /v1/codeq/tasks/:id/{heartbeat,abandon,nack,result}` | 401 | tenant, command and `WorkerID == sub`, else 403 `not-owner` |
| `POST /v1/codeq/tasks/batch/results` | 401 | per item, refused items get `not-owner` |
| `POST /v1/codeq/workers/subscriptions[/:id/heartbeat]` | 401 (worker audience; never promoted) | 403 `route_not_allowed` |
| `GET /v1/codeq/raft/status` | allowed | allowed |
| `/v1/codeq/admin/**` | 403 `route_not_allowed` before `RequireAdmin` | 401 |
| worker and producer gRPC streams | `PermissionDenied route_not_allowed` | same |

`ALLOW_PRODUCER_AS_WORKER` never promotes a binding-scoped or `codeq:publish`
token. A binding-scoped token never takes a role from the dev `X-Role` header.

Independently of token kind, `GET /v1/codeq/tasks/:id` and
`GET /v1/codeq/tasks/:id/result` now return a task only when its `tenantId`
equals the token's resolved tenant (platform follow-up F1, done here). A
foreign task is answered exactly like a missing one (same status and body, no
long-poll): `GET /tasks/:id` gives 404 `{"error":"not found"}` and
`GET /tasks/:id/result` keeps its existing missing-task body, 404
`{"error":"task not found"}`. Platform ADR-0022 C1.2 writes
`{"error":"not found"}` for both; codeQ keeps the existing result body because
changing only the foreign case would create the existence oracle C1.2 forbids,
and changing the missing case would break existing clients.

### Idempotency keys (C1.2: no cross-tenant replay, no oracle with data)

The idempotency index used to be global: `idempo:<key>` with no tenant or
command. A task created by one tenant was returned, with payload and
`tenantId`, to any token of another tenant that sent the same
`idempotencyKey`. Now:

- **Replay is tenant-bound for every token kind.** When a key already maps
  to a task, every backend (Pebble, sharded Pebble, Redis, and the cluster
  router, which maps the owner's answer back) returns it only when
  `task.tenantId` equals the caller's resolved tenant, by exact match
  (including the empty legacy tenant). Otherwise `POST /v1/codeq/tasks`
  answers 409 `{"error":"idempotency_conflict"}` with no task, no ID and
  nothing enqueued; a `POST /tasks/batch` item gets
  `{"error":"idempotency_conflict"}` and no `task`; a producer-stream
  `CreateAck` gets `ok=false`, `errorMessage="idempotency_conflict"` and no
  `taskId`. Same-tenant replay is unchanged: the original task is returned
  even when the replayed request differs.
- **Binding keys are namespaced.** For a Publish binding token the stored key
  is exactly

  ```
  tid + "\x00" + eventType + "\x00" + idempotencyKey
  ```

  with `tid` and `eventType` taken from the verified token (both restricted
  to `[a-z0-9-]`, so never NUL). Two topics of one tenant never collide, a
  binding key never collides with another tenant's key, and the reviewer's
  probe (a binding token of another tenant reusing a key) creates its own task
  instead of seeing the foreign one. Every other token kind keeps the client
  key unchanged. As defence in depth, a create that returns to a binding token
  a task of another tenant or command is answered with the same 409.
- **NUL is refused in client keys** for every token kind (400
  `{"error":"invalid 'idempotencyKey'"}`; per item in a batch), so no caller
  can write a key inside a binding namespace.
- **Sharded Pebble.** With an idempotency key the new task ID is drawn on the
  key's shard (`shardOf(id) == shardOf(key)`), so the task and its index are
  written by one batch on the shard the replay reads. Previously, with more
  than one shard, the index landed on the task's shard and same-tenant replay
  could miss. One-shard installations are unaffected.
- **Observability.** `codeq_idempotency_conflict_total{route,kind}` (`kind` is
  `binding` or `token`) and one `codeq_idempotency_conflict` warning line with
  route, method, caller tenant, request ID and, for binding tokens, topic and
  binding UID; never the key, task ID, payload or token.

**Migration.** No data migration and no new key format for legacy tokens.
Existing `idempo:` entries keep their raw keys and expire with task retention:
static, admin and other non-binding tokens send the same raw key and still
replay within their own tenant; a cross-tenant hit on an existing entry now
answers 409 instead of the task. Binding keys stored before this change (none
in practice: Tikti does not issue binding tokens yet) would not be found under
the namespace, so a retry spanning the upgrade would create one new task. The
tenant check is effective once the node that executes the create (the Raft
leader after C1.5 forwarding) runs this version.

**Residual.** Legacy keys stay in one global namespace, so a non-binding token
of another tenant learns one bit — that the raw key exists — from the 409, but
never the task, its ID or its tenant. Namespacing legacy keys by tenant needs a
dual-read migration of existing entries and is follow-up work.

The JWKS validator enforces `exp` with zero leeway (`jwt.Parse` without
`WithLeeway`), so an issued token is usable for at most its 300 s lifetime;
the platform keeps 360 s as the conservative revocation bound (C1.4).

### Leader forwarding (platform ADR-0022 C1.5)

codeQ runs three Raft voters behind one ClusterIP Service. A follower used to
answer writes with HTTP 307 to the leader's in-cluster `RAFT_PEER_HTTP_ADDRS`
URL, which a workload cluster cannot resolve, and batch writes on a follower
failed per item or with 500. `internal/leaderforward` now forwards the request
in-process. No client ever receives 307.

- **Target.** Only a value of the configured `RAFT_PEER_HTTP_ADDRS` map, by
  exact string, other than this node's own URL. Malformed values (not an
  absolute `http`/`https` URL, or with userinfo, query or fragment) are
  excluded at startup. Any other hint, including an unknown leader during an
  election, gives 503 `{"error":"leader_unavailable"}` with `Retry-After: 1`.
- **Request.** Same method, escaped path and query, the buffered body, and
  only `Authorization`, `Content-Type`, the dev-only `X-Role` and the request
  ID, plus `X-CodeQ-Forwarded: 1`. The client never uses an environment proxy
  and never follows redirects; a 3xx answer is turned into 503. Only
  `Content-Type` and `Retry-After` are copied back.
- **Timeout.** 10 s, or `waitSeconds` (capped at 30, like the scheduler) + 10 s
  on `POST /tasks/claim`. Transport failure, timeout or a leader lost
  mid-request gives 503 `leader_unavailable`. A non-idempotent create whose
  forward timed out may have been applied; clients use `idempotencyKey`.
- **Loop prevention.** A request that already carries `X-CodeQ-Forwarded`
  is never forwarded again and gets 503 `leader_unavailable` with
  `Retry-After: 1`. The header carries no authority: the leader re-runs
  authentication, the binding allow-list and every controller rule.
- **Order.** The gate runs after authentication, the binding allow-list and
  worker scope checks (a follower never forwards an unauthenticated or
  route-refused request) and before rate limiting and the controller (the
  leader counts the request once).
- **Body.** Bodies up to 16 MiB are buffered for replay. A larger body that
  must be forwarded gets 413 `{"error":"request_too_large"}`; the leader
  applies no new limit. codeQ had no task-route body limit before, so this
  is the limit the platform ADR's "existing limits" resolves to.
- **Observability.** `codeq_leader_forward_total{route,result}` with `route`
  a registered template and `result` one of `ok`, `upstream_unavailable`,
  `loop`, `unconfigured`, `no_leader`, `body_too_large`, `timeout`,
  `canceled`, `error`, `redirect_refused`, `disabled`. One
  `codeq_leader_forward` log line per failed decision (debug on success) with
  route, method, result, status, leader URL, request ID and duration; never
  `Authorization`, the body or a token.

**Which Raft group decides.** There is one Raft group per Pebble shard.
`RAFT_MUX_ENABLED` only shares the Raft transport port; it does not change
the group set. Both installations run one shard with mux enabled, so every
route is governed by that single group. With `numShards > 1`:

| Route | Group touched | Gate before the controller | After the controller |
|---|---|---|---|
| `POST /tasks` | `hash(new task ID)`, unknown in advance | forward when one peer leads every group, else local | the exact group's not-leader hint is forwarded |
| `/tasks/:id/{heartbeat,abandon,nack,result}` | `hash(task ID)` | same | same |
| `PUT`/`DELETE /admin/topics/:name` | group 0 | same | same |
| `/tasks/claim`, `/tasks/claim/batch` | every group this node leads (fan-out) | local when this node leads at least one group; forward when one peer leads every group; else 503 | `/claim/batch` forwards only when nothing was claimed |
| `/tasks/batch`, `/tasks/batch/results` | per item, `hash(ID)` | same as claims, before any item | per-item results (legacy) |

So a batch on a node that leads no group is forwarded whole when one peer
leads every group and is refused with 503 (retryable, nothing processed) when
leadership is split; a node that leads at least one group keeps the legacy
per-item results, where items for groups it does not lead report `not leader`.
A forwarded single create can still meet a group led by a third node and then
gets 503, because the leader never forwards again. Single-shard installations
are not affected by these split-leadership cases.

Refusals increment `codeq_binding_scope_denied_total{reason,route}` (bounded
labels) and write one `codeq_binding_scope_denied` log line with code, reason,
route, request ID and, when known, tenant, topic, policy and binding UID. Tokens
and the `Authorization` header are never logged.

## Consequences

- Nothing issues binding-scoped tokens until Tikti enables its exchange, so the
  enforcement is unconditional and needs no flag.
- Static tokens, `codeq:admin`, `role: ADMIN` and the topic-controller scope
  behave as before on every route except the task reads.
- Behavior change: a token can no longer read a task of another tenant. The
  installation static producer and worker resolve to the platform tenant (for
  example `local-tenant`) and can read only that tenant's tasks; tasks they
  create, and tasks claimed with them, are in that tenant, so their own flows
  are unaffected. `codeq:admin` gains no cross-tenant read; aggregate
  administration stays on `/admin`.
- Rolling back codeQ while Tikti issues binding tokens would let an older codeQ
  treat `codeq:publish` as an unrestricted producer token: disable Tikti first
  and wait 360 s.
- Webhooks and subscriptions remain unrestricted for legacy tokens; general
  URL egress validation is follow-up work.
- Behavior change (idempotency): reusing another tenant's `idempotencyKey`
  answers 409 `idempotency_conflict` instead of returning that tenant's task,
  and a key containing NUL is refused with 400. Clients that shared keys
  across tenants must use distinct keys.
- Rolling back codeQ re-opens the global idempotency replay and drops the
  binding namespace (binding retries across the rollback create new tasks):
  disable Tikti binding issuance first, as above.
- Behavior change (C1.5): followers no longer answer 307. Clients that relied
  on following the redirect now get the leader's answer directly, or 503
  `leader_unavailable` with `Retry-After: 1`. Unknown-leader writes that
  answered 400 or 500 now answer that 503.
- Residual (C1.5): the bearer token crosses one more in-cluster HTTP hop,
  follower to leader, in the same trust zone and over the same transport as
  the existing edge to `codeq:8080` hop. It goes only to configured peers.

## Alternatives considered

- A feature flag in codeQ: rejected; the issuer flag already gates exposure and
  a codeQ flag would add a fail-open configuration.
- Enforcing scope in each controller only: rejected; the allow-list must refuse
  admin routes before `RequireAdmin` and must cover routes added later.

## References

- Code Foundry platform ADR-0022, contract C1.
- [ADR 0002](0002-tenant-claim-resolution.md), [platform topic authority](../platform-topic-authority.md).
