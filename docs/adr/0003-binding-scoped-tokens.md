# ADR 0003: Enforce binding-scoped tokens for workload-cluster QueueTopic bindings

- **Status**: Proposed (implementation of platform ADR-0022, accepted by the owner on 2026-10-07; independent review pending)
- **Date**: 2026-10-07
- **Deciders**: Osvaldo Andrade
- **Tracks**: Code Foundry CFP-070, customer issue #33; platform ADR-0022 contract C1

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
keyed on the registered route template and method:

| Route | Publish | Subscribe |
|---|---|---|
| `POST /v1/codeq/tasks` | `command == eventTypes[0]`, no `webhook` | 401 (audience) |
| `POST /v1/codeq/tasks/batch` | every item checked before any enqueue; 403 with `index` | 401 |
| `GET /v1/codeq/tasks/:id`, `/:id/result` | tenant and command match, else 404 | same |
| `POST /v1/codeq/tasks/claim`, `/claim/batch` | 401 | commands subset of `eventTypes` |
| `POST /v1/codeq/tasks/:id/{heartbeat,abandon,nack,result}` | 401 | tenant, command and `WorkerID == sub`, else 403 `not-owner` |
| `POST /v1/codeq/tasks/batch/results` | 401 | per item, refused items get `not-owner` |
| `POST /v1/codeq/workers/subscriptions[/:id/heartbeat]` | 403 `route_not_allowed` | 403 `route_not_allowed` |
| `GET /v1/codeq/raft/status` | allowed | allowed |
| `/v1/codeq/admin/**` | 403 `route_not_allowed` before `RequireAdmin` | 401 |
| worker and producer gRPC streams | `PermissionDenied route_not_allowed` | same |

`ALLOW_PRODUCER_AS_WORKER` never promotes a binding-scoped or `codeq:publish`
token. A binding-scoped token never takes a role from the dev `X-Role` header.

Independently of token kind, `GET /v1/codeq/tasks/:id` and
`GET /v1/codeq/tasks/:id/result` now return a task only when its `tenantId`
equals the token's resolved tenant. A foreign task is answered exactly like a
missing one (same status and body, no long-poll).

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

## Alternatives considered

- A feature flag in codeQ: rejected; the issuer flag already gates exposure and
  a codeQ flag would add a fail-open configuration.
- Enforcing scope in each controller only: rejected; the allow-list must refuse
  admin routes before `RequireAdmin` and must cover routes added later.

## References

- Code Foundry platform ADR-0022, contract C1.
- [ADR 0002](0002-tenant-claim-resolution.md), [platform topic authority](../platform-topic-authority.md).
