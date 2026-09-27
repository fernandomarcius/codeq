# Platform topic controller authority

`codeq:topics:manage` is a reserved, narrow producer-token scope. It grants only
PUT/GET/DELETE `/v1/codeq/admin/topics/:topicName` and GET
`/v1/codeq/admin/queues/:command`. The authenticated `tid` remains the sole
tenant selector. It grants no task, result, worker, subscription, aggregate queue
listing or cleanup access, including when producer-as-worker compatibility is
configured. Mixed scope tokens carrying this scope are rejected.

Set `TOPIC_CONTROLLER_AUTHORITY_URL` only from trusted installation configuration.
Its exact path is `/v1/internal/codeq/topic-authority`. HTTPS is supported; HTTP
is permitted only for `tikti.codecloud-identity.svc.cluster.local` without a port
suffix, user info, query, fragment or encoded path. Each topic request sends the
same bearer token to this endpoint, bounded to two seconds and four KiB, with
no redirects or environment proxy. The response must name the same tenant and
signed `tenant_epoch`, current ACTIVE state and `codeq-topic-authority/v1`.
There is no cached positive authority result. Missing configuration, stale epoch,
retired tenant or unavailable authority denies the request. Legacy administrator
permissions retain their existing behavior.

Tikti owns issuance and live tenant authority. Installation configuration binds
its projected-token verifier to the observed MASTER issuer, cluster, namespace,
ServiceAccount and immutable ServiceAccount UID. Tokens are Pod-bound, short
lived and bound to a single tenant lifetime. No raw projected token, bearer token
or database/queue credential belongs in source, logs or release evidence.

## Readiness and upgrades

`GET /readyz/raft` is unauthenticated, data-free readiness metadata. It returns
`raft-readiness/v1`, `selfId`, `numGroups`, and `verifiedGroups` (zero-based shard
indices). A leader records a shard only after a fresh Raft VerifyLeader quorum
acknowledgement. A follower or partial local proof returns 503 with metadata;
a leader proving every local shard returns 200. Concurrent probes are bounded,
and the entire request has a two-second budget. This endpoint is not liveness.

The platform aggregates shard evidence from its fixed peer Services. Deploy the
new CodeQ image before requiring this endpoint. An old image without it becomes
NotReady, but TCP liveness/discovery remains unchanged and the platform does not
restart Pods to manufacture readiness. Network timeouts require separate live
investigation; this endpoint changes neither membership nor persistent data.
