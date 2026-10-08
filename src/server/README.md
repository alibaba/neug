# Service architecture

`NeugDBService` owns the complete service lifecycle. Its implementation owns a
`TpServiceRuntime` and an `IServiceTransport`; neither HTTP nor BRPC types appear
in the transport contract. The public constructor uses the default transport
factory to select BRPC without making the facade or runtime depend on its API.
Callers can supply a transport factory through the other public constructor;
the factory must leave request callbacks stopped until `Start()`.

```text
NeugDBService
  |-- TpServiceRuntime : ITpOperations
  |     |-- TpExecutionSlotPool
  |     |-- ServiceTransactionManager
  |     `-- background compaction
  `-- IServiceTransport
        `-- BrpcTransport
              |-- brpc::Server
              `-- BrpcHttpHandler --borrows--> ITpOperations
```

## Ownership and requests

The transport owns its server and protocol handlers. BRPC borrows the handlers;
its server is destroyed before those handlers. The transport is destroyed before
the runtime, so callbacks never refer to a destroyed business service.

`BrpcHttpHandler` parses HTTP requests, maps status codes and writes responses.
Its fixed HTTP codecs are internal functions, not a global protocol registry.
Additional BRPC protocols should register their own handlers with the server.
It calls `ITpOperations`, implemented by the runtime. The transport itself handles
listening and request draining. Only HTTP handlers are registered today. A future
protobuf RPC handler could share the same BRPC server and business interface;
no RPC endpoint or alternative networking library is implemented here.

`thread_num` limits the runtime's execution-slot pool. It is not the BRPC worker
count. The default backend sizes process-wide bthread capacity from the database
thread limit, while the runtime owns the service-local query limit, transaction
limits, timeouts, and compaction settings.

## Service contract types

`ITpOperations` defines request-facing business operations. `QueryRequest`,
`QueryResult`, and `TransactionMode` remain shared execution types in `main/`.
`ServiceTransactionInfo` in `server/service_transaction.h` contains the opaque
service transaction identifier and optional client-visible expiry. Both the
business interface and the internal transaction manager use this value type.
Admission, draining, and compaction controls remain on `TpServiceRuntime`.

## Lifecycle contract

The facade serializes transport lifecycle calls. `Start()` returns only after
listening succeeds, and returns the actual endpoint. Failed startup leaves no
listener or active callback. `StopAccepting()` closes the request entry point;
`Join()` waits until handlers can no longer call `ITpOperations`. Both are
idempotent. Restart is supported after `Join()`.

The facade stops in this order:

1. Close admission for new explicit transactions.
2. Stop accepting new transport requests and make `IsRunning()` return false.
3. Drain explicit transactions, waiting for their active operations and releasing
   locks needed by blocked auto-commit requests. Queued transaction operations
   may fail because their session has been closed.
4. Join active callbacks.
5. Stop compaction.
6. Notify blocking callers after cleanup completes.

Blocking exit waiting belongs to the facade, not to the transport. A guard rejects
new starts until an older blocking caller has completed its cleanup, even if
another thread has already stopped the server.

## Current backend dependencies

The facade asks the transport whether process shutdown has been requested.
`BrpcTransport` preserves `brpc::IsAskedToQuit()` behavior; its quit flag is
process-wide, not a per-service stop flag. `Stop()` must not run directly inside
an asynchronous signal handler. Signal tests therefore use separate processes.

The runtime installs the transport's scheduler wait callback in the version
manager and restores the native callback after callbacks and transactions
drain. The execution-slot pool uses a transport-provided synchronizer, so a
released slot wakes one waiter without polling or blocking a BRPC worker on a
native condition variable. `BrpcTransport` supplies bthread waiting and
synchronization; other transports use native defaults or provide their own.
BRPC and bthread remain build dependencies of the default backend.

## Validation

`test_db_svc` covers HTTP queries, transaction behavior, concurrency limits,
startup failure, shutdown, and restart. A private test factory injects a transport
without a listener or wait API to verify that facade lifecycle and transaction
drain ordering do not depend on BRPC server methods.
