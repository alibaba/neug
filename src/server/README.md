# Service architecture

`NeugDBService` owns the complete service lifecycle. Its implementation owns a
`TpServiceRuntime` and an `IServiceTransport`; neither HTTP nor BRPC types appear
in the transport contract. The public service constructor selects the existing
BRPC backend directly in `neug_db_service.cc`.

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
count. The transport receives only host and port, while the runtime owns database
capacity, transaction limits, timeouts, and compaction settings.

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
listener or active callback. `StopAndJoin()` is idempotent and returns only when
handlers can no longer call `ITpOperations`. Restart is supported.

The facade stops in this order:

1. Close admission for new explicit transactions.
2. Drain explicit transactions, waiting for their active operations and releasing
   locks needed by blocked auto-commit requests. Queued transaction operations
   may fail because their session has been closed.
3. Stop accepting network requests and join active callbacks.
4. Stop compaction.
5. Publish the stopped state and notify blocking callers.

Blocking exit waiting belongs to the facade, not to the transport. A guard rejects
new starts until an older blocking caller has completed its cleanup, even if
another thread has already stopped the server.

## Current backend dependencies

The facade preserves `brpc::IsAskedToQuit()` behavior. The quit flag is
process-wide, not a per-service stop flag; `Stop()` must not run directly inside
an asynchronous signal handler. Signal tests therefore use separate processes.

The runtime still installs `BthreadRuntimeWait` and initializes bthread capacity.
Adding a native-thread backend requires selecting and validating an appropriate
runtime wait strategy as well as changing transport construction. This refactor
does not claim to remove the build dependency on BRPC/bthread.

`ITpOperations` also retains existing JSON/protobuf result encodings. In particular,
explicit-transaction serialization remains inside execution so serialization
failure can mark the transaction for rollback.

## Validation

`test_db_svc` covers HTTP queries, transaction behavior, concurrency limits,
startup failure, shutdown, and restart. A private test factory injects a transport
without a listener or wait API to verify that facade lifecycle and transaction
drain ordering do not depend on BRPC server methods. Public construction and
Python/Node service entry points remain unchanged.

## Source compatibility

The public `NeugDBService` constructor and methods are unchanged. This refactor
removes the installed `neug/server/brpc_service_mgr.h` header and `IServiceManager`:
code that directly used `BrpcServiceManager`, `HttpServiceImpl`, or the old
protocol registry must migrate. These removals are a C++ source compatibility
change, even though no in-repository caller remains.

`ServiceConfig` now lives in `neug/server/service_config.h`; direct includes of
`neug/utils/service_manager.h` must use the new path.

Applications should use `NeugDBService`. New transport implementations implement
`IServiceTransport` and are assembled in `neug_db_service.cc`. BRPC handlers are
backend implementation details; the old registry is removed. No legacy
compatibility wrapper is provided in this change.
