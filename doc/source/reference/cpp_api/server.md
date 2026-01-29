# Namespace server



## Classes and Structures

## BrpcHttpHandlerManager

**Full name:** `server::BrpcHttpHandlerManager`

**Public Methods:**

- `BrpcHttpHandlerManager(gs::NeugDB &graph_db)`
- `~BrpcHttpHandlerManager()`
- `Init(const ServiceConfig &config) override`
- `Start() override`
- `Stop() override`
- `RunAndWaitForExit() override`
- `IsRunning() const override`


---

## HttpRequest

**Full name:** `server::HttpRequest`

**Public Methods:**

- `HttpRequest()`
- `HttpRequest(::PROTOBUF_NAMESPACE_ID::internal::ConstantInitialized)`
- `HttpRequest(const HttpRequest &from)`
- `HttpRequest(HttpRequest &&from) noexcept`
- `operator=(const HttpRequest &from)`
- `operator=(HttpRequest &&from) noexcept`
- `Swap(HttpRequest *other)`
- `UnsafeArenaSwap(HttpRequest *other)`
- `New(::PROTOBUF_NAMESPACE_ID::Arena *arena=nullptr) const final`
- `CopyFrom(const HttpRequest &from)`
- ... and 8 more methods


---

## HttpResponse

**Full name:** `server::HttpResponse`

**Public Methods:**

- `HttpResponse()`
- `HttpResponse(::PROTOBUF_NAMESPACE_ID::internal::ConstantInitialized)`
- `HttpResponse(const HttpResponse &from)`
- `HttpResponse(HttpResponse &&from) noexcept`
- `operator=(const HttpResponse &from)`
- `operator=(HttpResponse &&from) noexcept`
- `Swap(HttpResponse *other)`
- `UnsafeArenaSwap(HttpResponse *other)`
- `New(::PROTOBUF_NAMESPACE_ID::Arena *arena=nullptr) const final`
- `CopyFrom(const HttpResponse &from)`
- ... and 8 more methods


---

## HttpService

**Full name:** `server::HttpService`

**Public Methods:**

- `~HttpService()`
- `Schema(::PROTOBUF_NAMESPACE_ID::RpcController *controlle...`
- `ServiceStatus(::PROTOBUF_NAMESPACE_ID::RpcController *controlle...`
- `CypherQuery(::PROTOBUF_NAMESPACE_ID::RpcController *controlle...`
- `GetDescriptor()`
- `CallMethod(const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor *...`
- `GetRequestPrototype(const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor *...`
- `GetResponsePrototype(const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor *...`
- `descriptor()`


---

## HttpServiceImpl

**Full name:** `server::HttpServiceImpl`

**Public Methods:**

- `HttpServiceImpl(gs::NeugDB &graph_db)`
- `~HttpServiceImpl()`
- `CypherQuery(google::protobuf::RpcController *cntl_base, const...`
- `ServiceStatus(google::protobuf::RpcController *cntl_base, const...`
- `Schema(google::protobuf::RpcController *cntl_base, const...`


---

## HttpService_Stub

**Full name:** `server::HttpService_Stub`

**Public Methods:**

- `HttpService_Stub(::PROTOBUF_NAMESPACE_ID::RpcChannel *channel)`
- `HttpService_Stub(::PROTOBUF_NAMESPACE_ID::RpcChannel *channel, ::P...`
- `~HttpService_Stub()`
- `channel()`
- `Schema(::PROTOBUF_NAMESPACE_ID::RpcController *controlle...`
- `ServiceStatus(::PROTOBUF_NAMESPACE_ID::RpcController *controlle...`
- `CypherQuery(::PROTOBUF_NAMESPACE_ID::RpcController *controlle...`


---

## IHttpHandlerManager

**Full name:** `server::IHttpHandlerManager`

**Public Methods:**

- `~IHttpHandlerManager()=default`
- `Init(const ServiceConfig &config)=0`
- `Start()=0`
- `Stop()=0`
- `RunAndWaitForExit()=0`
- `IsRunning() const =0`


---

## NeugDBService

**Full name:** `server::NeugDBService`

NeuG database HTTP service wrapper.

`NeugDBService` provides an HTTP interface layer for the NeuG graph database. It manages the lifecycle of a BRPC-based HTTP server that handles Cypher queries, service status requests, and schema quer...

**Public Methods:**

- `NeugDBService(gs::NeugDB &db)` - Constructs a service around an existing database instance
- `graph_db()` - Gets direct access to the underlying graph database
- `~NeugDBService()` - Destructor that ensures proper cleanup
- `init(const ServiceConfig &config)` - Initializes the service with configuration settings
- `Start()` - Starts the HTTP server
- `Stop()` - Stops the HTTP server gracefully
- `GetServiceConfig() const` - Retrieves the current service configuration
- `IsInitialized() const` - Checks if the service has been initialized
- `IsRunning() const` - Checks if the HTTP server is currently running
- `service_status()` - Gets current service status information
- ... and 1 more methods


---

## ServiceConfig

**Full name:** `server::ServiceConfig`

**Public Methods:**

- `ServiceConfig()`
- `set_sharding_mode(const std::string &mode)`
- `get_exclusive_shard_id() const`
- `get_cooperative_shard_num() const`


---

