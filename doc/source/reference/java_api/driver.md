# Driver

`Driver` is the main entry point for Java applications using NeuG. `Config`
customizes its connection, timeout, and HTTP client behavior.

## Responsibilities

- Create and own the underlying HTTP client
- Apply shared connection and timeout settings
- Verify server connectivity
- Create `Session` instances
- Manage driver lifecycle through `close()`

## Configuration

Create a `Config` when an application needs to change driver-level HTTP
behavior without changing its query code. Typical use cases include shorter
connection timeouts in tests, longer read timeouts for heavy queries, and
connection-pool tuning for service workloads.

Depending on the driver version, `Config.Builder` can configure:

- connection timeout
- read timeout
- write timeout
- connection pool size
- keep-alive settings

Create the configuration once and reuse it when constructing drivers. Keep
timeouts consistent with the deployment environment, and prefer the defaults
unless there is a specific reason to tune them.

## Create a Driver

```java
import com.alibaba.neug.driver.Driver;
import com.alibaba.neug.driver.GraphDatabase;

Driver driver = GraphDatabase.driver("http://localhost:10000");
```

## Create a Driver with Config

```java
import com.alibaba.neug.driver.Driver;
import com.alibaba.neug.driver.GraphDatabase;
import com.alibaba.neug.driver.utils.Config;

Config config = Config.builder()
        .withConnectionTimeoutMillis(3000)
        .build();

Driver driver = GraphDatabase.driver("http://localhost:10000", config);
```

## Verify Connectivity

```java
try (Driver driver = GraphDatabase.driver("http://localhost:10000")) {
    driver.verifyConnectivity();
}
```

## Open Sessions

```java
try (Driver driver = GraphDatabase.driver("http://localhost:10000")) {
    try (Session session = driver.session()) {
        // run queries here
    }
}
```

## Lifecycle Notes

- Reuse one `Driver` for multiple queries and sessions when possible
- Close the driver when the application shuts down
- `isClosed()` can be used to inspect driver state

See also: [Session](session.md)
