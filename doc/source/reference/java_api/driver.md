# Driver

`Driver` is the main entry point for Java applications using NeuG.

## Responsibilities

- Create and own the underlying HTTP client
- Verify server connectivity
- Create `Session` instances
- Manage driver lifecycle through `close()`

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

See also: [Session](session)
