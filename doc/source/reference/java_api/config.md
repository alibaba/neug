# Config

`Config` is used to customize Java driver behavior such as connection and timeout settings.

## Purpose

Use `Config` when you want to adjust driver-level HTTP behavior without changing application query code.

Typical use cases include:

- shorter connection timeouts in tests
- longer read timeouts for heavy queries
- tuning connection pool settings for service workloads

## Basic Example

```java
import com.alibaba.neug.driver.Driver;
import com.alibaba.neug.driver.GraphDatabase;
import com.alibaba.neug.driver.utils.Config;

Config config = Config.builder()
        .withConnectionTimeoutMillis(3000)
        .build();

Driver driver = GraphDatabase.driver("http://localhost:10000", config);
```

## Common Options

Depending on the driver version, `Config.Builder` can be used to tune:

- connection timeout
- read timeout
- write timeout
- connection pool size
- keep-alive settings

## Usage Notes

- Create `Config` once and reuse it when constructing drivers
- Keep timeout values consistent with your deployment environment
- Prefer conservative defaults unless you have a specific performance reason to tune them

See also: [Driver](driver)
