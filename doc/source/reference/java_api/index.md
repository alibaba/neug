# Java API Reference

The NeuG Java API provides a Java-native driver for connecting to NeuG servers, executing Cypher queries, and consuming typed query results.

```{toctree}
:maxdepth: 1
:hidden:

driver
config
session
result_set
result_set_metadata
```

## Overview

The Java driver is designed for application integration and service-side usage:

- **Create drivers** to connect to a NeuG server over HTTP
- **Open sessions** to execute Cypher queries
- **Read results** through a typed `ResultSet` API
- **Inspect metadata** using native NeuG `Types`

## Deployment Model

The current Java SDK supports **remote access over HTTP only**, i.e., [**service mode**](../../getting_started/getting_started.md#service-mode).

- **Supported**: connect to a running NeuG server with `GraphDatabase.driver("http://host:port")`
- **Not supported**: embedded/in-process database access from Java

If you need embedded access, use the C++ or Python APIs. The Java SDK should be treated as a client for an already running NeuG service.

## Installation

### Use from this repository

```bash
cd tools/java_driver
mvn clean install -DskipTests
```

### Add dependency in another Maven project

```xml
<dependency>
	<groupId>com.alibaba.neug</groupId>
	<artifactId>neug-java-driver</artifactId>
	<version>${neug.version}</version>
</dependency>
```

## Core Interfaces

- **[Driver](driver)** - manages connectivity and creates sessions
- **[Config](config)** - customizes connection and timeout behavior
- **[Session](session)** - executes statements against a NeuG server
- **[ResultSet](result_set)** - reads rows and typed values from query results
- **[ResultSetMetaData](result_set_metadata)** - inspects result column names, nullability, and native NeuG types

## Quick Start

```java
import com.alibaba.neug.driver.Driver;
import com.alibaba.neug.driver.GraphDatabase;
import com.alibaba.neug.driver.ResultSet;
import com.alibaba.neug.driver.Session;

public class Example {
	public static void main(String[] args) {
		try (Driver driver = GraphDatabase.driver("http://localhost:10000")) {
			driver.verifyConnectivity();

			try (Session session = driver.session()) {
				try (ResultSet rs = session.run("RETURN 1 AS value")) {
					while (rs.next()) {
						System.out.println(rs.getInt("value"));
					}
				}
			}
		}
	}
}
```

## Start a NeuG Server

Before using the Java SDK, start a NeuG HTTP server that exposes the query endpoint.
You can use either the C++ binary or the Python API to start the server.

### Option A: Start with Python

If you have the `neug` Python package installed, you can start the server directly from Python:

```python
from neug import Database

db = Database("/path/to/graph", mode="rw")
# Blocks until the process is killed (Ctrl+C or SIGTERM)
db.serve(port=10000, host="0.0.0.0", blocking=True)
```

To run non-blocking (e.g. inside a larger script):

```python
import time
from neug import Database

db = Database("/path/to/graph", mode="rw")
uri = db.serve(port=10000, host="0.0.0.0", blocking=False)
print("Server started at:", uri)

try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    db.stop_serving()
```


### Option B: Start with the C++ binary

#### 1. Build the server binary

From the repository root:

```bash
cmake -S . -B build -DBUILD_EXECUTABLES=ON -DBUILD_HTTP_SERVER=ON
cmake --build build --target rt_server -j
```

#### 2. Start the server

```bash
./build/bin/rt_server --data-path /path/to/graph --http-port 10000 --host 0.0.0.0 --shard-num 16
```

Common options:

- `--data-path`: path to the NeuG data directory
- `--http-port`: HTTP port for Java clients, default is `10000`
- `--host`: bind address, default is `127.0.0.1`
- `--shard-num`: shard number of actor system, default is `9`

> **Note:** Make sure all local connections are closed before calling `db.serve()`.
> Once the server is running, no new local connections are allowed until `db.stop_serving()` is called.

### Connect from Java

After the server is started via either option:

```java
Driver driver = GraphDatabase.driver("http://localhost:10000");
```

## Parameterized Queries

```java
import java.util.HashMap;
import java.util.Map;

Map<String, Object> parameters = new HashMap<>();
parameters.put("name", "Alice");
parameters.put("age", 30);

try (Session session = driver.session()) {
	String query = "CREATE (p:Person {name: $name, age: $age}) RETURN p";
	try (ResultSet rs = session.run(query, parameters)) {
		if (rs.next()) {
			System.out.println(rs.getObject("p"));
		}
	}
}
```

## Dependencies

The Java driver depends on the following libraries:

- OkHttp - HTTP client
- Protocol Buffers - response serialization
- Jackson - JSON processing
- SLF4J - logging facade

These dependencies are managed automatically by Maven.

## API Documentation

- <a href="apidocs/index.html">Generated Javadoc</a>

## Build Javadoc Locally

```bash
cd tools/java_driver
mvn -DskipTests javadoc:javadoc
```

The generated Javadoc is written to `tools/java_driver/target/site/apidocs`.