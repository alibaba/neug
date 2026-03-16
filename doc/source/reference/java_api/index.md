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
	<version>1.0.0-SNAPSHOT</version>
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

			try (Session session = driver.session();
					ResultSet rs = session.run("RETURN 1 AS value")) {
				while (rs.next()) {
					System.out.println(rs.getInt("value"));
				}
			}
		}
	}
}
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

## Reference Pages

- [Driver](driver)
- [Config](config)
- [Session](session)
- [ResultSet](result_set)
- [ResultSetMetaData](result_set_metadata)

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