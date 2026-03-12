# NeuG Java Driver Usage Guide

## Using in Other Projects


1. Install to local Maven repository:
```bash
cd tools/java_driver
mvn clean install -DskipTests
```

2. Add dependency to your project's `pom.xml`:
```xml
<dependency>
    <groupId>com.alibaba.neug</groupId>
    <artifactId>neug-java-driver</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```


## Usage Examples

### Basic Connection

```java

public class Example {
    public static void main(String[] args) {
        // Create driver
        Driver driver = GraphDatabase.driver("http://localhost:10000");
        
        try {
            // Verify connectivity
            driver.verifyConnectivity();
            
            // Create session
            try (Session session = driver.session()) {
                // Execute query
                try (ResultSet rs = session.run("MATCH (n) RETURN n LIMIT 10")) {
                    while (rs.next()) {
                        System.out.println(rs.getObject("n"));
                    }
                }
            }
        } finally {
            driver.close();
        }
    }
}
```

### Connection with Configuration

```java
import com.alibaba.neug.driver.*;
import com.alibaba.neug.driver.utils.*;

public class ConfigExample {
    public static void main(String[] args) {
        Config config = Config.builder()
            .withConnectionTimeoutMillis(3000)
            .build();
        
        Driver driver = GraphDatabase.driver("http://localhost:10000", config);
        
        try (Session session = driver.session()) {
            // Read-only query
            try (ResultSet rs = session.run("MATCH (n:Person) RETURN n.name, n.age")) {
                while (rs.next()) {
                    String name = rs.getString("n.name");
                    int age = rs.getInt("n.age");
                    System.out.println(name + ", " + age);
                }
            }
        } finally {
            driver.close();
        }
    }
}
```

### Parameterized Query

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
            System.out.println("Created: " + rs.getObject("p"));
        }
    }
}
```

## Dependencies

This driver depends on the following libraries:
- OkHttp 4.11.0 - HTTP client
- Protocol Buffers 4.29.6 - Serialization
- Jackson 2.15.2 - JSON processing
- SLF4J 2.0.7 - Logging interface

These dependencies are automatically managed by Maven.
