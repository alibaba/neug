# Session

`Session` is the main query execution interface in the NeuG Java driver.

## Responsibilities

- Execute Cypher statements
- Send query parameters
- Select access mode when needed
- Return `ResultSet` objects for row-by-row reading

## Basic Query Execution

```java
try (Session session = driver.session();
        ResultSet rs = session.run("RETURN 1 AS value")) {
    while (rs.next()) {
        System.out.println(rs.getLong("value"));
    }
}
```

## Parameterized Queries

```java
import java.util.Map;

try (Session session = driver.session()) {
    try (ResultSet rs = session.run(
            "MATCH (n) WHERE n.name = $name RETURN n.age AS age",
            Map.of("name", "marko"))) {
        while (rs.next()) {
            System.out.println(rs.getLong("age"));
        }
    }
}
```

## Access Modes

```java
import com.alibaba.neug.driver.utils.AccessMode;
import java.util.Map;

try (Session session = driver.session();
        ResultSet rs = session.run(
                "MATCH (n) WHERE n.age > $age RETURN n",
                Map.of("age", 30),
                AccessMode.READ)) {
    while (rs.next()) {
        System.out.println(rs.getObject("n"));
    }
}
```

## Usage Notes

- `Session` is lightweight and intended for short-lived use
- Use try-with-resources to ensure it is closed cleanly
- Each `run(...)` call returns a `ResultSet` that should also be closed

See also: [Driver](driver), [ResultSet](result_set)
