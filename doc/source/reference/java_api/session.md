# Session

`Session` is the main query execution interface in the NeuG Java driver.

## Responsibilities

- Execute Cypher statements
- Send query parameters
- Select access mode when needed
- Return `ResultSet` objects for row-by-row reading
- Own one explicit transaction across multiple HTTP requests

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

## Explicit Transactions

```java
try (Session session = driver.session();
        Transaction txn = session.beginTransaction()) {
    try {
        txn.run("CREATE (:Person {id: 1, name: 'Alice'})").close();
        try (ResultSet rs = txn.run(
                "MATCH (n:Person {id: 1}) RETURN n.name AS name")) {
            while (rs.next()) {
                System.out.println(rs.getString("name"));
            }
        }
        txn.commit();
    } catch (RuntimeException e) {
        if (txn.isOpen()) {
            txn.rollback();
        }
        throw e;
    }
}
```

A statement failure makes the transaction rollback-only. The driver does not transparently retry requests after connection failures because replaying an operation whose response was lost may execute it twice. If a commit response is lost, the session treats the outcome as unknown; close that session and create another one. `close()` performs best-effort rollback for an active transaction, while the server-side absolute transaction deadline provides final resource reclamation.

## Usage Notes

- `Session` is lightweight and intended for short-lived use
- A session is not thread-safe and owns at most one explicit transaction
- Use try-with-resources to ensure it is closed cleanly
- Each `run(...)` call returns a `ResultSet` that should also be closed

See also: [Driver](driver), [Transaction](transaction), [ResultSet](result_set)
