# Session

`Session` is the main query execution interface in the NeuG Java driver. It can
execute individual statements or own one explicit `Transaction` across
multiple HTTP requests.

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

## Transactions

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

### Read-only transactions

`beginTransaction()` starts a read-write transaction by default. Select an
access mode when the entire transaction must be read-only:

```java
try (Session session = driver.session();
        Transaction txn =
                session.beginTransaction(Transaction.Mode.READ_ONLY);
        ResultSet result = txn.run("MATCH (n:Person) RETURN count(n) AS count")) {
    if (result.next()) {
        System.out.println(result.getLong("count"));
    }
    txn.commit();
}
```

The mode is fixed for the lifetime of the transaction:

- `Transaction.Mode.READ_WRITE` permits reads and writes and is the default.
- `Transaction.Mode.READ_ONLY` permits only read operations.

### Parameterized transaction statements

Use `Transaction.run(String)` for a statement without parameters, or
`Transaction.run(String, Map<String, Object>)` for a parameterized statement:

```java
try (Session session = driver.session();
        Transaction txn = session.beginTransaction()) {
    txn.run(
            "CREATE (:Person {id: $id, name: $name})",
            Map.of("id", 1, "name", "Alice"))
        .close();

    try (ResultSet result = txn.run(
            "MATCH (n:Person {id: $id}) RETURN n.name AS name",
            Map.of("id", 1))) {
        while (result.next()) {
            System.out.println(result.getString("name"));
        }
    }

    txn.commit();
}
```

Close every returned `ResultSet` after consuming it. While a transaction is
active, execute statements through `Transaction.run(...)`; the owning
`Session` rejects direct `Session.run(...)` calls and cannot start another
transaction.

### Commit, rollback, and state

Call `commit()` to make all changes permanent. Call `rollback()` when
application work fails. Closing an active or rollback-only transaction rolls
it back automatically, so try-with-resources provides a safe fallback.

| State | `run(...)` | `commit()` | `rollback()` | `isOpen()` |
|---|---|---|---|---|
| Active | Allowed | Allowed | Allowed | `true` |
| Rollback-only | Rejected | Rejected | Allowed | `true` |
| Rollback-only after HTTP 409 | Rejected | Rejected | Allowed | `true` |
| Closed after HTTP 410 | Rejected | Rejected | Rejected | `false` |
| Closed after commit/rollback | Rejected | Rejected | Rejected | `false` |
| Commit or rollback outcome unknown | Rejected | Rejected | Rejected | `false` |

A statement failure makes the transaction rollback-only. Roll it back before
reusing the session. If commit or rollback returns HTTP 409, an operation may
still be running and rollback can be retried. HTTP 410 confirms that the
transaction has expired or no longer exists.

The driver does not transparently retry after connection failures because
replaying an operation whose response was lost may execute it twice. If the
commit outcome is unknown, close the owning session and create a new one. The
server-side transaction deadline provides final resource reclamation.

### Transaction API

- `ResultSet run(String statement)` executes a statement in this transaction.
- `ResultSet run(String statement, Map<String, Object> parameters)` executes a parameterized statement.
- `void commit()` commits an active transaction.
- `void rollback()` rolls back an active or rollback-only transaction.
- `boolean isOpen()` reports whether the transaction can still be rolled back.
- `void close()` rolls back automatically while the transaction is still open.

## Usage Notes

- `Session` is lightweight and intended for short-lived use
- A session is not thread-safe and owns at most one explicit transaction
- Use try-with-resources to ensure it is closed cleanly
- Each `run(...)` call returns a `ResultSet` that should also be closed

See also: [Driver](driver.md), [ResultSet](result_set.md)
