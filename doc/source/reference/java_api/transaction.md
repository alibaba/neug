# Transaction

`Transaction` represents one explicit transaction created by a [Session](session). It keeps multiple Cypher statements in the same server-side transaction until the application commits or rolls them back.

## Start a Transaction

`beginTransaction()` starts a read-write transaction by default:

```java
try (Session session = driver.session();
        Transaction txn = session.beginTransaction()) {
    txn.run("CREATE (:Person {id: 1, name: 'Alice'})").close();
    txn.commit();
}
```

Select an access mode when the transaction must be read-only:

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

## Run Statements

Use `run(String)` for a statement without parameters, or `run(String, Map<String, Object>)` for a parameterized statement:

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

Close every returned `ResultSet` after consuming it. While a transaction is active, execute its statements through `Transaction.run(...)`; the owning `Session` rejects direct `Session.run(...)` calls and cannot start another transaction.

## Commit and Rollback

Call `commit()` to make all changes permanent. If application work fails, call `rollback()` before propagating the error:

```java
try (Session session = driver.session();
        Transaction txn = session.beginTransaction()) {
    txn.run("CREATE (:Person {id: 1})").close();
    txn.run("CREATE (:Person {id: 2})").close();
    txn.commit();
}
```

`close()` automatically rolls back an active or rollback-only transaction. Try-with-resources therefore provides a safe fallback, but applications should still call `commit()` explicitly on the successful path.

## Transaction State

| State | `run(...)` | `commit()` | `rollback()` | `isOpen()` |
|---|---|---|---|---|
| Active | Allowed | Allowed | Allowed | `true` |
| Rollback-only | Rejected | Rejected | Allowed | `true` |
| Rollback-only after HTTP 409 | Rejected | Rejected | Allowed | `true` |
| Closed after HTTP 410 | Rejected | Rejected | Rejected | `false` |
| Closed after commit/rollback | Rejected | Rejected | Rejected | `false` |
| Commit or rollback outcome unknown | Rejected | Rejected | Rejected | `false` |

A failed statement makes the transaction rollback-only. Roll it back before reusing the session.

If commit or rollback returns HTTP 409, an operation may still be running and the server retains the
transaction. The transaction becomes rollback-only so rollback can be retried. HTTP 410 confirms
that the transaction has expired or no longer exists, so the transaction is closed and the owning
session can be reused. Other HTTP failures leave the outcome unknown.

If the connection fails while committing or rolling back, the final server-side outcome may be unknown. The driver does not transparently retry requests because replaying an operation whose response was lost may execute it twice. Close the owning session and create a new session; the server-side transaction deadline provides final resource reclamation.

## API Summary

- `ResultSet run(String statement)` executes a statement in this transaction.
- `ResultSet run(String statement, Map<String, Object> parameters)` executes a parameterized statement.
- `void commit()` commits an active transaction.
- `void rollback()` rolls back an active or rollback-only transaction.
- `boolean isOpen()` reports whether the transaction can still be rolled back.
- `void close()` rolls back automatically when the transaction is still open.

`Transaction` and its owning `Session` are not thread-safe. Do not execute concurrent requests through the same transaction.

See also: [Session](session), [ResultSet](result_set)
