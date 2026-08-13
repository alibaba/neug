# ResultSet

`ResultSet` provides forward-only access to query results.

## Common Access Pattern

```java
try (Session session = driver.session()) {
    try (ResultSet rs = session.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age")) {
        while (rs.next()) {
            String name = rs.getString("name");
            long age = rs.getLong("age");
            System.out.println(name + ", " + age);
        }
    }
}
```

## Typed Getters

The Java driver exposes typed accessors for common value types:

- `getString(...)`
- `getInt(...)`
- `getLong(...)`
- `getBoolean(...)`
- `getDate(...)`
- `getTimestamp(...)`
- `getObject(...)`

## Access by Column Index

```java
try (ResultSet rs = session.run("RETURN 1 AS value")) {
    if (rs.next()) {
        long value = rs.getLong(0);
        Object raw = rs.getObject(0);
    }
}
```

## Null Handling

```java
Object value = rs.getObject(0);
boolean wasNull = rs.wasNull();
```

## Metadata

Each result set exposes metadata for column names and types:

```java
ResultSetMetaData metaData = rs.getMetaData();
```

## PROFILE and EXPLAIN

The Java driver also exposes PROFILE or EXPLAIN metadata through:

- `getProfileMetrics()`

### getProfileMetrics

Returns detailed PROFILE or EXPLAIN metrics as a `Map<String, Object>`:

```java
{
    "total_elapsed_ms" -> Double,
    "total_output_rows" -> Long,
    "operators" -> List<Map<String, Object>>
}
```

Each operator entry in `operators` has this structure:

```java
{
    "operator_id" -> Long,
    "parent_id" -> Long,
    "operator_name" -> String,
    "elapsed_ms" -> Double,
    "output_rows" -> Long,
    "child_ids" -> List<Long>
}
```

See also: [ResultSetMetaData](result_set_metadata), [Session](session)
