# ResultSetMetaData

`ResultSetMetaData` describes the columns returned by a query.

Unlike JDBC-oriented APIs, NeuG returns native driver `Types` instead of SQL type codes.

## Example

```java
ResultSetMetaData metaData = rs.getMetaData();
String columnName = metaData.getColumnName(0);
Types columnType = metaData.getColumnType(0);
String typeName = metaData.getColumnTypeName(0);
```

## Common Methods

- `getColumnCount(int)`
- `getColumnName(int)`
- `getColumnType(int)`
- `getColumnTypeName(int)`
- `isNullable(int)`
- `isSigned(int)`

## Why Native Types

The NeuG Java driver is not designed as a JDBC wrapper. Returning native `Types` makes it easier to:

- preserve NeuG-specific type information
- avoid lossy JDBC mappings
- build driver-native abstractions on top of metadata

## Example Dispatch

```java
Types type = rs.getMetaData().getColumnType(0);
if (type == Types.INT64) {
    long value = rs.getLong(0);
}
```

See also: [ResultSet](result_set)