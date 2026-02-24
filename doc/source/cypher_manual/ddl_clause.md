# DDL Clause

DDL (Data Definition Language) is a set of operations specifically designed for schema management. NeuG supports operations for adding, deleting, and modifying schema nodes, edges, and properties. When creating property related schema, users can optionally specify default values for properties to prevent `NULL` fields during data ingestion. If a default value is not explicitly provided, the system will automatically assign the defined default value.

The following table lists the recommended syntax for defining default values for each supported data type, along with the system-assigned default value used when no explicit default is provided.

| Data Type         | Default Value Example                     | System Default Value               |
|-------------------|-------------------------------------------|------------------------------------|
| `INT32`           | `prop INT32 DEFAULT 0`                    | `0`                                |
| `INT64`           | `prop INT64 DEFAULT 0`                    | `0`                                |
| `UINT32`          | `prop UINT32 DEFAULT 0`                   | `0`                                |
| `UINT64`          | `prop UINT64 DEFAULT 0`                   | `0`                                |
| `DOUBLE`          | `prop DOUBLE DEFAULT 0.0`                 | `0.0`                              |
| `FLOAT`           | `prop FLOAT DEFAULT 0.0`                  | `0.0`                              |
| `STRING`          | `prop STRING DEFAULT ''`                  | `''` (empty string)                |
| `DATE`            | `prop DATE DEFAULT DATE('1970-01-01')`    | `DATE('1970-01-01')`               |
| `TIMESTAMP`       | `prop TIMESTAMP DEFAULT TIMESTAMP('1970-01-01')` | `TIMESTAMP('1970-01-01')`    |
| `INTERVAL`        | `prop INTERVAL DEFAULT INTERVAL('0 year 0 month 0 day')` | `INTERVAL('0 year 0 month 0 day')`         |

Please refer to the following examples for more usages.

## Create Node Type

Create a node with Label type "person", specifying the property names, types, and primary key for the person.

```
CREATE NODE TABLE person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

By default, if a person type already exists in the database, an error will be reported. Use IF NOT EXISTS to avoid errors - it will only create if the type doesn't exist in the database, otherwise it will do nothing.

```
CREATE NODE TABLE IF NOT EXISTS person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

## Create Edge Type

Create an edge of type "knows" from person to person, specifying the property names and types for knows. Currently, edges do not support specifying primary keys.

```
CREATE REL TABLE IF NOT EXISTS knows (
    FROM person TO person,
    weight DOUBLE
);
```

## Drop Node Type

Delete a specified Node type. Use IF EXISTS to avoid errors when the type doesn't exist.

```
DROP TABLE IF EXISTS person;
```

## Drop Edge Type

Delete a specified Edge type. Use IF EXISTS to avoid errors when the type doesn't exist.

```
DROP TABLE IF EXISTS knows;
```

## Rename Node or Edge Type

Rename a node or edge type by `RENAME TO`.

```
ALTER TABLE person RENAME TO person2;
ALTER TABLE knows RENAME TO knows2;
```

## Add Property

Add properties to a node or edge type.

```
ALTER TABLE person ADD IF NOT EXISTS gender INT32;
ALTER TABLE knows ADD IF NOT EXISTS info STRING;
```

## Drop Property

Remove properties from a node or edge type.

```
ALTER TABLE person DROP IF EXISTS gender;
ALTER TABLE knows DROP IF EXISTS info;
```

## Rename Property

Rename properties of a node or edge type.

```
ALTER TABLE person RENAME age TO age2;
ALTER TABLE knows RENAME weight TO weight2;
```