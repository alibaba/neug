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

Create a node with Label type "Person", specifying the property names, types, and primary key for the Person.

```
CREATE NODE TABLE Person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

By default, if a Person type already exists in the database, an error will be reported. Use IF NOT EXISTS to avoid errors - it will only create if the type doesn't exist in the database, otherwise it will do nothing.

```
CREATE NODE TABLE IF NOT EXISTS Person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

## Create Edge Type

Create an edge of type "KNOWS" from Person to Person, specifying the property names and types for KNOWS. Currently, edges do not support specifying primary keys.

```
CREATE REL TABLE IF NOT EXISTS KNOWS (
    FROM Person TO Person,
    weight DOUBLE
);
```

**Multiplicity**

Optionally, you can add exactly one *multiplicity* token after the last column definition (and a comma), before the closing `)` of the `CREATE REL TABLE` header. It describes cardinality along the forward direction (from source to target). Allowed values are `ONE_TO_ONE`, `ONE_TO_MANY`, `MANY_TO_ONE`, and `MANY_TO_MANY`. If you omit it, the edge type uses `MANY_TO_MANY` by default.

For example, on the same `Person` / `KNOWS` / `weight` shape as above:

```
CREATE REL TABLE IF NOT EXISTS KNOWS (
    FROM Person TO Person,
    weight DOUBLE,
    MANY_TO_MANY
);
```

**Table options (`WITH`)**

You can append a `WITH ( … )` clause *after* the closing `)` of the table header. Inside the parentheses, pass one or more options as `name = value`, where values are literals. A common key is `sort_key_for_nbr`, whose value is typically a string literal naming an edge property used for ordering. The clause is optional.

Example, still with `Person`, `KNOWS`, and `weight` only—here `weight` is used as the sort column name:

```
CREATE REL TABLE IF NOT EXISTS KNOWS (
    FROM Person TO Person,
    weight DOUBLE
) WITH (sort_key_for_nbr = 'weight');
```

**Where multiplicity and options apply**

Multiplicity and `WITH` options are defined at **edge type** scope (the edge name and its shared column definitions), not at the level of an individual source–edge–target triplet. When a rel table declares multiple `FROM … TO …` entries for the same edge type, a single multiplicity value and a single option set apply uniformly to every such pair; per-pair multiplicity or option bindings are not supported.

## Drop Node Type

Delete a specified Node type. Use IF EXISTS to avoid errors when the type doesn't exist.

```
DROP TABLE IF EXISTS Person;
```

## Drop Edge Type

Delete a specified Edge type. Use IF EXISTS to avoid errors when the type doesn't exist.

```
DROP TABLE IF EXISTS KNOWS;
```

## Rename Node or Edge Type

Rename a node or edge type by `RENAME TO`.

```
ALTER TABLE Person RENAME TO Person2;
ALTER TABLE KNOWS RENAME TO KNOWS2;
```

## Add Property

Add properties to a node or edge type.

```
ALTER TABLE Person ADD IF NOT EXISTS gender INT32;
ALTER TABLE KNOWS ADD IF NOT EXISTS info STRING;
```

## Drop Property

Remove properties from a node or edge type.

```
ALTER TABLE Person DROP IF EXISTS gender;
ALTER TABLE KNOWS DROP IF EXISTS info;
```

## Rename Property

Rename properties of a node or edge type.

```
ALTER TABLE Person RENAME age TO age2;
ALTER TABLE KNOWS RENAME weight TO weight2;
```