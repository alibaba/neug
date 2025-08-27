# DDL Clause

DDL (Data Definition Language) is a set of operations specifically designed for schema management. Neug supports operations for adding, deleting, and modifying schema nodes, edges, and properties. Please refer to the following usage examples.

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

## Create Rel Type

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

## Drop Rel Type

Delete a specified Relationship type. Use IF EXISTS to avoid errors when the type doesn't exist.

```
DROP TABLE IF EXISTS knows;
```

## Rename Node or Rel Type

Rename a node or relationship type by `RENAME TO`.

```
ALTER TABLE person RENAME TO person2;
ALTER TABLE knows RENAME TO knows2;
```

## Add Property

Add properties to a node or relationship type.

```
ALTER TABLE person ADD IF NOT EXISTS gender INT32;
ALTER TABLE knows ADD IF NOT EXISTS info STRING;
```

## Drop Property

Remove properties from a node or relationship type.

```
ALTER TABLE person DROP IF EXISTS gender;
ALTER TABLE knows DROP IF EXISTS info;
```

## Rename Property

Rename properties of a node or relationship type.

```
ALTER TABLE person RENAME age TO age2;
ALTER TABLE knows RENAME weight TO weight2;
```