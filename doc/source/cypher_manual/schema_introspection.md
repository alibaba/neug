# Schema Introspection

Schema Introspection procedures expose the schema of the current NeuG graph.
They can be used to discover node tables and relationship triplets, or to
inspect the properties declared on a particular table.

NeuG provides the following procedures:

- `SHOW_NODE_TABLES()` lists all node tables.
- `SHOW_REL_TABLE()` lists all relationship tables by source, relationship,
  and destination triplet.
- `SHOW_TABLE_INFO()` describes the properties of one node or relationship
  table.

All procedures are invoked with `CALL`. You can use `RETURN *` to return every
column, or name individual output columns in a `RETURN` clause.

## List Node Tables

Use `SHOW_NODE_TABLES()` to list the node tables in the current schema:

```cypher
CALL SHOW_NODE_TABLES() RETURN *;
```

The result contains one row per node table and is ordered by node label name.

| Column | Type | Description |
| --- | --- | --- |
| `vertex_label_name` | `STRING` | Node label name. |
| `primary_key` | `STRING` | Primary-key property name. |
| `temporary` | `BOOL` | `true` for a temporary node table and `false` for a persistent node table. |

For example, given persistent `Person` and `Company` node tables and a
temporary `TempPerson` node table, the result is similar to:

| vertex_label_name | primary_key | temporary |
| --- | --- | --- |
| Company | id | false |
| Person | id | false |
| TempPerson | id | true |

## List Relationship Tables

NeuG identifies a relationship table by its complete
`[source label, relationship label, destination label]` triplet. Use
`SHOW_REL_TABLE()` to list every valid relationship triplet in the current
schema:

```cypher
CALL SHOW_REL_TABLE() RETURN *;
```

The result is ordered by relationship label, source label, and destination
label.

| Column | Type | Description |
| --- | --- | --- |
| `edge_label_name` | `STRING` | Relationship label name. |
| `src_label_name` | `STRING` | Source node label name. |
| `dst_label_name` | `STRING` | Destination node label name. |
| `multiplicity` | `STRING` | Relationship multiplicity: `MANY_TO_MANY`, `ONE_TO_MANY`, `MANY_TO_ONE`, or `ONE_TO_ONE`. |
| `temporary` | `BOOL` | `true` for a temporary relationship table and `false` for a persistent relationship table. |
| `extra_options` | `STRING` | Additional relationship-table options encoded as a JSON object. |

For this relationship table:

```cypher
CREATE REL TABLE WorksAt(
    FROM Person TO Company,
    since INT32 DEFAULT 2000,
    role STRING,
    MANY_TO_ONE
) WITH (sort_key_for_nbr = 'since');
```

the corresponding row is:

| edge_label_name | src_label_name | dst_label_name | multiplicity | temporary | extra_options |
| --- | --- | --- | --- | --- | --- |
| WorksAt | Person | Company | MANY_TO_ONE | false | `{"sort_key_for_nbr":"since"}` |

When no additional options are set, `extra_options` is `{}`.

## Inspect Table Properties

Use `SHOW_TABLE_INFO()` with a node label to inspect a node table:

```cypher
CALL SHOW_TABLE_INFO('Person') RETURN *;
```

To inspect a relationship table, pass its complete triplet as a string. The
triplet syntax is the same as the relationship syntax used by
[Namespace](./namespace.md):

```cypher
CALL SHOW_TABLE_INFO('[Person, WorksAt, Company]') RETURN *;
```

Whitespace around the triplet and its elements is ignored. The source and
destination labels are significant, so reversing them identifies a different
relationship table.

The result preserves property declaration order and contains these columns:

| Column | Type | Description |
| --- | --- | --- |
| `property_name` | `STRING` | Property name. |
| `property_type` | `STRING` | NeuG property type, such as `INT64`, `VARCHAR`, or `BOOLEAN`. |
| `default_value` | `STRING` | Default declared in DDL, or the system default when DDL does not specify one. |
| `primary_key` | `BOOL` | Whether the property is part of the node table's primary key. Relationship properties return `false`. |

For example, given:

```cypher
CREATE NODE TABLE Person(
    id INT64 PRIMARY KEY,
    name STRING DEFAULT 'anonymous',
    age INT32,
    active BOOL DEFAULT true
);
```

`CALL SHOW_TABLE_INFO('Person') RETURN *;` returns:

| property_name | property_type | default_value | primary_key |
| --- | --- | --- | --- |
| id | INT64 | 0 | true |
| name | VARCHAR | anonymous | false |
| age | INT32 | 0 | false |
| active | BOOLEAN | true | false |

For the earlier `WorksAt` relationship table,
`CALL SHOW_TABLE_INFO('[Person, WorksAt, Company]') RETURN *;` returns:

| property_name | property_type | default_value | primary_key |
| --- | --- | --- | --- |
| since | INT32 | 2000 | false |
| role | VARCHAR |  | false |

`SHOW_TABLE_INFO()` requires one constant string argument. NeuG reports an
error if the node label does not exist, the triplet is malformed, or the exact
relationship triplet is not present in the current schema.
