# Namespace

A **Namespace** is a named, reusable logical view over part of a graph. It restricts queries and graph analysis to selected node types, relationship types, and property conditions **without copying or materializing the underlying graph data**.

Namespaces can be used directly in Cypher queries to define a logical graph view. They are also used by the **GDS extension** as the input graph view for graph algorithms, allowing algorithms to run on a selected subset of the original graph.

NeuG provides a set of Projected Graph APIs, including `project_graph`, `show_projected_graphs`, `projected_graph_info`, and `drop_projected_graph`, to create and manage Namespaces.

Consider a graph that contains two node tables, `Entity` and `Product`, and two relationship tables, `rel_ee` and `rel_ep`:

```cypher
CREATE NODE TABLE Entity(
    uid STRING PRIMARY KEY,
    name STRING,
    description STRING,
    entity_type STRING,
    product STRING,
    authority INT64,
    kg_id STRING,
    embedding FLOAT[512],
    domain STRING
);
```

```cypher
CREATE NODE TABLE Product(
    name STRING PRIMARY KEY,
    uid STRING,
    description STRING,
    domain STRING
);
```

```cypher
CREATE REL TABLE rel_ee(
    FROM Entity TO Entity,
    rel_type STRING,
    content STRING
);
```

```cypher
CREATE REL TABLE rel_ep(
    FROM Entity TO Product,
    rel_type STRING,
    content STRING
);
```

In this example, both `Entity` and `Product` contain a `domain` property. The property identifies the logical domain or group to which a node belongs. 

> **Note:** `domain` is only an example property used in this document. It is **not** a special or reserved property required by Namespace, and users do not need to add a `domain` property to their schema before using Namespace. Any existing property can be used to define filtering conditions according to the application's data model.

For example:

```text
Entity A   domain = "user1"
Entity B   domain = "user1"
Entity C   domain = "user2"

Product X  domain = "user1"
Product Y  domain = "user2"
```

Multiple domains can therefore share the same physical node and relationship tables while exposing different logical subsets of the graph through Namespaces.

For example, a Namespace for `user1` can include only:

- `Entity` nodes where `domain = "user1"`;
- `Product` nodes where `domain = "user1"`;
- `rel_ee` relationships whose source and destination nodes are included in the Namespace;
- `rel_ep` relationships whose source `Entity` and destination `Product` are included in the Namespace.

The underlying `Entity`, `Product`, `rel_ee`, and `rel_ep` tables remain unchanged and are not duplicated.

A Namespace contains:

- one or more node types, optionally filtered by node properties;
- one or more relationship triplets in the form `[source type, relationship type, destination type]`, optionally filtered by relationship properties.

Both endpoint types of every relationship triplet must be included in the Namespace.

## Create a Namespace

Use `CALL project_graph` to create a named Namespace by specifying:

- the Namespace name;
- the node types to include, optionally with property filters;
- the relationship triplets to include, optionally with property filters.

For example, the following statement creates `user1_subgraph`:

```cypher
CALL project_graph(
    'user1_subgraph',
    {
        'Entity': 'n.domain = "user1"',
        'Product': 'n.domain = "user1"'
    },
    [
        '[Entity, rel_ee, Entity]',
        '[Entity, rel_ep, Product]'
    ]
);
```

This Namespace contains:

```text
user1_subgraph
├── Entity
│   └── domain = "user1"
├── Product
│   └── domain = "user1"
├── [Entity, rel_ee, Entity]
└── [Entity, rel_ep, Product]
```

Here, the node definition uses a map to associate each node type with its property filter:

```cypher
{
    'Entity': 'n.domain = "user1"',
    'Product': 'n.domain = "user1"'
}
```

As a result, only `Entity` and `Product` nodes whose `domain` is `user1` are included in the Namespace.

The relationship definition is specified as a list:

```cypher
[
    '[Entity, rel_ee, Entity]',
    '[Entity, rel_ep, Product]'
]
```

Using a list means that no additional relationship property filter is applied. Relationship membership is determined by the relationship triplet and the node sets selected by the Namespace.

For example:

```text
Entity A (user1) ──rel_ep──> Product X (user1)   ← included

Entity A (user1) ──rel_ep──> Product Y (user2)   ← excluded

Entity C (user2) ──rel_ep──> Product X (user1)   ← excluded
```

Therefore, even though no additional filter is defined on `rel_ep`, a relationship is included only when its source and destination nodes belong to the corresponding node sets in the Namespace.

To apply an additional relationship property filter, specify the relationship definitions as a map. For example:

```cypher
CALL project_graph(
    'user1_subgraph_with_rel_filter',
    {'Entity': 'n.domain = "user1"'},
    {'[Entity, rel_ee, Entity]': 'r.year > 2010'}
);
```

This Namespace contains `Entity` nodes whose `domain` is `user1`, and `rel_ee` relationships whose `year` is greater than `2010`. A relationship is included only when its relationship predicate and the node predicates on both endpoints are all satisfied.

Predicate expressions use the following variables:

- Use `n` to refer to the node in a node predicate, for example `n.domain = "user1"`.
- Use `r` to refer to the relationship in a relationship predicate, for example `r.year > 2010`.

### Supported Predicate Expressions

A Namespace predicate must return `BOOL`. Predicates support property
expressions, literals, dynamic parameters, and scalar operations composed from
those values. Supported scalar operations include:

- comparisons such as `=`, `<>`, `<`, `<=`, `>`, and `>=`;
- membership and string predicates such as `IN`, `CONTAINS`, `STARTS WITH`, and
  `ENDS WITH`;
- `CAST` expressions;
- logical combinations using `AND`, `OR`, and `NOT`.

Complex expression forms such as `CASE`, aggregate expressions, lambda
expressions, path expressions, and subqueries are not supported in Namespace
predicates.

## Query a Namespace

A Namespace can be selected for an entire query using:

```text
USE NAMESPACE <namespace>
```

For example:

```cypher
USE NAMESPACE user1_subgraph
MATCH (n:Entity)
RETURN n.uid, n.name;
```

Although the physical `Entity` table may contain data from multiple domains, this query only matches `Entity` nodes included in `user1_subgraph`:

```text
Entity
├── Entity A   domain = "user1"   ← matched
├── Entity B   domain = "user1"   ← matched
└── Entity C   domain = "user2"   ← excluded
```

### Combine Namespace Filters with Query Filters

A query can add its own `WHERE` conditions on top of the filters defined by the Namespace:

```cypher
USE NAMESPACE user1_subgraph
MATCH (n:Entity)
WHERE n.status = 'active'
RETURN n.uid, n.name;
```

Conceptually, both conditions are applied:

```text
Namespace filter:
    domain = "user1"

Query filter:
    status = "active"

Effective condition:
    domain = "user1" AND status = "active"
```

The Namespace defines the graph scope, while the query can further restrict data within that scope.

### Query Relationships

The selected Namespace also constrains relationships and their endpoints:

```cypher
USE NAMESPACE user1_subgraph
MATCH (n:Entity)-[r:rel_ep]->(p:Product)
RETURN n.name, p.name, r.rel_type;
```

The query only returns `rel_ep` relationships whose complete source–relationship–destination triplet belongs to the Namespace.

For example:

```text
Entity A (user1) ──rel_ep──> Product X (user1)   ← matched

Entity A (user1) ──rel_ep──> Product Y (user2)   ← excluded

Entity C (user2) ──rel_ep──> Product X (user1)   ← excluded
```

Relationship membership is defined by the complete:

```text
[source type, relationship type, destination type]
```

triplet rather than by the relationship type alone. This also prevents an identically named relationship type connecting other node types from being included unintentionally.

### Match All Types in a Namespace

Omit a label or relationship type to match any type included in the selected Namespace:

```cypher
USE NAMESPACE user1_subgraph
MATCH (source)-[rel]->(target)
RETURN source, rel, target;
```

For `user1_subgraph`, each unlabeled pattern element resolves to the node or relationship definitions registered in that Namespace. Explicit labels are also checked against the Namespace; using a label that is not part of it is an error.

This is useful when a query should operate on the entire logical subgraph rather than on a specific node or relationship type.

### Use Namespace with OPTIONAL MATCH

`USE NAMESPACE` also applies to `OPTIONAL MATCH`.

For example:

```cypher
USE NAMESPACE user1_subgraph
MATCH (a:Entity)
OPTIONAL MATCH (a)-[r]->(b)
RETURN a, r, b;
```

The first `MATCH` selects `Entity` nodes from `user1_subgraph`. The `OPTIONAL MATCH` then tries to find outgoing relationships and target nodes that are also included in the same Namespace.

If a matching relationship exists in the original graph but the relationship or target node is outside `user1_subgraph`, it does not satisfy the optional pattern. As with a regular `OPTIONAL MATCH`, the row from the preceding `MATCH` is preserved and the unmatched optional elements are returned as `NULL`.

For example:

```text
Entity A (user1) ──rel_ep──> Product X (user1)
    → r = rel_ep, b = Product X

Entity B (user1) ──rel_ep──> Product Y (user2)
    → r = NULL, b = NULL
```

In the second case, `Entity B` is still returned because it matched the required part of the query, but `Product Y` is outside `user1_subgraph`. Therefore, the `OPTIONAL MATCH` does not match that path and returns `NULL` for the optional pattern elements.

### Query the Original Graph

Namespace definitions do not change the original graph.

A query without `USE NAMESPACE` continues to refer directly to the original graph:

```cypher
MATCH (n:Entity)
RETURN n;
```

This query can return `Entity` nodes from all domains because it is not restricted by a Namespace.

For example:

```text
MATCH (n:Entity)
    ↓
Entity A   domain = "user1"
Entity B   domain = "user1"
Entity C   domain = "user2"

USE NAMESPACE user1_subgraph
MATCH (n:Entity)
    ↓
Entity A   domain = "user1"
Entity B   domain = "user1"
```

The Namespace selection is query-wide. It applies to every `MATCH` and `OPTIONAL MATCH`, including nested pattern expressions and subqueries; individual pattern elements cannot opt out or select a different Namespace.

## Inspect Namespaces

Use `show_projected_graphs` to list the Namespaces currently defined in the database:

```cypher
CALL show_projected_graphs()
RETURN *;
```

To inspect the node types, relationship triplets, and property filters of a specific Namespace, use `projected_graph_info`:

```cypher
CALL projected_graph_info('user1_subgraph')
RETURN *;
```

## Drop a Namespace

Use `drop_projected_graph` to remove a Namespace:

```cypher
CALL drop_projected_graph('user1_subgraph');
```

Dropping a Namespace only removes its logical definition. It does **not** delete or modify nodes and relationships in the original graph.

## Namespace Persistence

Namespaces are automatically persisted as part of the database. Once a Namespace is created, its definition remains available after the database is closed and reopened.

Users do not need to recreate Namespaces after reopening the database.

For example, after creating:

```cypher
CALL project_graph(
    'user1_subgraph',
    {
        'Entity': 'n.domain = "user1"',
        'Product': 'n.domain = "user1"'
    },
    [
        '[Entity, rel_ee, Entity]',
        '[Entity, rel_ep, Product]'
    ]
);
```

`user1_subgraph` remains available after the database is reopened and can be queried directly:

```cypher
USE NAMESPACE user1_subgraph
MATCH (n:Entity)
RETURN n;
```

## Automatic Data and Schema Updates

A Namespace is a **logical view** over the original graph rather than a materialized copy. As a result, Namespace queries automatically operate on the latest graph data and schema.

### Data Updates

A Namespace does not maintain a separate copy of graph data. When the underlying data changes, subsequent Namespace queries automatically operate on the latest data.

For example, if a new `Entity` node with `domain = "user1"` is inserted into the original graph, it automatically becomes visible through:

```cypher
USE NAMESPACE user1_subgraph
MATCH (n:Entity)
RETURN n;
```

There is no need to recreate or manually refresh `user1_subgraph`.

Similarly, if an existing node's `domain` property changes and no longer satisfies the Namespace filter, that node will no longer be returned by subsequent Namespace queries.

### Schema Updates

A Namespace also automatically reflects changes to the schema of the original graph.

For example, suppose `user1_subgraph` initially contains the following node types:

```text
[Entity, Product]
```

If the `Entity` node type is later dropped from the original graph, querying it through the Namespace:

```cypher
USE NAMESPACE user1_subgraph
MATCH (n:Entity)
RETURN n;
```

will automatically detect that `Entity` no longer exists and report the corresponding **Label not found** error.

Wildcard queries also operate against the latest schema. After `Entity` is removed, the following query:

```cypher
USE NAMESPACE user1_subgraph
MATCH (n)
RETURN n;
```

will correctly resolve the node types currently available in `user1_subgraph` as:

```text
[Product]
```

No explicit Namespace refresh or recreation is required after data or schema changes.

## Limitations

`USE NAMESPACE` is currently supported only for read-only graph matching operations:

- `MATCH`
- `OPTIONAL MATCH`

For example:

```cypher
USE NAMESPACE user1_subgraph
MATCH (n:Entity)
RETURN n;
```

`USE NAMESPACE` queries currently **do not support write operations such as `CREATE` or `MERGE`**.

For example, the following usage is not supported:

```cypher
USE NAMESPACE user1_subgraph
CREATE (n:Entity {...});
```

This restriction prevents a Namespace query from directly modifying the underlying graph, where the intended write semantics can otherwise be ambiguous.

A Namespace defines a logical query scope over the original graph rather than an independent writable graph.
