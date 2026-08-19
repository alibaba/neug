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

If no node property filters are required, node types can also be specified as a list:

```cypher
CALL project_graph(
    'social_graph',
    ['Person'],
    [
        '[Person, KNOWS, Person]'
    ]
);
```

In this form, `project_graph` projects the specified node and relationship types without applying additional property filters.

The Namespace name must be non-empty and unique. All referenced node types, relationship types, and properties must exist, and each property filter must evaluate to a Boolean result.

## Query a Namespace

A Namespace can be referenced in a query by qualifying a node or relationship type using:

```text
<namespace>.<type>
```

For example:

```cypher
MATCH (n:user1_subgraph.Entity)
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
MATCH (n:user1_subgraph.Entity)
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

Relationships use the same Namespace qualification:

```cypher
MATCH (n:user1_subgraph.Entity)
      -[r:user1_subgraph.rel_ep]->
      (p:user1_subgraph.Product)
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

Use `<namespace>.*` to match any node or relationship type included in a Namespace:

```cypher
MATCH (source:user1_subgraph.*)
      -[rel:user1_subgraph.*]->
      (target:user1_subgraph.*)
RETURN source, rel, target;
```

For `user1_subgraph`, `*` expands to the node and relationship definitions registered in that Namespace.

This is useful when a query should operate on the entire logical subgraph rather than on a specific node or relationship type.

### Use Namespace with OPTIONAL MATCH

Namespace-qualified types can also be used with `OPTIONAL MATCH`.

For example:

```cypher
MATCH (a:user1_subgraph.Entity)
OPTIONAL MATCH (a)-[r:user1_subgraph.*]->(b:user1_subgraph.*)
RETURN a, r, b;
```

The first `MATCH` selects `Entity` nodes from `user1_subgraph`. The `OPTIONAL MATCH` then tries to find outgoing relationships and target nodes that are also included in the same Namespace.

If a matching relationship exists in the original graph but the relationship or target node is outside `user1_subgraph`, it does not satisfy the Namespace-qualified optional pattern. As with a regular `OPTIONAL MATCH`, the row from the preceding `MATCH` is preserved and the unmatched optional elements are returned as `NULL`.

For example:

```text
Entity A (user1) ──rel_ep──> Product X (user1)
    → r = rel_ep, b = Product X

Entity B (user1) ──rel_ep──> Product Y (user2)
    → r = NULL, b = NULL
```

In the second case, `Entity B` is still returned because it matched the required part of the query, but `Product Y` is outside `user1_subgraph`. Therefore, the Namespace-qualified `OPTIONAL MATCH` does not match that path and returns `NULL` for the optional pattern elements.

### Query the Original Graph

Namespace definitions do not change the original graph.

An unqualified type continues to refer directly to the original graph:

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

MATCH (n:user1_subgraph.Entity)
    ↓
Entity A   domain = "user1"
Entity B   domain = "user1"
```

### Mix Qualified and Unqualified Pattern Elements

Different pattern elements in the same pattern can be independently namespace-qualified or unqualified.

For example, the following query restricts `a` to `Entity` nodes in `user1_subgraph`, while the relationship `r` and node `b` are not restricted by the Namespace:

```cypher
MATCH (a:user1_subgraph.Entity)-[r]->(b)
RETURN b;
```

Here, Namespace qualification applies only to the pattern element on which it is specified. Therefore:

* `a` must be an `Entity` node included in `user1_subgraph`;
* `r` can be any relationship matching the pattern;
* `b` can be any node connected by `r`, including nodes outside `user1_subgraph`.

This allows a query to start from or constrain specific parts of a pattern to a Namespace without requiring the entire pattern to operate within that Namespace.

However, **qualified and unqualified types cannot be mixed within the same pattern element**.

For example, the following usage is not supported:

```cypher
MATCH (n:user1_subgraph.Entity:Product)
RETURN n;
```

Here, `user1_subgraph.Entity` is Namespace-qualified while `Product` is unqualified, so the same node pattern mixes qualified and unqualified types.

Similarly, **types from different Namespaces cannot be mixed within the same pattern element**:

```cypher
MATCH (n:user1_subgraph.Entity:user2_subgraph.Product)
RETURN n;
```

A single pattern element must therefore use one consistent scope: either unqualified types or types qualified by the same Namespace.

This restriction applies independently to each node and relationship pattern element. Different elements in the overall pattern can still use different scopes.

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
MATCH (n:user1_subgraph.Entity)
RETURN n;
```

## Automatic Data and Schema Updates

A Namespace is a **logical view** over the original graph rather than a materialized copy. As a result, Namespace queries automatically operate on the latest graph data and schema.

### Data Updates

A Namespace does not maintain a separate copy of graph data. When the underlying data changes, subsequent Namespace queries automatically operate on the latest data.

For example, if a new `Entity` node with `domain = "user1"` is inserted into the original graph, it automatically becomes visible through:

```cypher
MATCH (n:user1_subgraph.Entity)
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
MATCH (n:user1_subgraph.Entity)
RETURN n;
```

will automatically detect that `Entity` no longer exists and report the corresponding **Label not found** error.

Wildcard queries also operate against the latest schema. After `Entity` is removed, the following query:

```cypher
MATCH (n:user1_subgraph.*)
RETURN n;
```

will correctly resolve the node types currently available in `user1_subgraph` as:

```text
[Product]
```

No explicit Namespace refresh or recreation is required after data or schema changes.

## Limitations

Namespace-qualified types are currently supported only for read-only graph matching operations:

- `MATCH`
- `OPTIONAL MATCH`

For example:

```cypher
MATCH (n:user1_subgraph.Entity)
RETURN n;
```

Namespace-qualified types are currently **not supported in write operations such as `CREATE` or `MERGE`**.

For example, the following usage is not supported:

```cypher
CREATE (n:user1_subgraph.Entity {...});
```

This restriction prevents Namespace-qualified syntax from directly modifying the underlying graph, where the intended write semantics can otherwise be ambiguous.

A Namespace defines a logical query scope over the original graph rather than an independent writable graph.
