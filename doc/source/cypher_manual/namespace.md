# Namespace

A Namespace is a named, reusable view of part of a graph. It lets you limit
queries and graph analysis to selected node types, relationship types, and
property conditions without copying the underlying data.

For example, if several business domains share the same `Entity` and `Product`
types, a Namespace can expose only the records whose `domain` property belongs
to one domain. Queries that use the Namespace cannot match records outside that
selection.

## Before you begin

The node and relationship types used by a Namespace must already exist. If you
want to isolate data by tenant, domain, or user group, first store that value in
a consistent property such as `domain` on the relevant data.

A Namespace contains:

- one or more node types, optionally filtered by node properties;
- one or more relationship triplets in the form
  `[source type, relationship type, destination type]`, optionally filtered by
  relationship properties.

Both endpoints of every relationship triplet must be included in the Namespace.

## Create a Namespace

Use `project_graph` with a name, a node definition, and a relationship
definition:

```cypher
CALL project_graph(
    'user1_subgraph',
    {
        'Entity': 'domain = "user1"',
        'Product': 'domain = "user1"'
    },
    {
        '[Entity, rel_ee, Entity]': '',
        '[Entity, rel_ep, Product]': ''
    }
);
```

An empty string means that no additional property filter is applied. To filter
relationships as well, provide a condition as the triplet's value:

```cypher
CALL project_graph(
    'recent_user1_subgraph',
    {'Entity': 'domain = "user1"'},
    {'[Entity, rel_ee, Entity]': 'created_at >= Date("2026-01-01")'}
);
```

If no property filters are needed, node types can be passed as a list:

```cypher
CALL project_graph(
    'social_graph',
    ['Person'],
    {'[Person, KNOWS, Person]': ''}
);
```

The Namespace name must be non-empty and unique. All referenced types and
properties must exist, and each filter must produce a Boolean result.

## Query a Namespace

Qualify a node or relationship type as `<namespace>.<type>` in a `MATCH` or
`OPTIONAL MATCH` pattern:

```cypher
MATCH (n:user1_subgraph.Entity)
RETURN n.uid, n.name;
```

The query returns only `Entity` nodes that satisfy the filter stored in
`user1_subgraph`. Your own `WHERE` clause is applied in addition to the
Namespace filter:

```cypher
MATCH (n:user1_subgraph.Entity)
WHERE n.status = 'active'
RETURN n.uid, n.name;
```

Relationships use the same qualification:

```cypher
MATCH (n:user1_subgraph.Entity)
      -[r:user1_subgraph.rel_ep]->
      (p:user1_subgraph.Product)
RETURN n.name, p.name, r.rel_type;
```

Relationship membership is based on the complete source–relationship–destination
triplet. This prevents an identically named relationship connecting other node
types from entering the result.

### Match all types in a Namespace

Use `<namespace>.*` to match any node or relationship type included in the
Namespace:

```cypher
MATCH (source:user1_subgraph.*)
      -[rel:user1_subgraph.*]->
      (target:user1_subgraph.*)
RETURN source, rel, target;
```

### Query the original graph

An unqualified type continues to refer to the original graph:

```cypher
MATCH (n:Entity)
RETURN n;
```

This query is not restricted by any Namespace.

## Inspect Namespaces

List the available Namespaces:

```cypher
CALL show_projected_graphs()
RETURN *;
```

Inspect the node types, relationship triplets, and filters of one Namespace:

```cypher
CALL projected_graph_info('user1_subgraph')
RETURN *;
```

## Drop a Namespace

Remove a Namespace when it is no longer needed:

```cypher
CALL drop_projected_graph('user1_subgraph');
```

Dropping a Namespace does not delete nodes or relationships from the original
graph.

## Scope and persistence

- Namespace-qualified labels are supported in `MATCH` and `OPTIONAL MATCH`
  patterns. They are not supported in `CREATE`, `MERGE`, or pattern
  expressions.
- A Namespace is saved with the database and remains available after the
  database is reopened.
- A Namespace is a view of existing data. Updates to the underlying properties
  can therefore change which records match its filters.
- Queries fail if the Namespace does not exist or if a qualified type is not
  part of that Namespace.
