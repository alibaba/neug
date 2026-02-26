# DML Clause

DML (Data Manipulation Language) provides operations for data insertion, deletion, and modification in graph databases. NeuG supports both bulk data operations (such as COPY FROM) and individual data operations (such as CREATE, SET, and DELETE). This document provides examples and explanations for each operation type.

## COPY FROM

The COPY FROM command allows you to bulk load data from external data sources and construct nodes and edges in the graph storage. 
Refer to [Import Data](../../data_io/import_data) for more details.

### Loading Node Data

Load person node data from a CSV file. Each row in the CSV maps to a node, with columns corresponding to the node properties defined in the person schema.

**person.csv:**
```
name,age
marko,39
vadas,27
josh,32
peter,35
```

**Command:**
```cypher
COPY person FROM "person.csv"
```

### Loading Edge Data

Load knows edge data from a CSV file. The first two columns specify the primary keys of source and target node, while additional columns define edge properties.

**knows.csv:**
```
src_name,dst_name,weight
marko,josh,1.0
marko,vadas,0.5
josh,peter,0.8
```

**Command:**
```cypher
COPY knows FROM "knows.csv"
```

## CREATE

The CREATE clause is used to insert new nodes and edges into the graph.

### Creating Nodes

Create new nodes with specified properties. If a node with the same primary key already exists, an error will be reported.

```cypher
CREATE (a:person {name: 'taylor', age: 25}), (b:person {name: 'julie', age: 30})
```

### Creating Nodes and Edges

Create nodes and edges in a single statement. This is useful when you need to create both the nodes and the edge between them.

```cypher
CREATE (a:person {name: 'mars', age: 28})-[:knows {weight: 16.0}]->(b:person {name: 'jennie', age: 26})
```

### Creating Edges Between Existing Nodes

First match existing nodes, then create an edge between them.

```cypher
MATCH (a:person {name: 'taylor'}), (b:person {name: 'julie'})
CREATE (a)-[:knows {weight: 20.0}]->(b)
```

## SET

The SET clause is used to update properties of existing nodes and edges.

### Updating Node Properties

Update properties of a specific node.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
SET a.age = 37, a.city = 'New York'
RETURN a.*
```

### Updating Edge Properties

Update properties of a specific edge.

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
SET k.weight = 10.0, k.since = '2023-01-01'
RETURN k.*
```

## DELETE

The DELETE clause is used to remove nodes and edges from the graph.

### Deleting Nodes

Delete a node from the graph. By default, you can only delete nodes that have no edge to avoid creating dangling edges.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DELETE a
```

### Deleting Nodes with Edges (DETACH DELETE)

Use DETACH DELETE to forcibly delete a node and all its attached edges. This prevents errors when trying to delete nodes that have existing edges.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DETACH DELETE a
```

### Deleting Edges

Delete specific edges between nodes while keeping the nodes.

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
DELETE k
```
