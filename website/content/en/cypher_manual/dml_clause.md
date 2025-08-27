# DML Clause

DML (Data Manipulation Language) provides operations for data insertion, deletion, and modification in graph databases. Neug supports both bulk data operations (such as COPY FROM) and individual data operations (such as CREATE, SET, and DELETE). This document provides examples and explanations for each operation type.

## COPY FROM

The COPY FROM command allows you to bulk load data from external data sources and construct vertices and edges in the graph storage. Currently supported data sources include CSV formats.

### Loading Vertex Data

Load person vertex data from a CSV file. Each row in the CSV maps to a vertex, with columns corresponding to the vertex properties defined in the person schema.

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

Load knows edge data from a CSV file. The first two columns specify the primary keys of source and target vertex, while additional columns define edge properties.

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

The CREATE clause is used to insert new vertices and relationships into the graph.

### Creating Vertices

Create new vertices with specified properties. If a vertex with the same primary key already exists, an error will be reported.

```cypher
CREATE (a:person {name: 'taylor', age: 25}), (b:person {name: 'julie', age: 30})
```

### Creating Vertices and Relationships

Create vertices and relationships in a single statement. This is useful when you need to create both the vertices and the relationship between them.

```cypher
CREATE (a:person {name: 'mars', age: 28})-[:knows {weight: 16.0}]->(b:person {name: 'jennie', age: 26})
```

### Creating Relationships Between Existing Vertices

First match existing vertices, then create a relationship between them.

```cypher
MATCH (a:person {name: 'taylor'}), (b:person {name: 'julie'})
CREATE (a)-[:knows {weight: 20.0}]->(b)
```

## SET

The SET clause is used to update properties of existing vertices and relationships.

### Updating Vertex Properties

Update properties of a specific vertex.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
SET a.age = 37, a.city = 'New York'
RETURN a.*
```

### Updating Relationship Properties

Update properties of a specific relationship.

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
SET k.weight = 10.0, k.since = '2023-01-01'
RETURN k.*
```

## DELETE

The DELETE clause is used to remove vertices and relationships from the graph.

### Deleting Vertices

Delete a vertex from the graph. By default, you can only delete vertices that have no relationships to avoid creating dangling edges.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DELETE a
```

### Deleting Vertices with Relationships (DETACH DELETE)

Use DETACH DELETE to forcibly delete a vertex and all its relationships. This prevents errors when trying to delete vertices that have existing relationships.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DETACH DELETE a
```

### Deleting Relationships

Delete specific relationships between vertices while keeping the vertices intact.

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
DELETE k
```
