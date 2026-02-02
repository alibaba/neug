# LDBC Small Dataset for NeuG

This is a small LDBC-like graph dataset converted to NeuG format for testing purposes.

## Dataset Overview

### Vertices
| Type | Count | Description |
|------|-------|-------------|
| person | 3 | People in the social network (P0, P1, P2) |
| comment | 1 | A comment on a post |
| post | 2 | Posts in the forum |

### Edges
| Type | Count | Description |
|------|-------|-------------|
| person_knows_person | 3 | Friendship relationships |
| comment_hasCreator_person | 1 | Comment authored by person |
| post_hasCreator_person | 2 | Post authored by person |

## Graph Structure

```
    P0 ──knows── P1
     │           │
   knows       knows
     │           │
     └─── P2 ───┘
     
comment(3) ──hasCreator── P0
post(4) ──hasCreator── P1
post(5) ──hasCreator── P1
```

## Files

### Schema & Configuration
- `graph.yaml` - Graph schema definition
- `import.yaml` - Data import configuration

### Data Files (CSV)
- `person.csv` - Person vertex data
- `comment.csv` - Comment vertex data
- `post.csv` - Post vertex data
- `person_knows_person.csv` - Knows relationship data
- `comment_hasCreator_person.csv` - Comment creator relationships
- `post_hasCreator_person.csv` - Post creator relationships

### Original Files (JSONL)
- `vertex_property_schema.jsonl` - Original vertex schema
- `vertex_property.jsonl` - Original vertex data
- `edge_property_schema.jsonl` - Original edge schema
- `edge_property.jsonl` - Original edge data
- `ldbc.graph` - Original graph structure

## Usage

### Method 1: YAML Configuration Import

Use the provided `graph.yaml` and `import.yaml` files with NeuG's bulk loader.

### Method 2: Cypher COPY Commands

```cypher
-- Create schema
CREATE NODE TABLE person(
    id INT32 PRIMARY KEY,
    firstName STRING,
    lastName STRING,
    gender STRING,
    birthday STRING,
    creationDate STRING,
    locationIP STRING,
    browserUsed STRING,
    language STRING,
    email STRING
);

CREATE NODE TABLE comment(
    id INT32 PRIMARY KEY,
    creationDate STRING,
    locationIP STRING,
    browserUsed STRING,
    content STRING,
    length INT32
);

CREATE NODE TABLE post(
    id INT32 PRIMARY KEY,
    imageFile STRING,
    creationDate STRING,
    locationIP STRING,
    browserUsed STRING,
    language STRING,
    content STRING,
    length INT32
);

CREATE REL TABLE person_knows_person(
    FROM person TO person,
    creationDate STRING
);

CREATE REL TABLE comment_hasCreator_person(FROM comment TO person);
CREATE REL TABLE post_hasCreator_person(FROM post TO person);

-- Import data
COPY person FROM "person.csv" (header=true, delimiter="|");
COPY comment FROM "comment.csv" (header=true, delimiter="|");
COPY post FROM "post.csv" (header=true, delimiter="|");

COPY person_knows_person FROM "person_knows_person.csv" (
    from="person", to="person", header=true, delimiter="|"
);
COPY comment_hasCreator_person FROM "comment_hasCreator_person.csv" (
    from="comment", to="person", header=true, delimiter="|"
);
COPY post_hasCreator_person FROM "post_hasCreator_person.csv" (
    from="post", to="person", header=true, delimiter="|"
);
```

### Verification Queries

```cypher
-- Count vertices
MATCH (p:person) RETURN count(p) AS person_count;
MATCH (c:comment) RETURN count(c) AS comment_count;
MATCH (po:post) RETURN count(po) AS post_count;

-- Count edges
MATCH ()-[k:person_knows_person]->() RETURN count(k) AS knows_count;

-- Sample query: Find all friends
MATCH (p1:person)-[k:person_knows_person]->(p2:person)
RETURN p1.firstName, p2.firstName, k.creationDate;

-- Find post creators
MATCH (po:post)-[:post_hasCreator_person]->(p:person)
RETURN po.id, p.firstName;
```
