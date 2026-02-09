# Getting Started

This guide will walk you through creating your first graph database, performing basic operations, and exploring both embedded and service modes.

## Prerequisites

Before you begin, make sure you have NeuG installed. If you haven't installed it yet, please follow the [installation guide](../../installation/installation).

## Database Storage Modes

### Persistent Database
- **Use case**: Production applications, data analysis, long-term storage
- **Durability**: Data survives application restarts

```python
# Persistent mode examples
# Make sure that the database directory exists and is writable by the user.
db_persistent = neug.Database("/path/to/database")
```

### In-Memory Database
- **Use case**: Temporary computations, testing, prototyping
- **Durability**: Data is lost when the process ends

```python
# Memory mode examples
db_memory = neug.Database("")
```

> **Note:** Currently, NeuG's in-memory mode creates a temporary database directory that is automatically cleaned up when the process exits.

## Connection Modes

NeuG provides two modes to access your database:

### Embedded Mode
Direct in-process access - easiest for single-user scenarios:

```python
import neug

# Create database and connect directly
db = neug.Database("/path/to/database")  # or "" for in-memory
conn = db.connect()

print("Connect to NeuG in embedded mode")

conn.close()
db.close()
```

### Service Mode
Network-based access - ideal for multi-user applications:

**Start the service:**
```python
import neug

# Start NeuG as a service
db = neug.Database("/path/to/database")
service = db.serve(host="localhost", port=10000)
```

**Connect from client:**
```python
from neug import Session

# Connect to the service
session = Session("http://localhost:10000/")

session.close()
```

## Basic Operations

The following operations work the same way regardless of which database mode (in-memory or persistent) and connection mode (embedded or service) you choose. Let's assume that we are using a persistent database in embedded mode for this example.

```python
import neug

# Create database and establish connection
db = neug.Database("/path/to/database")
conn = db.connect()
```

### Creating Nodes and Edges

Before inserting data, you need to define your graph schema with node and edge types:

```python
# Create node tables
conn.execute("""
    CREATE NODE TABLE Person(
        id INT64 PRIMARY KEY,
        name STRING,
        age INT64,
        email STRING
    )
""")

conn.execute("""
    CREATE NODE TABLE Company(
        id INT64 PRIMARY KEY,
        name STRING,
        industry STRING,
        founded_year INT64
    )
""")

# Create edge tables
conn.execute("""
    CREATE REL TABLE WORKS_FOR(
        FROM Person TO Company,
        position STRING,
        start_date DATE,
        salary DOUBLE
    )
""")

conn.execute("""
    CREATE REL TABLE KNOWS(
        FROM Person TO Person,
        since_year INT64,
        relationship_type STRING
    )
""")

print("Graph schema created successfully!")
```

### Inserting Data

Now let's add some data to our graph:

```python
# Insert nodes
conn.execute("""
    CREATE (p:Person {id: 1, name: 'Alice Johnson', age: 30, email: 'alice@example.com'})
""")

conn.execute("""
    CREATE (p:Person {id: 2, name: 'Bob Smith', age: 35, email: 'bob@example.com'})
""")

conn.execute("""
    CREATE (c:Company {id: 1, name: 'TechCorp', industry: 'Technology', founded_year: 2010})
""")

# Insert relationships
conn.execute("""
    MATCH (p:Person), (c:Company) WHERE p.id = 1 AND c.id = 1
    CREATE (p)-[:WORKS_FOR {position: 'Software Engineer', start_date: date('2020-01-15'), salary: 75000.0}]->(c)
""")

conn.execute("""
    MATCH (p1:Person {id: 2}), (p2:Person {id: 1})
    CREATE (p1)-[:KNOWS {since_year: 2018, relationship_type: 'colleague'}]->(p2)
""")

print("Data inserted successfully!")
```

### Querying Data

Let's explore your graph with some queries:

```python
# Simple node query
result = conn.execute("MATCH (p:Person) RETURN p.name, p.age")
for record in result:
    print(record)
    # ['Alice Johnson', 30]
    # ['Bob Smith', 35]

# Relationship query
result = conn.execute("""
    MATCH (p:Person)-[w:WORKS_FOR]->(c:Company)
    RETURN p.name, w.position, c.name
""")
for record in result:
    print(f"{record[0]} works as {record[1]} at {record[2]}")
    # Alice Johnson works as Software Engineer at TechCorp

# Complex pattern query
result = conn.execute("""
    MATCH (p1:Person)-[:KNOWS]->(p2:Person)-[:WORKS_FOR]->(c:Company)
    RETURN p1.name as person1, p2.name as person2, c.name as company
""")
for record in result:
    print(f"{record[0]} knows {record[1]} who works at {record[2]}")
    # Bob Smith knows Alice Johnson who works at TechCorp
```

### Close the connection and database

```python
conn.close()
db.close()
```


### Using builtin dataset

NeuG provides several builtin datasets that you can use to quickly get started with graph analysis, learning, or testing. These datasets are ready-to-use and require no setup.

#### Available Datasets

You can list all available builtin datasets:

```python
from neug.datasets import get_available_datasets

# List all available datasets
datasets = get_available_datasets()
for dataset in datasets:
    print(f"{dataset.name}: {dataset.description}")
```

#### Loading Builtin Datasets

```python
import neug

# Open/create a database
db = neug.Database("/path/to/database")

# Load a builtin dataset into it
db.load_builtin_dataset(dataset_name="modern_graph")
```

> **Note:** Loading a builtin dataset into an existing database will fail if there are schema conflicts (e.g., existing node/edge types with the same names).


## Next Steps

Congratulations! You've learned the basics of NeuG. Here's what you can explore next:

1. **[Data Import/Export](../../data_io/import_data)**: Learn how to import large datasets
2. **[Advanced Cypher Queries](../../cypher_manual)**: Master complex graph patterns
3. **[Tutorials](../../tutorials/tinysnb_tutorial)**: Try these interesting tutorials of NeuG