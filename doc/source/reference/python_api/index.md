# Python API Reference

The NeuG Python API provides a high-level, Pythonic interface for interacting with NeuG graph databases. Designed for ease of use and rapid development.

## Overview

The Python API offers a simple yet powerful way to:

- **Connect to databases**: Establish connections to local or remote NeuG instances
- **Execute queries**: Run Cypher queries with automatic result parsing
- **Manage transactions**: Handle ACID transactions for data consistency
- **Process results**: Work with graph data using familiar Python patterns

## Core Classes

- **[Database](database)** - The entrance of the NeuG database
- **[Connection](connection)** - Connection represents a logical connection to a database
- **[Session](session)** - Session is a class that connects to the NeuG server
- **[QueryResult](query_result)** - QueryResult represents the result of a cypher query


## Quick Start

### Installation

```bash
pip install neug
```

### Basic Usage

```python
import neug

# Connect to database
db = neug.Database("/path/to/database")
conn = db.connect()

# Execute a simple query
result = conn.execute("MATCH (n) RETURN n LIMIT 10")

# Process results
for record in result:
    node = record['n']
    print(f"Node ID: {node.id}, Labels: {node.labels}")

# Close connection
conn.close()
```

### Context Manager Usage

```python
import neug

# Using context manager for automatic cleanup
with neug.Database("/path/to/database").connect() as conn:
    result = conn.execute("MATCH (n:Person) RETURN n.name")
    names = [record['n.name'] for record in result]
    print(names)
```

## Transaction Management

```python
# Using transactions
with conn.begin() as tx:
    tx.execute("CREATE (p:Person {name: $name})", name="Alice")
    tx.execute("CREATE (p:Person {name: $name})", name="Bob")
    # Automatic commit on successful completion
```

## Advanced Features

### Parameterized Queries

```python
# Safe parameter passing
result = conn.execute(
    "MATCH (p:Person) WHERE p.age > $min_age RETURN p",
    min_age=25
)
```

### Batch Operations

```python
# Execute multiple statements efficiently
statements = [
    ("CREATE (p:Person {name: $name})", {"name": "Alice"}),
    ("CREATE (p:Person {name: $name})", {"name": "Bob"}),
]
conn.execute_batch(statements)
```

## Error Handling

```python
try:
    result = conn.execute("INVALID CYPHER QUERY")
except neug.CypherError as e:
    print(f"Query error: {e}")
except neug.ConnectionError as e:
    print(f"Connection error: {e}")
```

## Type Hints Support

The Python API includes comprehensive type hints for better IDE support:

```python
from neug import Database, Connection, QueryResult
from typing import Iterator, Dict, Any

db: Database = Database("path/to/database")
conn: Connection = db.connect()
result: QueryResult = conn.execute("MATCH (n) RETURN n")
records: Iterator[Dict[str, Any]] = iter(result)
```

