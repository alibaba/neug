# Transaction Management

This document describes NeuG's transaction management system, which operates differently depending on the deployment mode. NeuG supports two primary modes: Embedded mode suitable for analytical processing (AP) and Service mode suitable for transactional processing (TP).

## Overview

NeuG implements distinct transaction management strategies optimized for different workload patterns:

**Embedded Mode (AP - Analytical Processing)**
- Designed for single-user analytical workloads and batch processing
- Employs global locking for concurrency control
- Uses explicit checkpointing for data persistence
- Optimized for maximum single-query performance

**Service Mode (TP - Transactional Processing)**  
- Designed for multi-user concurrent applications
- Implements Multi-Version Concurrency Control (MVCC) with Write-Ahead Logging (WAL)
- Provides automatic persistence with full ACID guarantees
- Optimized for concurrent access and data integrity

## Transaction Model

NeuG implements a statement-level transaction model where each statement automatically executes as an atomic unit. Manual (and more flexible) transaction control will be supported in future version.

### Statement Execution Semantics

1. Each statement is automatically wrapped in a transaction
2. Successful statements commit their changes according to the mode's persistence rules
3. Failed statements are automatically rolled back with no partial state changes
4. No explicit transaction management is required from the user

## ACID Properties

### Embedded Mode (AP)

- **Atomicity**: Each statement executes completely or not at all within memory
- **Consistency**: Schema constraints and referential integrity are enforced
- **Isolation**: Achieved through global locking (readers concurrent, writers exclusive)
- **Durability**: Changes are persisted to disk only after an explicit `CHECKPOINT` operation or database closure with `checkpoint_when_close=True`. For in-memory databases, durability is not applicable as data exists only in volatile memory.

### Service Mode (TP)

- **Atomicity**: Each statement executes as an all-or-nothing operation
- **Consistency**: Schema constraints and referential integrity are enforced  
- **Isolation**: Serializable isolation level implemented via MVCC
- **Durability**: Immediate persistence through Write-Ahead Logging (WAL)

## Concurrency Control

### Embedded Mode: Global Locking

Embedded mode uses a global lock-based approach for simplicity and analytical performance:

| Operation Type | Concurrency Behavior |
|----------------|---------------------|
| Read-Read      | Concurrent execution allowed |
| Read-Write     | Mutually exclusive (write takes exclusive lock) |
| Write-Write    | Mutually exclusive (serialized execution) |

**Example:**
```python
import neug
import threading

db = neug.Database(db_path='/tmp/analytics_db', mode='w')
conn = db.connect()

def execute_ddl(connection, statement):
    connection.execute(statement)

# These operations will execute serially due to write locks
t1 = threading.Thread(target=execute_ddl, 
                     args=(conn, 'CREATE NODE TABLE Person(id INT32, name STRING, PRIMARY KEY(id));'))
t2 = threading.Thread(target=execute_ddl, 
                     args=(conn, 'CREATE NODE TABLE Company(id INT32, name STRING, PRIMARY KEY(id));'))

t1.start()
t2.start()
t1.join()
t2.join()

conn.close()
db.close()
```

### Service Mode: MVCC with WAL

Service mode implements Multi-Version Concurrency Control for high-concurrency scenarios:

- Multiple statements can execute concurrently
- Each statement observes a consistent snapshot (serializable isolation)
- Read operations do not block write operations
- Write operations are persisted via WAL before completion

**Example:**
```python
from neug import Session

# Concurrent sessions can operate simultaneously
session1 = Session("http://localhost:10000/")
session2 = Session("http://localhost:10000/")

# These operations can execute concurrently
session1.execute("CREATE (p:Person {name: 'Alice', age: 30})")
session2.execute("MATCH (p:Person) RETURN count(p)")

session1.close()
session2.close()
```

## Data Persistence

### Embedded Mode: Checkpoint-Based Persistence

Embedded mode prioritizes performance by maintaining changes in memory until explicitly persisted.

#### Manual Checkpointing

Execute the `CHECKPOINT` statement to persist all in-memory changes to disk:

```cypher
CHECKPOINT;
```

The checkpoint operation:
- Creates an atomic snapshot of the current database state
- Replaces the previous checkpoint atomically
- Blocks other operations during execution
- May require significant time for large datasets

**Python Example:**
```python
import neug

db = neug.Database(db_path='/tmp/persistent_db')
conn = db.connect()

# Execute data modifications
conn.execute("CREATE (p:Person {name: 'Alice', age: 30})")
conn.execute("CREATE (c:Company {name: 'TechCorp'})")

# Persist changes to disk
conn.execute('CHECKPOINT')

conn.close()
db.close()
```

#### Automatic Checkpointing

By default, NeuG automatically creates a checkpoint when closing the database:

```python
import neug

# Automatic checkpoint on close (default behavior)
db = neug.Database(db_path='/tmp/persistent_db')
conn = db.connect()

conn.execute("CREATE (p:Person {name: 'Bob', age: 25})")

# Automatic checkpoint occurs during database closure
conn.close()
db.close()
```

To disable automatic checkpointing and always manage checkpoints manually:
```python
db = neug.Database(db_path='/tmp/persistent_db', checkpoint_when_close=False)
```

### Service Mode: WAL-Based Persistence

Service mode provides immediate persistence through Write-Ahead Logging:

- All modifications are logged to WAL before execution
- Changes are automatically persisted upon statement completion
- Supports crash recovery and point-in-time consistency

**Example:**
```python
from neug import Session

session = Session("http://localhost:10000/")

# Changes are immediately persisted via WAL
session.execute("CREATE (p:Person {name: 'Charlie', age: 35})")

session.close()
```

#### Manual Checkpointing in Service Mode

Service mode supports manual checkpointing to consolidate WAL entries and optimize storage:

```python
from neug import Session

session = Session("http://localhost:10000/")
session.execute("CHECKPOINT")
session.close()
```

When executed in Service mode, the `CHECKPOINT` operation:
- Temporarily blocks all (read/write) operations while the checkpoint is being created
- Consolidates the previous checkpoint data with all accumulated WAL entries
- Creates a new unified checkpoint file
- Clears processed WAL entries to optimize storage space
- Continues normal WAL-based persistence for subsequent operations
- Does not affect the automatic durability of individual statements

## Error Recovery

### Embedded Mode Recovery

- Database state remains consistent after failures
- Failed operations can be safely retried
- Use checkpoints to establish recovery points
- Restart from last successful checkpoint after crashes

### Service Mode Recovery

- Automatic crash recovery using WAL replay
- Uncommitted operations are automatically rolled back
- Database automatically restores to last consistent state
- No manual recovery intervention required

**Error Handling Example:**
```python
from neug import Session

try:
    session = Session("http://localhost:10000/")
    session.execute("CREATE (p:Person {name: 'David', age: 40})")
    # Statement automatically committed and persisted
except Exception as e:
    # Automatic rollback - no cleanup required
    print(f"Operation failed: {e}")
finally:
    if 'session' in locals():
        session.close()
```

## Configuration Parameters

### Embedded Mode Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `checkpoint_when_close` | boolean | `True` | Automatically create checkpoint when closing database |

**Example:**
```python
db = neug.Database(
    db_path='/tmp/analytics_db',
    checkpoint_when_close=True
)
```


## Best Practices

### Embedded Mode Best Practices

```python
import neug

db = neug.Database(db_path='/tmp/analytics')
conn = db.connect()

# Create schema first
conn.execute("CREATE NODE TABLE Person(id INT32, name STRING, department STRING, PRIMARY KEY(id))")
conn.execute("CREATE NODE TABLE Company(id INT32, name STRING, industry STRING, PRIMARY KEY(id))")
conn.execute("CREATE REL TABLE WorksAt(FROM Person TO Company)")
conn.execute("CREATE REL TABLE Manages(FROM Person TO Person)")
conn.execute("CHECKPOINT")  # Checkpoint after schema creation

# Load large datasets in batches with periodic checkpoints
conn.execute("COPY Person FROM 'employees.csv'")
conn.execute("COPY Company FROM 'companies.csv'") 
conn.execute("CHECKPOINT")  # Checkpoint after loading base data

conn.execute("COPY WorksAt FROM 'employment.csv'")
conn.execute("COPY Manages FROM 'management.csv'")
conn.execute("CHECKPOINT")  # Checkpoint after loading relationships

conn.close()
db.close()
```

### Service Mode Best Practices

```python
from neug import Session

session = Session("http://localhost:10000/")

try:
    # Create schema first
    session.execute("CREATE NODE TABLE Person(id INT32, name STRING, department STRING, PRIMARY KEY(id))")
    session.execute("CREATE NODE TABLE Company(id INT32, name STRING, industry STRING, PRIMARY KEY(id))")
    session.execute("CREATE REL TABLE WorksAt(FROM Person TO Company)")
    
    # Load large datasets - each automatically persisted via WAL
    session.execute("COPY Person FROM 'employees.csv'")
    session.execute("COPY Company FROM 'companies.csv'")
    session.execute("COPY WorksAt FROM 'employment.csv'")
    
    # Optionally create checkpoint to consolidate WAL entries
    session.execute("CHECKPOINT")
    
finally:
    session.close()
```

## Mode Comparison Summary

| Aspect | Embedded Mode (AP) | Service Mode (TP) |
|--------|-------------------|-------------------|
| **Transaction Scope** | Per-statement atomic execution | Per-statement atomic execution |
| **Concurrency Model** | Global locking (reads concurrent, writes exclusive) | MVCC with serializable isolation |
| **Persistence Strategy** | Explicit checkpointing | Automatic WAL-based persistence |
| **Manual Checkpointing** | Available for in-memory to disk persistence | Available to consolidate WAL into checkpoint |
| **Durability Guarantee** | After CHECKPOINT or database close | Immediate upon statement completion |
| **Concurrency Level** | Limited (single writer) | High (concurrent readers/writers) |
| **Recovery Method** | Checkpoint-based recovery | WAL-based automatic recovery |
| **Performance Focus** | Single-query throughput | Concurrent access scalability |
| **Target Workloads** | Analytics, ETL, batch processing | Interactive applications, web services |