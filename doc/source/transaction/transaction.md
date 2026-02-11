# Transaction Management

This document describes NeuG's transaction management system, which operates differently depending on the deployment mode. NeuG supports two primary modes: **Embedded mode** suitable for analytical processing (AP) and **Service mode** suitable for transactional processing (TP).

## Design Philosophy

NeuG's transaction model reflects trade-offs based on deep understanding of graph workload characteristics:

- **Graph-native workloads** are predominantly read-heavy with insert operations, while updates and schema changes are relatively infrequent
- **Analytical workloads** prioritize single-query throughput over complex consistency guarantees
- **Transactional workloads** require concurrent read/insert performance with strong isolation

Rather than implementing a one-size-fits-all transaction system with high complexity, NeuG provides workload-appropriate guarantees that balance simplicity, performance, and correctness.

## Transaction Model

NeuG implements an **implicit transaction model** with auto-commit semantics:

- **No explicit transaction primitives**: NeuG does not currently expose `BEGIN`, `COMMIT`, or `ABORT` commands
- **Auto-commit semantics**: Each `execute()` call automatically forms its own transaction — one statement per call

```python
# Each execute() is an independent, auto-committed transaction
conn.execute("CREATE (p:Person {name: 'Alice'})")  # Transaction 1
conn.execute("CREATE (p:Person {name: 'Bob'})")    # Transaction 2
```

> **Coming in v0.2:** Multi-statement execution (multiple statements separated by `;` in a single `execute()` call) and explicit transaction control (`BEGIN`/`COMMIT`/`ABORT`) will be supported in NeuG v0.2.

## Deployment Modes Overview

| Aspect | Embedded Mode | Service Mode |
|--------|-------------------|-------------------|
| **Use Case** | Analytics, ETL, batch processing | Interactive applications, web services |
| **Concurrency** | Single-user, sequential writes | Multi-user, concurrent access |
| **Persistence** | Explicit checkpoint-based | Automatic WAL-based |
| **Optimization Goal** | Maximum single-query performance | Concurrent read/insert throughput |

## ACID Properties

### Embedded Mode (AP-oriented)

Embedded mode is designed for analytical workloads where simplicity and single-query performance take precedence over complex transaction guarantees.

#### Atomicity

**Current Limitation:** Statement-level atomicity is **not fully guaranteed** for large write operations.

For statements involving substantial data writes (such as `COPY` for bulk loading), a failure mid-execution may result in partial data being written to memory. This is a known trade-off for AP workloads where the overhead of full rollback mechanisms is typically unnecessary.

**Practical Workaround:** Since all writes are held in memory until an explicit `CHECKPOINT`, users can establish recovery points:

```cypher
-- Create a recovery point before large operations
CHECKPOINT;

-- Perform bulk data loading
COPY Person FROM 'large_dataset.csv';

-- If an error occurs before this point, restart the database
-- to reload from the last checkpoint (all uncommitted changes are discarded)
```

#### Consistency

Schema constraints and referential integrity are enforced at the statement level. Invalid operations (such as creating duplicate primary keys or violating edge constraints) are rejected.

#### Isolation

**Pessimistic exclusive locking** is used for all write operations. Any write statement (insert, update, delete, or schema modification) acquires an exclusive lock, blocking all other operations until completion.

| Operation Type | Concurrency Behavior |
|----------------|---------------------|
| Read + Read | Concurrent execution allowed |
| Write + READ/Write | Mutually exclusive |

This simple locking model is sufficient for single-user analytical workloads and avoids the complexity of fine-grained concurrency control.

#### Durability

Changes are persisted to disk **only** after:
- An explicit `CHECKPOINT` statement, or
- Database closure with `checkpoint_when_close=True` (default)

For in-memory databases, durability is not applicable as data exists only in volatile memory.

```python
import neug

db = neug.Database("/path/to/database")
conn = db.connect()

# Changes are in memory only
conn.execute("CREATE (p:Person {name: 'Alice'})")

# Explicit persistence to disk
conn.execute("CHECKPOINT")

# Or rely on automatic checkpoint at close
conn.close()
db.close()  # checkpoint_when_close=True by default
```

### Service Mode

Service mode is designed for high-throughput transactional workloads with concurrent client access. The concurrency model is optimized for graph-native access patterns where reads and inserts dominate.

#### Atomicity

Each statement executes as an all-or-nothing operation. Failed statements are automatically rolled back with no partial state changes visible to other transactions.

#### Consistency

Schema constraints and referential integrity are enforced. All concurrent operations observe a consistent view of the database.

#### Isolation

NeuG uses **Multi-Version Concurrency Control (MVCC)** to provide serializable isolation, with operation-specific concurrency rules:

| Operation Type | Concurrency Behavior |
|----------------|---------------------|
| Read | Concurrent with all reads and inserts |
| Insert | Concurrent with reads and other inserts |
| Update | Acquires global write lock, blocks all operations |
| Schema (DDL) | Acquires global write lock, blocks all operations |
| Checkpoint | Acquires global write lock, blocks all operations |

**Design Rationale:** This hybrid approach reflects the reality of graph workloads:

- **Reads and inserts** are the dominant operations in most graph applications (social networks, knowledge graphs, recommendation systems)
- **Updates and schema changes** are relatively rare and can tolerate exclusive locking
- Full MVCC for all write types would add significant complexity with minimal benefit for typical graph workloads

```python
from neug import Session

# Concurrent sessions
session1 = Session("http://localhost:10000/")
session2 = Session("http://localhost:10000/")

# These can execute concurrently (read + insert)
session1.execute("MATCH (p:Person) RETURN count(p)", access_mode="read")
session2.execute("CREATE (p:Person {name: 'Bob'})", access_mode="insert")

# This will block other operations (update takes global lock)
session1.execute("MATCH (p:Person) SET p.updated = true", access_mode="update")
```

#### Durability

All modifications are immediately persisted through **Write-Ahead Logging (WAL)**:

- Changes are logged to WAL before execution completes
- Crash recovery automatically replays WAL to restore consistent state
- No explicit checkpoint required for durability (but available for WAL consolidation)

## Access Mode

When executing queries, you can specify an `access_mode` to control transaction behavior and optimize performance:

| Mode | Aliases | Description |
|------|---------|-------------|
| `read` | `r` | Read-only operations (MATCH without mutations) |
| `insert` | `i` | Insert-only operations (CREATE new vertices/edges) |
| `update` | `u` | Update/delete operations (SET, DELETE, MERGE) |
| `schema` | `s` | Schema modifications (CREATE/DROP node/edge tables) |

**Default Behavior:** If not specified, NeuG infers the appropriate mode. Explicitly specifying the correct mode enables better concurrency optimization in Service mode.

**Access Mode Hierarchy:** Access modes follow a hierarchy where higher modes provide stronger guarantees but lower concurrency:

```
read < insert < update ≈ schema
```

- **Upward compatibility**: Using a higher access mode than required always works (e.g., `update` mode for a read-only query), but may reduce throughput due to stronger locking
- **Downward restriction**: Using a lower access mode than required causes an error (e.g., `read` mode for an insert operation)

| Specified Mode | Actual Operation | Result |
|----------------|------------------|--------|
| `read` | read | ✅ OK |
| `read` | insert/update | ❌ Error |
| `insert` | read/insert | ✅ OK |
| `insert` | update/schema | ❌ Error |
| `update`/`schema` | any | ✅ OK (global lock) |

```python
# Optimal: match access mode to operation for best concurrency
conn.execute("MATCH (n) RETURN n", access_mode="read")        # read lock only
conn.execute("CREATE (p:Person {name: 'Alice'})", access_mode="insert")  # insert lock

# Works but suboptimal: update mode for read query (takes global lock)
conn.execute("MATCH (n) RETURN n", access_mode="update")      # works, but blocks other operations
```

## Data Persistence

### Embedded Mode: Checkpoint-Based

All changes are held in memory until explicitly persisted:

```python
import neug

db = neug.Database("/path/to/database")
conn = db.connect()

# Establish recovery point
conn.execute("CHECKPOINT")

# Perform operations (changes in memory)
conn.execute("COPY Person FROM 'employees.csv'")
conn.execute("COPY Company FROM 'companies.csv'")

# Persist all changes to disk
conn.execute("CHECKPOINT")

conn.close()
db.close()
```

**Checkpoint Characteristics:**
- Creates an atomic snapshot of the current database state
- Blocks other operations during execution
- May require significant time for large datasets
- Replaces the previous checkpoint atomically

### Service Mode: WAL-Based

Changes are automatically persisted through Write-Ahead Logging:

```python
from neug import Session

session = Session("http://localhost:10000/")

# Each statement is immediately durable
session.execute("CREATE (p:Person {name: 'Alice'})")  # Persisted via WAL
session.execute("CREATE (p:Person {name: 'Bob'})")    # Persisted via WAL

# Optional: consolidate WAL into checkpoint for storage optimization
session.execute("CHECKPOINT")

session.close()
```

**Service Mode Checkpoint:**
- Temporarily blocks all operations
- Consolidates WAL entries into a unified checkpoint
- Clears processed WAL entries to reclaim storage
- Does not affect the automatic durability of individual statements

## Error Recovery

### Embedded Mode

Recovery relies on the last successful checkpoint:

```python
import neug

db = neug.Database("/path/to/database")
conn = db.connect()

conn.execute("CHECKPOINT")  # Recovery point

try:
    conn.execute("COPY LargeTable FROM 'huge_file.csv'")
    conn.execute("CHECKPOINT")  # Commit if successful
except Exception as e:
    # Partial data may be in memory
    # Close and reopen to restore from last checkpoint
    conn.close()
    db.close()
    
    db = neug.Database("/path/to/database")  # Restores from checkpoint
    conn = db.connect()
```

### Service Mode

Automatic crash recovery using WAL:

- Uncommitted operations are automatically rolled back
- Database restores to last consistent state on startup
- No manual intervention required

## Best Practices

### Embedded Mode

```python
import neug

db = neug.Database("/path/to/database")
conn = db.connect()

# 1. Create schema and checkpoint
conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))")
conn.execute("CREATE NODE TABLE Company(id INT64, name STRING, PRIMARY KEY(id))")
conn.execute("CREATE REL TABLE WorksAt(FROM Person TO Company)")
conn.execute("CHECKPOINT")

# 2. Load data in batches with periodic checkpoints
conn.execute("COPY Person FROM 'employees_batch1.csv'")
conn.execute("COPY Person FROM 'employees_batch2.csv'")
conn.execute("CHECKPOINT")

conn.execute("COPY Company FROM 'companies.csv'")
conn.execute("COPY WorksAt FROM 'employment.csv'")
conn.execute("CHECKPOINT")

# 3. Run analytical queries (read-only, high performance)
result = conn.execute("MATCH (p:Person)-[:WorksAt]->(c:Company) RETURN c.name, count(p)")

conn.close()
db.close()
```

### Service Mode (TP-oriented)

```python
from neug import Session

session = Session("http://localhost:10000/")

try:
    # Reads and inserts can be concurrent across sessions
    session.execute("CREATE (p:Person {name: 'Alice'})", access_mode="insert")
    
    # Reads don't block inserts
    result = session.execute("MATCH (p:Person) RETURN count(p)", access_mode="read")
    
    # Updates take global lock - use sparingly in high-concurrency scenarios
    session.execute("MATCH (p:Person) WHERE p.name = 'Alice' SET p.verified = true", 
                   access_mode="update")
    
    # Periodic checkpoint to consolidate WAL (optional)
    session.execute("CHECKPOINT")
    
finally:
    session.close()
```

## Summary

| Property | Embedded Mode  | Service Mode  |
|----------|-------------------|-------------------|
| **Atomicity** | Partial (checkpoint-based recovery) | Full (automatic rollback) |
| **Consistency** | Schema constraints enforced | Schema constraints enforced |
| **Isolation** | Exclusive write locks | MVCC for read/insert, exclusive lock for update/DDL |
| **Durability** | Explicit CHECKPOINT or close | Automatic WAL persistence |
| **Concurrent Reads** | Yes | Yes |
| **Concurrent Inserts** | No | Yes |
| **Concurrent Updates** | No | No (global lock) |
| **Recovery** | Manual (checkpoint reload) | Automatic (WAL replay) |

## Roadmap

**v0.2: Transaction Control**
- Multi-statement execution: multiple statements separated by `;` in a single `execute()` call, sharing the same transaction boundary
- Explicit transaction primitives (`BEGIN`/`COMMIT`/`ABORT`) for fine-grained transaction control
- Savepoint support for partial rollback within transactions

**Recovery & Durability**
- More transparent recovery mechanisms for Embedded mode
- Delta checkpointing for efficient incremental persistence
- Reduced checkpoint blocking time for large datasets
- Automatic checkpoint

