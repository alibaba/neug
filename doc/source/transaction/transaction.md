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

In Embedded mode, checkpoints can be used as recovery points around large
write operations. See [Checkpoints](checkpoint.md) for a safe recovery example
and the interaction with `checkpoint_on_close`.

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
- Database closure with `checkpoint_on_close=True` (default)

For in-memory databases, durability is not applicable as data exists only in volatile memory.
For usage and failure behavior, see [Checkpoints](checkpoint.md).

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
| Read | Reads a consistent snapshot and continues during active inserts/updates. |
| Insert | Concurrent with reads and other inserts. Waits if an update is in progress. |
| Update | One at a time. Reads continue on a consistent snapshot. Inserts and other updates wait until the update finishes. |
| Schema (DDL) | Same concurrency behavior as update. |
| Checkpoint | Exclusive maintenance. See [Checkpoints](checkpoint.md). |

> Note: A read that arrives while an update is finishing may wait briefly,
> typically at millisecond scale.

**Design Rationale:** This hybrid approach reflects the reality of graph workloads:

- **Reads and inserts** are the dominant operations in most graph applications (social networks, knowledge graphs, recommendation systems)
- **Updates and schema changes** are relatively rare and are serialized, while reads continue on a consistent snapshot with only brief waits when they arrive as a write is finishing
- **Checkpoints** are database-level maintenance operations; their concurrency behavior is described in [Checkpoints](checkpoint.md)
- Full MVCC for all write types would add significant complexity with minimal benefit for typical graph workloads

```python
from neug import Session

# Concurrent sessions
session1 = Session("http://localhost:10000/")
session2 = Session("http://localhost:10000/")

# These can execute concurrently (read + insert)
session1.execute("MATCH (p:Person) RETURN count(p)", access_mode="read")
session2.execute("CREATE (p:Person {name: 'Bob'})", access_mode="insert")

# This is serialized with inserts and other updates. Reads continue on a
# consistent snapshot while the update runs.
session1.execute("MATCH (p:Person) SET p.updated = true", access_mode="update")
```

#### Durability

All modifications are immediately persisted through **Write-Ahead Logging (WAL)**:

- Changes are logged to WAL before execution completes
- Crash recovery automatically replays WAL to restore consistent state
- No explicit checkpoint is required for durability; checkpoints are available for WAL consolidation. See [Checkpoints](checkpoint.md)

## Access Mode

When executing queries, you can specify an `access_mode` to control transaction behavior and optimize performance:

| Mode | Aliases | Description |
|------|---------|-------------|
| `read` | `r` | Read-only operations (MATCH without mutations) |
| `insert` | `i` | Insert-only operations (CREATE new vertices/edges) |
| `update` | `u` | Update/delete operations (SET, DELETE, MERGE) |
| `schema` | `s` | Schema modifications (CREATE/DROP node/edge tables) |

**Default Behavior:** If not specified, NeuG infers the appropriate mode. Explicitly specifying the correct mode enables better concurrency optimization in Service mode.

**Access Mode Capabilities:** Access modes are operation-specific rather than a strict hierarchy. In Service mode, `read` and `insert` are narrow modes, while `update` and `schema` use the general serialized update path. Using a mode that does not support the planned operation returns an error.

| Specified Mode | Actual Operation | Result |
|----------------|------------------|--------|
| `read` | read | ✅ OK |
| `read` | insert/update/schema/checkpoint | ❌ Error |
| `insert` | insert | ✅ OK |
| `insert` | read/update/schema/checkpoint | ❌ Error |
| `update` | read/insert/update/schema/checkpoint | ✅ OK (serialized update path) |
| `schema` | read/insert/update/schema | ✅ OK (serialized update path) |

In Embedded mode, modes other than `read` primarily select shared versus exclusive query locking rather than distinct transaction implementations. Explicit `read` still rejects write operations, and the special `CHECKPOINT` restriction below applies in both deployment modes.

`CHECKPOINT` is a special administrative statement. See
[Checkpoints](checkpoint.md) for its accepted access modes and
`EXPLAIN`/`PROFILE` behavior.

```python
# Optimal: match access mode to operation for best concurrency
conn.execute("MATCH (n) RETURN n", access_mode="read")        # read lock only
conn.execute("CREATE (p:Person {name: 'Alice'})", access_mode="insert")  # insert lock

# Works but suboptimal: update mode for read query enters the update path
conn.execute("MATCH (n) RETURN n", access_mode="update")      # works, but reduces write concurrency
```

## Data Persistence

### Embedded Mode: Checkpoint-Based

All changes are held in memory until a checkpoint persists them. See
[Checkpoints](checkpoint.md) for usage, examples, and recovery behavior.

### Service Mode: WAL-Based

Changes are automatically persisted through Write-Ahead Logging:

```python
from neug import Session

session = Session("http://localhost:10000/")

# Each statement is immediately durable
session.execute("CREATE (p:Person {name: 'Alice'})")  # Persisted via WAL
session.execute("CREATE (p:Person {name: 'Bob'})")    # Persisted via WAL

session.close()
```

An optional checkpoint consolidates the WAL but is not required for statement
durability. See [Checkpoints](checkpoint.md).

## Error Recovery

### Embedded Mode

Recovery relies on the last successful checkpoint. See
[Recovering from a failed Embedded write](checkpoint.md) for an example.

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
conn.execute("CREATE REL TABLE WORKS_AT(FROM Person TO Company)")
conn.execute("CHECKPOINT")

# 2. Load data in batches with periodic checkpoints
conn.execute("COPY Person FROM 'employees_batch1.csv'")
conn.execute("COPY Person FROM 'employees_batch2.csv'")
conn.execute("CHECKPOINT")

conn.execute("COPY Company FROM 'companies.csv'")
conn.execute("COPY WORKS_AT FROM 'employment.csv'")
conn.execute("CHECKPOINT")

# 3. Run analytical queries (read-only, high performance)
result = conn.execute("MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name, count(p)")

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
    
    # Updates are serialized with inserts and other updates. Reads continue on
    # consistent snapshots, but keep updates short in high-concurrency scenarios.
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
| **Isolation** | Exclusive write locks | MVCC for reads/inserts; serialized updates/DDL; exclusive checkpoints |
| **Durability** | Explicit CHECKPOINT or close | Automatic WAL persistence |
| **Concurrent Reads** | Yes | Yes, except during exclusive checkpoint maintenance |
| **Concurrent Inserts** | No | Yes, unless an update is active |
| **Concurrent Updates** | No | No, updates are serialized while reads continue on consistent snapshots |
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
