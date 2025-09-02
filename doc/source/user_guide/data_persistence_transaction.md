
# Data Persistence and Transaction Management

NeuG handles data persistence and managest transactions differently depending on the connection mode you choose. Understanding these differences is crucial for building robust applications.

## Checkpoint
In NeuG, users can create a checkpoint to persist the data on the current graph to the database directory. Only one checkpoint exists per database directory, the newly created checkpoint will overwrite the previous checkpoint data.

Creating a checkpoint is performed through an UpdateTransaction and will block the execution of other queries until it completes. It can be manually invoked by the user through a command, or automatically triggered when the database is shutting down.

### Create a checkpoint manually

```
CHECKPOINT;
```

This command is used to manually trigger checkpoint actions in the system. 

### Automatic checkpoint on shutdown
By default, NeuG performs a checkpoint creation automatically when the database closes. Users can explicitly control whether a checkpoint is created during shutdown by passing the parameter `checkpoint_on_shutdown` when initialized the NeuG Database. TODO(zhanglei): Make sure the paramter is valid.

## Data Persistence

### Embedded Mode: Manual Persistence Control

In embedded mode, NeuG prioritizes performance by keeping data modifications in memory until explicitly persisted.

**How it works:**
- When you open a persistent database, existing data is loaded into memory
- Data modifications (CREATE, UPDATE, DELETE, COPY, ALTER) are performed in memory only
- Changes are not automatically written to disk for optimal performance
- You control when to persist changes using the CHECKPOINT operation

**Creating checkpoints:**
```python
import neug

db = neug.Database(db_path='/tmp/persist')
conn = db.connect()

# Perform data modifications
conn.execute("CREATE (p:Person {name: 'Alice'})")

# Manually persist changes to disk
conn.execute('CHECKPOINT')

conn.close()
db.close()
```

```{note}
The CHECKPOINT is a synchronized operation, and may take time to complete for large datasets
```

**Automatic checkpointing on close:**
For convenience, NeuG automatically conducts checkpointing when closing the database.

```python
import neug

# Enable automatic checkpoint on close (default: True)
db = neug.Database(db_path='/tmp/persist', checkpoint_when_close=True)
conn = db.connect()

# Your data modifications...
conn.execute("CREATE (p:Person {name: 'Bob'})")

# Checkpoint will be automatically created when closing
conn.close()
db.close()  # Automatic checkpoint happens here
```

Set it to `db = neug.Database(db_path='/tmp/persist', checkpoint_when_close=False)` if you want to manage checkpoints manually and avoid automatic persistence on close.

### Service Mode: Automatic Persistence with MVCC

Service mode provides automatic data persistence with full ACID transaction support.

**Key features:**
- **MVCC (Multi-Version Concurrency Control)**: Supports concurrent transactions
- **Write-Ahead Logging (WAL)**: All modifications are logged before execution
- **Automatic persistence**: Changes are automatically saved without manual checkpoints
- **ACID compliance**: Full transaction guarantees for data integrity

#### ACID Properties in Service Mode

NeuG's service mode provides full **ACID** transaction guarantees:

**Atomicity**: Transactions are all-or-nothing operations. If any part of a transaction fails, the entire transaction is rolled back, ensuring the database remains in a consistent state. NeuG adapt a auto-transaction way, which means for each query you submitted, it is automatically wrapped in a transaction.

**Consistency**: The database maintains referential integrity and enforces all constraints. Transactions can only transition the database from one valid state to another.

**Isolation**: Concurrent transactions are isolated from each other using serializable isolation level. Each transaction sees a consistent snapshot of the database without interference from other concurrent transactions.

```python
# Two concurrent sessions see consistent, isolated views
session1 = Session("http://localhost:10001/")
session2 = Session("http://localhost:10001/")

# session1 and session2 operations don't interfere with each other
session1.execute("CREATE (p:Person {name: 'Bob'})")
session2.execute("MATCH (p:Person) RETURN count(p)")  # Consistent view
```

**Durability**: Once a transaction is committed, the changes are permanently stored and will survive system crashes. This is ensured through the Write-Ahead Logging (WAL) mechanism.

**How it works:**
1. All graph modifications are first written to the WAL
2. Changes are then applied to the in-memory database
3. Data is automatically persisted without manual intervention
4. You can manually trigger checkpoints if needed.

```python
# Client connection to service mode
from neug import Session

session = Session("http://localhost:10001/")

# All modifications are automatically persisted
session.execute("CREATE (p:Person {name: 'Charlie'})")
# No manual CHECKPOINT needed - data is already persistent

session.close()

session2 = Session("http://localhost:10001/")
# Create checkpoint 
session2.execute("CHECKPOINT") #TODO(zhanglei): currently not implemented
session2.close()
```

When `CHECKPOINT` is executed, NeuG creates a snapshot of the current graph state and writes it to the checkpoint directory, atomically replacing the previous checkpoint. If the checkpoint operation fails, the original checkpoint remains intact, ensuring data consistency.


## Transaction Management

NeuG implements different transaction models optimized for each connection mode's typical usage patterns.

### Embedded Mode: Global Lock-Based Transactions

Embedded mode uses a **global database lock** approach for transaction management, prioritizing simplicity and performance for analytical workloads.

**How it works:**
- Each database operation acquires an exclusive global lock
- Only write can execute at a time across the entire database, read queires could be executed concurrently.
- Read and write operations are mutually exclusive - they cannot run concurrently
- No WAL (Write-Ahead Logging) or MVCC overhead

**Transaction characteristics:**
```python
import neug
import threading

db = neug.Database(db_path='/tmp/mydb', mode='w')
conn = db.connect()

def run_query(conn, query):
    conn.execute(query)

# Each write/update/delete operation holds an exclusive lock
t1 = threading.Thread(target=run_query, args=(conn, 'CREATE NODE TABLE person(id INT32, name STRING, PRIMARY KEY(id));'))
t2 = threading.Thread(target=run_query, args=(conn, 'CREATE NODE TABLE software(id INT32, name STRING, PRIMARY KEY(id))'))

t1.start()
t2.start()

t1.join()
t2.join()

conn.close()
db.close()
```

**Why this design?**
- **Performance-optimized for analytics**: Large analytical queries can utilize all system resources without contention
- **Simplified transaction model**: No complex concurrency control overhead
- **Acceptable trade-offs**: For batch processing and analytical workloads, exclusive access is often preferred
- **Cost-effective recovery**: If a transaction fails, re-running the query is typically acceptable for analytical use cases

**Limitations:**
- No concurrent access - only one operation at a time
- Not suitable for high-frequency transactional workloads
- Write operations block all reads (and vice versa)

### Service Mode: MVCC-Based Transactions

Service mode implements **Multi-Version Concurrency Control (MVCC)** with **Write-Ahead Logging (WAL)** to provide full ACID transaction guarantees.

**How it works:**
- Multiple concurrent transactions are supported
- Each transaction sees a consistent snapshot of the database
- WAL ensures durability and enables recovery
- Serializable isolation level is maintained

**Transaction characteristics:**
```python
from neug import Session

# Multiple concurrent sessions are supported
session1 = Session("http://localhost:10001/")
session2 = Session("http://localhost:10001/")

# Concurrent operations are possible
session1.execute("CREATE (p:Person {name: 'Alice'})")
session2.execute("MATCH (p:Person) RETURN p")  # Can run concurrently

session1.close()
session2.close()
```

**MVCC Benefits:**
- **Concurrent Read-Write**: Multiple transactions can read while others write
- **Snapshot Isolation**: Each transaction sees a consistent view of the data
- **No Read Locks**: Readers never block writers (and vice versa)
- **ACID Compliance**: Full transaction guarantees with serializable isolation

**WAL (Write-Ahead Logging):**
- All modifications are logged before execution
- Enables crash recovery and data durability
- Supports point-in-time recovery
- Automatic log rotation and cleanup

**Why not use MVCC/WAL in embedded mode?**

The design choice to use different transaction models reflects the distinct use case patterns:

1. **WAL Size Concerns**: Large analytical operations in embedded mode would generate massive WAL files, consuming significant disk space and I/O bandwidth

2. **Performance Overhead**: MVCC and WAL introduce computational overhead that's unnecessary for typical analytical workloads where exclusive access is acceptable

3. **Recovery Cost**: In analytical scenarios, re-running a failed query is often more cost-effective than maintaining complex transaction logs

4. **Resource Utilization**: Analytical queries benefit from utilizing all available system resources without concurrent access constraints

### Choosing the Right Mode

**Use Embedded Mode when:**
- Performing large-scale data analysis
- Batch processing and ETL operations
- Single-user analytical workflows
- Maximum query performance is priority
- Exclusive database access is acceptable

**Use Service Mode when:**
- Building multi-user applications
- Requiring concurrent read/write access
- Need ACID transaction guarantees
- Handling real-time transactional workloads
- Building production web services

### Transaction Best Practices

**For Embedded Mode:**
```python
# Group related operations together
conn.execute("CREATE (p1:Person {name: 'Alice'})")
conn.execute("CREATE (p2:Person {name: 'Bob'})")
conn.execute("CREATE (p1)-[:KNOWS]->(p2)")
# Checkpoint when logical unit is complete
conn.execute("CHECKPOINT")
```

**For Service Mode:**
```python
# Leverage concurrent access
session = Session("http://localhost:10001/")

# Multiple operations in sequence - automatically managed
session.execute("CREATE (p:Person {name: 'Alice'})")
session.execute("MATCH (p:Person) RETURN p")

# No manual checkpoint needed - automatically persisted
session.close()
```

### Error Handling and Recovery

**Embedded Mode Recovery:**
- Failed operations can be safely retried
- Use checkpoints to create recovery points
- Database state remains consistent after failures

**Service Mode Recovery:**
- Automatic crash recovery using WAL
- Uncommitted transactions are automatically rolled back
- Database automatically restores to last consistent state

```python
try:
    session = Session("http://localhost:10001/")
    session.execute("CREATE (p:Person {name: 'Alice'})")
    # Automatic commit and persistence
except Exception as e:
    # Automatic rollback - no manual cleanup needed
    print(f"Transaction failed: {e}")
``` 