### Key Concepts
Database, Connection, and Session are fundamental concepts in NeuG.

## Database

A **Database** in NeuG represents a graph consisting of vertices and edges. By default, data is persisted on disk in a specified directory. NeuG also offers an **in-memory** mode, where data resides only in memory and is not saved to disk. To create an in-memory database, set `db_path` to an empty string:

```python
from neug import Database

# Create an in-memory database
db = Database(db_path="")
```

### Access Modes

NeuG databases can be accessed in two modes: `read_write` and `read_only`.

- **read_write**: Opening a directory in **read_write** mode grants exclusive access to that process by locking the directory, preventing any other processes from accessing it concurrently.
- **read_only**: When opened in **read_only** mode, multiple processes can access the directory simultaneously in **read_only** mode.

It's important to note that in-memory databases cannot be opened in `read_only` mode.

## Connection

A **Connection** serves as the primary interface for interacting with the embedded database, facilitating query execution and data management.

### Establishing Connections from Database

A **Connection** object acts as a conduit for accessing the database, with its behavior influenced by the database's access mode.

- In **read_write** mode, a Connection can execute both read (MATCH) and write queries (CREATE, COPY FROM). Only one connection object may be created for a read_write database to maintain data consistency.
  
- In **read_only** mode, a Connection is limited to executing read queries.

## Service Mode and Session

NeuG can also function in **service mode**, where the database operates as a server, accepting connections remotely. This mode is optimal for applications requiring frequent, concurrent, or distributed access.

To initiate NeuG in service mode, specify the host and port:

```python
import neug

# Launch NeuG as a service
db = neug.Database("./neug/db")
service = db.serve(host="localhost", port=10000)
print(f"NeuG service started on {service}")
```

Once the server is active, clients can connect using a `Session` object:

```python
from neug import Session

# Establish a connection to the NeuG service
session = Session("http://localhost:10000/")

# Perform a query
session.execute('MATCH(n) RETURN count(n)')

session.close()
```

Multiple sessions can be instantiated across various processes to submit concurrent queries. The database server ensures the ACID properties of the database are maintained throughout.