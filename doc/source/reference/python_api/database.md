<a id="neug.database"></a>

# Module neug.database

The Neug database module.

<a id="neug.database.time"></a>

## Database Objects

```python
class Database(object)
```

The entrance of the Neug database.

This class is used to open a database connection and manage the database. User should use this class to
open a database connection, and then use the `connect` method to get a `Connection` object to interact with the database.

By passing an empty string as the database path, the database will be opened in memory mode.

The database could be opened with different modes(read-only or read-write) and different planners.

When the database is opened in read-only mode, other databases could also open the same database directory in
read-only mode, inside the same process or in different processes.
When the database is opened in read-write mode, no other databases could open the same database directory in
either read-only or read-write mode, inside the same process or in different processes.

When the database is closed, all the connections to the database will be closed automatically.

```python

    >>> from neug import Database
    >>> db = Database("/tmp/test.db", mode="w")
    >>> conn = db.connect()

    >>> # Use the connection to interact with the database
    >>> conn.execute('CREATE TABLE person(id INT64, name STRING);')
    >>> conn.execute('CREATE TABLE knows(FROM person TO person, weight DOUBLE);')

    >>> # Import data from csv file.
    >>> conn.execute('COPY person FROM "person.csv"')
    >>> conn.execute('COPY knows FROM "knows.csv" (from="person", to="person");')

    >>> res = conn.execute('MATCH(n) return n.id;)
    >>> for record in res:
    >>>     print(record)

```

<a id="neug.database.Database.__init__"></a>

### \_\_init\_\_

```python
def __init__(db_path: str = None,
             mode: str = "read-write",
             max_thread_num: int = 0,
             checkpoint_on_close: bool = True,
             buffer_strategy: str = "M_FULL")
```

Open a database.

- **Parameters:**
  - `db_path` (str)
    Path to the database file. required. If it is set to empty string, the database will be opened in memory mode.
    Note that in memory mode, the database will not be persisted to disk, and all data will be
    lost when the program exits. In this case, the db_path should not contain any illegal characters.
  - `mode` (str)
    Mode to open the database, could be 'r', 'read', 'readwrite', 'w', 'rw', 'write'. Default is 'r' for read-only.
  - `max_thread_num` (int)
    Maximum number of threads to use. Default is 0, which means no limit.
  - `checkpoint_on_close` (bool)
    Whether to automatically create a checkpoint when the database is closed. Default is True.
    If False, no checkpoint is created automatically when close the database.
  - `buffer_strategy` (str)
    Buffer strategy to use for the database, could be 'InMemory' (or 'M_FULL'), 'SyncToFile' (or 'M_LAZY')
    or 'HugePagePreferred' (or 'M_HUGE'). Default is 'M_FULL'.
    - 'InMemory' / 'M_FULL': The database will be opened fully in memory, and the changes will not be
    persisted to disk until checkpoint is created.
    - 'SyncToFile' / 'M_LAZY': The database will be opened in memory on demand, suitable for large databases
    that cannot fit into memory. Also changes will not be persisted to disk until checkpoint is created.
    - 'HugePagePreferred' / 'M_HUGE': Similar to 'InMemory', but it will try to use huge pages for memory
    allocation, which may improve performance for large databases.

- **Raises:**
  - **RuntimeError**
    If the database file does not exist or the mode is invalid.
  - **ValueError**
    If the mode is not one of 'r', 'read', 'w', 'rw', 'write'.
    If the planner is not 'gopt'.

<a id="neug.database.Database.version"></a>

### version

```python
@property
def version()
```

Get the version of the database.

<a id="neug.database.Database.mode"></a>

### mode

```python
@property
def mode() -> str
```

Get the mode of the database.

- **Returns:**
  - **str**
    The mode of the database, could be 'r', 'read', 'w', 'rw', 'write', 'readwrite'.

<a id="neug.database.Database.connect"></a>

### connect

```python
def connect() -> Connection
```

Connect to the database.

- **Returns:**
  - **Connection**
    A Connection object to interact with the database.
- **Raises:**
  - **RuntimeError**
    If the database is closed or not opened.

<a id="neug.database.Database.serve"></a>

### serve

```python
def serve(port: int = 10000, host: str = "localhost", blocking: bool = True)
```

Start the database server for handling remote connections(TP mode).
This method is used to start the database server for handling remote connections.
When db.serve() is called, the database will switch to the TP mode, and all the connections to the local database
will be closed. After that, no new connections to the local database will be allowed.
It will start a server that listens on a specific port, and clients can connect to the server to interact with the
database. User could use Session to connect to the server. For detail usage, please refer to the
documentation of Session.

- **Parameters:**
  - `port` (int)
    The port to listen on. Default is 10000.
  - `host` (str)
    The host to listen on. Default is 'localhost'.
  - `blocking` (bool)
    Whether to block the process after starting the database server.

- **Returns:**
  - `uri` (str)
    The URI of the server, in the format of 'http://host:port'.

- **Raises:**
  - **RuntimeError**
    If there are open connections to the local database.
    If the database is already serving.

- **Notes:**
  - **Make sure to close all connections before starting the server.**
  - **After starting the server, no new connections to the local database will be allowed.**

<a id="neug.database.Database.stop_serving"></a>

### stop\_serving

```python
def stop_serving()
```

Stop the database server.
This method is used to stop the database server that was started by the `serve` method.
After calling this method, the database will switch back to the local mode, and new connections to the local
database will be allowed again.

- **Raises:**
  - **RuntimeError**
    If the database is not serving.

<a id="neug.database.Database.async_connect"></a>

### async\_connect

```python
def async_connect() -> AsyncConnection
```

Connect to the database asynchronously.

- **Returns:**
  - **AsyncConnection**
    An AsyncConnection object to interact with the database asynchronously.
- **Raises:**
  - **RuntimeError**
    If the database is closed or not opened.

<a id="neug.database.Database.close"></a>

### close

```python
def close()
```

Close the database connection.

<a id="neug.database.Database.load_builtin_dataset"></a>

### load\_builtin\_dataset

```python
def load_builtin_dataset(dataset_name: str) -> None
```

Load a builtin dataset into this database. If the database is in read-only mode, this method will raise an error.
If the schema of the dataset conflicts with the existing schema of the database, this method will raise an error.

- **Parameters:**
  - `dataset_name` (str)
    Name of the builtin dataset to load

- **Raises:**
  - **RuntimeError**
    If the database is closed or in read-only mode
  - **ValueError**
    If the dataset doesn't exist

<a id="neug.database.Database.from_builtin_dataset"></a>

### from\_builtin\_dataset

```python
@staticmethod
def from_builtin_dataset(dataset_name: str,
                         database_path: str = None,
                         mode: str = "read-write")
```

Create a Database instance from a builtin dataset.

- **Parameters:**
  - `dataset_name` (str)
    The name of the builtin dataset to use.
  - `database_path` (str)
    The path to the database file. If None, the database will be opened in memory mode.
  - `mode` (str)
    The mode to open the database, could be 'r', 'read', 'w', 'rw', 'write', 'readwrite'.
    Default is 'read-write'.

- **Returns:**
  - **Database**
    A Database instance with the builtin dataset loaded.
