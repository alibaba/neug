<a id="neug.database"></a>

# Module neug.database

The Neug database module.

<a id="neug.database.Database"></a>

## Database Objects

```javascript
class Database
```

The entrance of the Neug database. (Only AP mode is supported.)

This class is used to open a database connection and manage the database. User should use this class to
open a database connection, and then use the `connect` method to get a `Connection` object to interact with the database.

By passing an empty string as the database path, the database will be opened in memory mode.

The database could be opened with different modes(read-only or read-write) and different buffer strategies.

When the database is opened in read-only mode, other databases could also open the same database directory in
read-only mode, inside the same process or in different processes.
When the database is opened in read-write mode, no other databases could open the same database directory in
either read-only or read-write mode, inside the same process or in different processes.

When the database is closed, all the connections to the database will be closed automatically.

```javascript

    const { Database } = require('neug');
    const db = new Database({ databasePath: '/tmp/test.db', mode: 'w' });
    const conn = db.connect();

    // Use the connection to interact with the database
    conn.execute('CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));');
    conn.execute('CREATE REL TABLE KNOWS(FROM Person TO Person, weight DOUBLE);');

    // Import data from csv file.
    conn.execute('COPY Person FROM "person.csv"');
    conn.execute('COPY KNOWS FROM "knows.csv" (from="Person", to="Person");');

    const res = conn.execute('MATCH(n) RETURN n.id;');
    for (const row of res) {
        console.log(row);
    }

```

<a id="neug.database.Database.constructor"></a>

### constructor

```javascript
constructor(options = {}) {
  const {
    databasePath = null,
    mode = 'read-write',
    maxThreadNum = 0,
    checkpointOnClose = true,
    bufferStrategy = 'M_FULL',
  } = options;
}
```

Open a database.

- **Parameters:**
  - `options` (Object)
    Database configuration options.
  - `options.databasePath` (string | null)
    Path to the database file. Default is `null`. If it is set to empty string (`''`) or `null`, the database will be opened in memory mode.
    Note that in memory mode, the database will not be persisted to disk, and all data will be
    lost when the program exits.
    **Note**: `null` cannot be combined with read-only mode; `''` (empty string) can.
  - `options.mode` (string)
    Mode to open the database. Supported values: 'r', 'read', 'read-only', 'read_only', 'w', 'rw', 'write', 'readwrite', 'read-write', 'read_write'. Default is 'read-write'.
  - `options.maxThreadNum` (number)
    Maximum database thread count. The default `0` auto-selects from hardware
    concurrency and falls back to `1` if the runtime cannot detect it.
  - `options.checkpointOnClose` (boolean)
    Whether to automatically create a checkpoint when the database is closed. Default is true.
    If false, no checkpoint is created automatically when close the database.
  - `options.bufferStrategy` (string)
    Buffer strategy to use for the database, could be 'InMemory' (or 'M_FULL'), 'SyncToFile' (or 'M_LAZY')
    or 'HugePagePreferred' (or 'M_HUGE'). Default is 'M_FULL'.
    - 'InMemory' / 'M_FULL': The database will be opened fully in memory, and the changes will not be
    persisted to disk until checkpoint is created.
    - 'SyncToFile' / 'M_LAZY': The database will be opened in memory on demand, suitable for large databases
    that cannot fit into memory. Also changes will not be persisted to disk until checkpoint is created.
    - 'HugePagePreferred' / 'M_HUGE': Similar to 'InMemory', but it will try to use huge pages for memory
    allocation, which may improve performance for large databases.

- **Throws:**
  - **Error** (ERR_INVALID_PATH)
    If the database path contains illegal characters.
  - **Error** (ERR_INVALID_ARGUMENT)
    If the mode is not one of the supported modes.
    If maxThreadNum exceeds the number of CPU cores.
    If in-memory mode is opened with read-only mode.
  - **Error** (ERR_CONFIG_INVALID)
    If maxThreadNum is negative.

<a id="neug.database.Database.version"></a>

### version

```javascript
get version() -> string
```

Get the version of the database.

<a id="neug.database.Database.mode"></a>

### mode

```javascript
get mode() -> string
```

Get the mode of the database.

- **Returns:**
  - **string**
    The mode of the database, could be 'r', 'read', 'w', 'rw', 'write', 'readwrite', 'read-write', 'read-only'.

<a id="neug.database.Database.connect"></a>

### connect

```javascript
connect() -> Connection
```

Connect to the database.

- **Returns:**
  - **Connection**
    A Connection object to interact with the database.
- **Throws:**
  - **Error**
    If the database is closed.

<a id="neug.database.Database.asyncConnect"></a>

### asyncConnect

```javascript
asyncConnect() -> AsyncConnection
```

Connect to the database asynchronously.

- **Returns:**
  - **AsyncConnection**
    An AsyncConnection object to interact with the database asynchronously.
- **Throws:**
  - **Error**
    If the database is closed.

<a id="neug.database.Database.close"></a>

### close

```javascript
close()
```

Close the database connection and release all resources.
All open connections and async connections will be closed automatically.
