<a id="neug.connection"></a>

# Module neug.connection

The Neug connection module.

<a id="neug.connection.Connection"></a>

## Connection Objects

```javascript
class Connection
```

Connection represents a logical connection to a NeuG database. User should use this class to interact
with the database, such as executing queries and managing transactions.
The connection is created by the `Database.connect` method, and should be closed by calling the `close` method
when it is no longer needed. If the database is closed, all the connections to the database will be closed automatically.

<a id="neug.connection.Connection.constructor"></a>

### constructor

```javascript
constructor(nativeConnection)
```

Initialize a Connection object. This is called internally by `Database.connect()`.
- **Parameters:**
  - `nativeConnection` (object)
    The underlying native connection object that provides the actual database connection.

<a id="neug.connection.Connection.isOpen"></a>

### isOpen

```javascript
get isOpen() -> boolean
```

Check if the connection is open.
- **Returns:**
  - **boolean**
    True if the connection is open, False otherwise.

<a id="neug.connection.Connection.close"></a>

### close

```javascript
close()
```

Close the connection.

<a id="neug.connection.Connection.execute"></a>

### execute

```javascript
execute(query, accessMode = '', parameters = null) -> QueryResult
```

Execute a cypher query on the database. User could specify multiple queries in a single string,
separated by semicolons. The query will be executed in the order they are specified.
If any query fails, the whole execution will be rolled back.
If the query is a DDL query, such as `CREATE NODE TABLE`, `CREATE REL TABLE`, `DROP TABLE`, etc., the database will be
modified accordingly.

For the details of the query syntax, please refer to the documentation of cypher manual.
The result of the query will be returned as a `QueryResult` object, which contains the result of
the query and the metadata of the query.
The QueryResult object supports the JavaScript iterator protocol (`for...of`), providing methods to iterate over the results,
such as `hasNext()` and `getNext()`.

If the query is a DDL or DML query, the result will be an empty `QueryResult` object.

Some of the cypher queries could change the state of the database, such as `CREATE NODE TABLE`, `INSERT`,
`UPDATE`, `DELETE`, etc. Other queries, such as `MATCH(n) RETURN n.id`, will not change the state of
the database, but will return the results of the query.

If the database is opened in read-only mode, any DDL or DML query will throw an error.
If the database is opened in read-write mode, all queries could be executed, and the state of the
database will be changed accordingly.

```javascript

    const { Database } = require('neug');
    const db = new Database({ databasePath: '/tmp/test.db', mode: 'w' });
    const conn = db.connect();
    conn.execute('CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));');
    conn.execute('CREATE REL TABLE KNOWS(FROM Person TO Person, weight DOUBLE);');
    conn.execute('COPY Person FROM "person.csv"');
    conn.execute('COPY KNOWS FROM "knows.csv" (from="Person", to="Person");');
    const res = conn.execute('MATCH(n) RETURN n.id');
    for (const row of res) {
        console.log(row);
    }
    const res2 = conn.execute('MATCH(p:Person)-[:KNOWS]->(q:Person) RETURN p.id, q.id LIMIT 10;');
    // submitting query with parameters
    const res3 = conn.execute(
        'MATCH (n:Person) WHERE n.id = $id RETURN n.name', 'read', { id: 12345 });

```

- **Parameters:**
  - `query` (string)
    The query to execute.
  - `accessMode` (string)
    The access mode of the query. It could be `read(r)`, `insert(i)`, `update(u)` (include deletion),
    or `schema(s)` for schema modifications. User should specify the correct access mode for the query
    to ensure the correctness of the database. If the access mode is not specified, it will be set to
    `update` by default. Supported access modes are:
    - `read`,`r`,`READ`,`R`: for read-only queries
    - `insert`,`i`,`INSERT`,`I`: for insert-only queries
    - `update`,`u`,`UPDATE`,`U`: for update queries (include deletion)
    - `schema`,`s`,`SCHEMA`,`S`: for schema modification operations
  - `parameters` (Object | null)
    The parameters to be used in the query. The parameters should be an object, where the keys are the
    parameter names, and the values are the parameter values. If no parameters are needed, it can be set to null.

- **Returns:**
  - `query_result` (QueryResult)
    The result of the query.

- **Throws:**
  - **Error**
    If the connection is closed, the access mode is invalid, or the query execution fails.

<a id="neug.connection.Connection.getSchema"></a>

### getSchema

```javascript
getSchema() -> string
```

Get the schema of the NeuG database.

**Returns**:

The schema of the NeuG database as a string.
