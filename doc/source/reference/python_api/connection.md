<a id="neug.connection"></a>

# Module neug.connection

The Neug connection module.

<a id="neug.connection.annotations"></a>

## Connection Objects

```python
class Connection(object)
```

Connection represents a logical connection to a database. User should use this class to interact
with the database, such as executing queries and managing transactions.
The connection is created by the `Database.connect` method, and should be closed by calling the `close` method
when it is no longer needed. If the database is closed, all the connections to the database will be closed automatically.

<a id="neug.connection.Connection.__init__"></a>

### \_\_init\_\_

```python
def __init__(py_connection)
```

Initialize a Connection object.
- **Parameters:**
  - `py_connection` (PyConnection)
    The underlying c++ connection object that provides the actual database connection.

<a id="neug.connection.Connection.is_open"></a>

### is\_open

```python
@property
def is_open() -> bool
```

Check if the connection is open.
- **Returns:**
  - **bool**
    True if the connection is open, False otherwise.

<a id="neug.connection.Connection.close"></a>

### close

```python
def close()
```

Close the connection. An active explicit transaction is rolled back.

<a id="neug.connection.Connection.has_active_transaction"></a>

### has\_active\_transaction

```python
@property
def has_active_transaction() -> bool
```

Whether this connection has an active explicit transaction.

The property remains true while a failed transaction is rollback-only. Call
`rollback()` to return the connection to auto-commit mode.

<a id="neug.connection.Connection.begin_transaction"></a>

### begin\_transaction

```python
def begin_transaction(read_only: bool = False)
```

Begin an explicit embedded AP transaction.

- **Parameters:**
  - `read_only` (bool): Pin one read view and reject writes when true. The
    default starts a read-write transaction with a private COW view.

- **Raises:**
  - **RuntimeError:** If the connection is closed or already has an active
    transaction.

<a id="neug.connection.Connection.commit"></a>

### commit

```python
def commit()
```

Commit the active explicit transaction. A rollback-only transaction must be
rolled back instead.

<a id="neug.connection.Connection.rollback"></a>

### rollback

```python
def rollback()
```

Roll back the active explicit transaction and return to auto-commit.

<a id="neug.connection.Connection.execute"></a>

### execute

```python
def execute(query: str,
            access_mode="",
            parameters: Optional[Dict[str, Any]] = None) -> QueryResult
```

Execute a cypher query on the database. User could specify multiple queries in a single string,
separated by semicolons. The query will be executed in the order they are specified.
If any query fails, the whole execution will be rolled back.
If the query is a DDL query, such as `CREATE NODE TABLE`, `CREATE REL TABLE`, `DROP TABLE`, etc., the database will be
modified accordingly.

For the details of the query syntax, please refer to the documentation of cypher manual.
The result of the query will be returned as a `QueryResult` object, which contains the result of
the query and the metadata of the query.
The QueryResult object is like an iterator, providing methods to iterate over the results,
such as `__iter__` and `__next__`.

If the query is a DDL or DML query, the result will be an empty `QueryResult` object.

Inside an explicit transaction, a query that reaches the database engine and
fails leaves the transaction rollback-only. Call `rollback()` before executing
another query. Client-side validation errors that occur before execution, such
as an invalid `access_mode`, do not change the transaction state.

Some of the cypher queries could change the state of the database, such as `CREATE NODE TABLE`, `INSERT`,
`UPDATE`, `DELETE`, etc. Other queries, such as `MATCH(n) RETURN n.id`, will not change the state of
the database, but will return the results of the query.

If the database is opened in read-only mode, any DDL or DML query will raise an exception.
If the database is opened in read-write mode, all queries could be executed, and the state of the
database will be changed accordingly.

```python

    >>> from neug import Database
    >>> db = Database("/tmp/test.db", mode="w")
    >>> conn = db.connect()
    >>> res = conn.execute('CREATE NODE TABLE Person(id INT64, name STRING);')
    >>> res = conn.execute('CREATE REL TABLE KNOWS(FROM Person TO Person, weight DOUBLE);')
    >>> res = conn.execute('COPY Person FROM "person.csv"')
    >>> res = conn.execute('COPY KNOWS FROM "knows.csv" (from="Person", to="Person");')
    >>> res = conn.execute('MATCH(n) RETURN n.id')
    >>> for record in res:
    >>>    print(record)
    >>> res = conn.execute('MATCH(p:Person)-[:KNOWS]->(q:Person) RETURN p.id, q.id LIMIT 10;')
    >>> # submitting query with parameters
    >>> res = conn.execute(
        'MATCH (n:Person) WHERE n.id = $id RETURN n.name', access_mode='r', parameters={'id': 12345})

```

- **Parameters:**
  - `query` (str)
    The query to execute.
  - `access_mode` (str)
    The access mode of the query. It could be `read(r)`, `insert(i)`, `update(u)` (include deletion),
    or `schema(s)` for schema modifications. User should specify the correct access mode for the query
    to ensure the correctness of the database. If the access mode is not specified, NeuG infers it
    from the query text. Supported access modes are:
    - `read`,`r`,`READ`,`R`: for read-only queries
    - `insert`,`i`,`INSERT`,`I`: for insert-only queries
    - `update`,`u`,`UPDATE`,`U`: for update queries (include deletion)
    - `schema`,`s`,`SCHEMA`,`S`: for schema modification operations
  - `parameters` (dict[str, Any] | None)
    The parameters to be used in the query. The parameters should be a dictionary, where the keys are the
    parameter names, and the values are the parameter values. If no parameters are needed, it can be set to None.

- **Returns:**
  - `query_result` (QueryResult)
    The result of the query.

<a id="neug.connection.Connection.get_schema"></a>

### get\_schema

```python
def get_schema()
```

Get the schema of the NeuG database.

**Returns**:

The schema of the NeuG database.
