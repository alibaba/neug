<a id="neug.connection"></a>

# Module neug.connection

The Neug connection module.

<a id="neug.connection.Any"></a>

## Connection Objects

```python
class Connection(object)
```

Connection represents a logical connection to a database. User should use this class to interact
with the database, such as executing queries and managing transactions.
The connection is created by the `Database.connect` method, and should be closed by calling the `close` method
when it is no longer needed. If the database is closed, all the connections to the database will be closed automatically.

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

Close the connection.

<a id="neug.connection.Connection.execute"></a>

### execute

```python
def execute(query: str,
            access_mode="",
            parameters: dict[str, Any] | None = None) -> QueryResult
```

Execute a cypher query on the database. User could specify multiple queries in a single string,
separated by semicolons. The query will be executed in the order they are specified.
If any query fails, the whole execution will be rolled back.
If the query is a DDL query, such as `CREATE TABLE`, `DROP TABLE`, etc., the database will be
modified accordingly.

For the details of the query syntax, please refer to the documentation of cypher manual.
The result of the query will be returned as a `QueryResult` object, which contains the result of
the query and the metadata of the query.
The QueryResult object is like an iterator, providing methods to iterate over the results,
such as `__iter__` and `__next__`.

If the query is a DDL or DML query, the result will be an empty `QueryResult` object.

Some of the cypher queries could change the state of the database, such as `CREATE TABLE`, `INSERT`,
`UPDATE`, `DELETE`, etc. Other queries, such as `MATCH(n) RETURN n.id`, will not change the state of
the database, but will return the results of the query.

If the database is opened in read-only mode, any DDL or DML query will raise an exception.
If the database is opened in read-write mode, all queries could be executed, and the state of the
database will be changed accordingly.

```python

    >>> from neug import Database
    >>> db = Database("/tmp/test.db", mode="w")
    >>> conn = db.connect()
    >>> res = conn.execute('CREATE TABLE person(id INT64, name STRING);')
    >>> res = conn.execute('CREATE TABLE knows(FROM person TO person, weight DOUBLE);')
    >>> res = conn.execute('COPY person FROM "person.csv"')
    >>> res = conn.execute('COPY knows FROM "knows.csv" (from="person", to="person");')
    >>> res = conn.execute('MATCH(n) RETURN n.id')
    >>> for record in res:
    >>>    print(record)
    >>> res = conn.execute('MATCH(p:person)-[knows]->(q:person) RETURN p.id, q.id LIMIT 10;')
    >>> # submitting query with parameters
    >>> res = conn.execute(
        'MATCH (n:person) WHERE n.id = $id RETURN n.name', access_mode='r', parameters={'id': 12345})

```

- **Parameters:**
  - `query` (str)
    The query to execute.
  - `access_mode` (str)
    The access mode of the query. It could be `read(r)`, `insert(i)`, `update(u)` (include deletion),
    or `schema(s)` for schema modifications. User should specify the correct access mode for the query
    to ensure the correctness of the database. If the access mode is not specified, it will be set to
    `update` by default. Supported access modes are:
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

