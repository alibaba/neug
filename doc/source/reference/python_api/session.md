<a id="neug.session"></a>

# Module neug.session

<a id="neug.session.QueryResult"></a>

## Session Objects

```python
class Session()
```

Session is a class that connects to the NeuG server. User could use it just like a normal NeuG Connection,
while it is actually a session that connects to the NeuG server.

A NeuG Server could be started with `Database::serve()` method, and it will listen to the specified endpoint.

```python

    >>> from neug import Database
    >>> db = Database("/tmp/test.db", mode="w")
    >>> db.serve(port = 10000, host = "localhost")

```

And on another python shell, user could connect to the NeuG server with the following code:

```python

    >>> from neug import Session
    >>> sess = Session('http://localhost:10000', timeout='10s')
    >>> sess.execute('MATCH(n) return count(n)')

```

The query will be sent to the NeuG http server, and the result will be returned as a response.
The session will automatically handle the connection and disconnection to the server.

To stop the NeuG server, user could send terminal signal to the process.
To close the session, user could call the `close()` method.

<a id="neug.session.Session.__init__"></a>

### \_\_init\_\_

```python
def __init__(endpoint: str = "http://localhost:10000",
             timeout: str = "10s",
             num_threads: int = 1)
```

Initialize a session with the given endpoint and timeout.

**Arguments**:

- `endpoint`: The endpoint URL for the session.
- `timeout`: The timeout duration for the session.

<a id="neug.session.Session.open"></a>

### open

```python
@staticmethod
def open(endpoint: str = "http://localhost:10000",
         timeout: str = "10s",
         num_threads: int = 1)
```

Open a session with the given endpoint and timeout.

**Arguments**:

- `endpoint`: The endpoint URL for the session.
- `timeout`: The timeout duration for the session.

**Returns**:

An instance of the Session class.

<a id="neug.session.Session.close"></a>

### close

```python
def close()
```

Close the session. An active explicit transaction is rolled back on a
best-effort basis.

<a id="neug.session.Session.has_active_transaction"></a>

### has\_active\_transaction

```python
@property
def has_active_transaction() -> bool
```

Whether this session has an active explicit transaction.

The property remains true while a failed transaction is rollback-only.
Call `rollback()` to discard that transaction before issuing another
query or beginning a new transaction.

<a id="neug.session.Session.begin_transaction"></a>

### begin\_transaction

```python
def begin_transaction(read_only: bool = False)
```

Begin an explicit transaction.

- **Parameters:**
  - `read_only` (bool)
    Pin one read view and reject writes when true. The default starts
    a read-write transaction with a private COW view.

- **Raises:**
  - **ConnectionError**
    If the session is closed or the service cannot be reached.
  - **RuntimeError**
    If the session already has an active transaction or the service
    rejects the begin request.

<a id="neug.session.Session.commit"></a>

### commit

```python
def commit()
```

Commit the active explicit transaction.

A rollback-only transaction must be rolled back instead.

<a id="neug.session.Session.rollback"></a>

### rollback

```python
def rollback()
```

Roll back the active explicit transaction and return to auto-commit.

<a id="neug.session.Session.execute"></a>

### execute

```python
def execute(query: str,
            access_mode: str = "",
            parameters: dict = None) -> QueryResult
```

Execute a query on the NeuG server.

While an explicit transaction is active, the query runs in that
transaction. A failure reported by the service leaves it rollback-only;
call `rollback()` before issuing another query. Client-side validation
errors, such as an invalid `access_mode`, do not change the transaction
state.

**Arguments**:

- `query`: The query string to be executed.
- `access_mode`: The access mode for the query. When omitted, NeuG infers it
  from the query text. Supported modes are:
- `read` or `r`: Read-only queries
- `insert` or `i`: Insert-only operations
- `update` or `u`: Update/delete operations
- `schema` or `s`: Schema modification operations
- `parameters`: Optional dict of query parameters.

**Returns**:

The result of the query execution.

<a id="neug.session.Session.service_status"></a>

### service\_status

```python
def service_status()
```

Get the service status of the NeuG server.

**Returns**:

The status of the NeuG server.

<a id="neug.session.Session.get_schema"></a>

### get\_schema

```python
def get_schema()
```

Get the schema of the NeuG database.

**Returns**:

The schema of the NeuG database.

<a id="neug.session.Session.timeout"></a>

### timeout

```python
@property
def timeout()
```

Get the timeout duration for the session, in seconds.
