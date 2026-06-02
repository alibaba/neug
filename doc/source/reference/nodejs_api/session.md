<a id="neug.session"></a>

# Module neug.session

The Neug session module.

<a id="neug.session.Session"></a>

## Session Objects

```javascript
class Session
```

Session is a class that connects to the NeuG server. User could use it just like a normal NeuG Connection,
while it is actually a session that connects to the NeuG server via HTTP.

A NeuG Server could be started with `Database.serve()` method, and it will listen to the specified endpoint.

```javascript

    const { Database } = require('neug');
    const db = new Database({ databasePath: '/tmp/test.db', mode: 'w' });
    db.serve({ port: 10000, host: 'localhost' });

```

And in another process, user could connect to the NeuG server with the following code:

```javascript

    const { Session } = require('neug');
    const session = await Session.open({ endpoint: 'http://localhost:10000', timeout: '10s' });
    const result = await session.execute('MATCH(n) RETURN count(n)');
    for (const row of result) {
        console.log(row);
    }

```

The query will be sent to the NeuG HTTP server, and the result will be returned as a response.
The session will automatically handle the connection and disconnection to the server.

To stop the NeuG server, user could send terminal signal to the process.
To close the session, user could call the `close()` method.

<a id="neug.session.Session.constructor"></a>

### constructor

```javascript
constructor(options = {}) {
  const {
    endpoint = 'http://localhost:10000',
    timeout = '10s',
  } = options;
}
```

Initialize a session with the given endpoint and timeout.

- **Parameters:**
  - `options` (Object)
    Session configuration options.
  - `options.endpoint` (string)
    The endpoint URL for the session. Default is 'http://localhost:10000'.
  - `options.timeout` (string | number)
    The timeout duration for the session. Could be a string like '10s', '500ms', or a number (seconds).
    Default is '10s'.

- **Note:**
  Unlike the Python API, `num_threads` is not supported in the NodeJS Session because Node.js operates on a single-threaded event loop model.

<a id="neug.session.Session.open"></a>

### open

```javascript
static async open(options = {}) -> Promise<Session>
```

Open a session with the given endpoint and timeout. This static method performs a connectivity check
before returning the session, ensuring the server is ready.

- **Parameters:**
  - `options` (Object)
    Session configuration options (same as constructor).

- **Returns:**
  - **Promise\<Session\>**
    A connected Session instance.

<a id="neug.session.Session.close"></a>

### close

```javascript
close()
```

Close the session.

<a id="neug.session.Session.execute"></a>

### execute

```javascript
async execute(query, accessMode = '', parameters = null) -> Promise<QueryResult>
```

Execute a query on the NeuG server.

- **Parameters:**
  - `query` (string)
    The query string to be executed.
  - `accessMode` (string)
    The access mode for the query. Supported modes are:
    - `read` or `r`: Read-only queries
    - `insert` or `i`: Insert-only operations
    - `update` or `u`: Update/delete operations (default)
    - `schema` or `s`: Schema modification operations
  - `parameters` (Object | null)
    Optional object of query parameters.

- **Returns:**
  - **Promise\<QueryResult\>**
    The result of the query execution.

<a id="neug.session.Session.serviceStatus"></a>

### serviceStatus

```javascript
async serviceStatus() -> Promise<Object>
```

Get the service status of the NeuG server.

- **Returns:**
  - **Promise\<Object\>**
    The status of the NeuG server as a JSON object.

<a id="neug.session.Session.getSchema"></a>

### getSchema

```javascript
async getSchema() -> Promise<Object>
```

Get the schema of the NeuG database from the server.

- **Returns:**
  - **Promise\<Object\>**
    The schema of the NeuG database as a JSON object.
