<a id="neug.query_result"></a>

# Module neug.query\_result

The Neug result module.

<a id="neug.query_result.QueryResult"></a>

## QueryResult Objects

```javascript
class QueryResult
```

QueryResult represents the result of a cypher query. It supports the JavaScript iterator protocol (`for...of`),
and provides methods to iterate over the results.

It has the following methods to iterate over the results:
    - `hasNext()`: Returns true if there are more results to iterate over.
    - `getNext()`: Returns the next result as an array.
    - `getAt(index)`: Returns the result at the specified index.
    - `length()`: Returns the total number of results.
    - `columnNames()`: Returns the projected column names as strings.

```javascript

    const { Database } = require('neug');
    const db = new Database({ databasePath: '/tmp/test.db', mode: 'r' });
    const conn = db.connect();
    const result = conn.execute('MATCH (n) RETURN n');
    for (const row of result) {
        console.log(row);
    }

```

<a id="neug.query_result.QueryResult.constructor"></a>

### constructor

```javascript
constructor(nativeResult)
```

Initialize the QueryResult.

- **Parameters:**
  - `nativeResult` (object)
    The result of the query, returned by the query engine. It is a C++ object and is exported to Node.js via the native binding.

<a id="neug.query_result.QueryResult.hasNext"></a>

### hasNext

```javascript
hasNext() -> boolean
```

Check if there are more results available.

- **Returns:**
  - **boolean**
    True if there are more results, False otherwise.

<a id="neug.query_result.QueryResult.getNext"></a>

### getNext

```javascript
getNext() -> Array
```

Get the next row of results.

- **Returns:**
  - **Array**
    The next row as an array of values.

<a id="neug.query_result.QueryResult.getAt"></a>

### getAt

```javascript
getAt(index) -> Array
```

Get the result at the specified index.

- **Parameters:**
  - `index` (number)
    The index of the result to retrieve. Supports negative indexing (e.g., `-1` for the last row).

- **Returns:**
  - **Array**
    The row at the specified index.

- **Throws:**
  - **RangeError**
    If the index is out of range (after resolving negative indices).

<a id="neug.query_result.QueryResult.length"></a>

### length

```javascript
length() -> number
```

Get the total number of results.

- **Returns:**
  - **number**
    The number of results.

<a id="neug.query_result.QueryResult.columnNames"></a>

### columnNames

```javascript
columnNames() -> string[]
```

Return the projected column names as an array of strings.

<a id="neug.query_result.QueryResult.statusCode"></a>

### statusCode

```javascript
statusCode() -> number
```

Get the status code of the query result.

- **Returns:**
  - **number**
    0 for success, non-zero for error.

<a id="neug.query_result.QueryResult.statusMessage"></a>

### statusMessage

```javascript
statusMessage() -> string
```

Get the status message of the query result.

- **Returns:**
  - **string**
    The status message.

<a id="neug.query_result.QueryResult.getBoltResponse"></a>

### getBoltResponse

```javascript
getBoltResponse() -> string
```

Get the result in Bolt response format.

- **Returns:**
  - **string**
    The result in Bolt response format.

<a id="neug.query_result.QueryResult.close"></a>

### close

```javascript
close()
```

Close the query result and release resources.

<a id="neug.query_result.QueryResult.Symbol.iterator"></a>

### \[Symbol.iterator\]

```javascript
[Symbol.iterator]() -> Iterator
```

Makes QueryResult iterable with `for...of` loops. Each iteration yields a row as an array of values.

```javascript
const result = conn.execute('MATCH (p:Person) RETURN p.name, p.age');
for (const row of result) {
  console.log(`${row[0]}: ${row[1]}`);
}
```

- **Yields:**
  - **Array**
    Each row as an array of column values.
