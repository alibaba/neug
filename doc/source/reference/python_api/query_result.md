<a id="neug.query_result"></a>

# Module neug.query\_result

The Neug result module.

<a id="neug.query_result.QueryResult"></a>

## QueryResult Objects

```python
class QueryResult(object)
```

QueryResult represents the result of a cypher query. Could be visited as a iterator.

It has the following methods to iterate over the results.
    - `hasNext()`: Returns True if there are more results to iterate over.
    - `getNext()`: Returns the next result as a list.
    - `length()`: Returns the total number of results.
    - `column_names()`: Returns the projected column names as strings.

```python

    >>> from neug import Database
    >>> db = Database("/tmp/test.db", mode="r")
    >>> conn = db.connect()
    >>> result = conn.execute('MATCH (n) RETURN n')
    >>> for row in result:
    >>>     print(row)

```

<a id="neug.query_result.QueryResult.column_names"></a>

### column\_names

```python
def column_names()
```

Return the projected column names as a list of strings.

<a id="neug.query_result.QueryResult.get_bolt_response"></a>

### get\_bolt\_response

```python
def get_bolt_response() -> str
```

Get the result in Bolt response format.
TODO(zhanglei,xiaoli): Make sure the format consistency with neo4j bolt response.

- **Returns:**
  - **str**
    The result in Bolt response format.
