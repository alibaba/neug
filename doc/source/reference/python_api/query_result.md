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
    - `get_result_schema()`: Returns the schema of the result, which is a
        yaml string describing the structure of the result.

```python

    >>> from neug import Database
    >>> db = Database("/tmp/test.db", mode="r")
    >>> conn = db.connect()
    >>> result = conn.execute('MATCH (n) RETURN n')
    >>> for row in result:
    >>>     print(row)

```

<a id="neug.query_result.QueryResult.get_result_schema"></a>

### get\_result\_schema

```python
def get_result_schema() -> str
```

Get the schema of the result.

- **Returns:**
  - **str**
    The schema of the result, which is a yaml string describing the structure of the result.

<a id="neug.query_result.QueryResult.get_bolt_response"></a>

### get\_bolt\_response

```python
def get_bolt_response() -> str
```

Get the result in Bolt response format.

- **Returns:**
  - **str**
    The result in Bolt response format.
