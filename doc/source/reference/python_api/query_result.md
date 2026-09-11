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

<a id="neug.query_result.QueryResult.__init__"></a>

### \_\_init\_\_

```python
def __init__(result)
```

Initialize the QueryResult.

- **Parameters:**
  - `result` (PyQueryResult)
    The result of the query, returned by the query engine. It is a C++ object and is exported to python via pybind.

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

<a id="neug.query_result.QueryResult.has_profile_result"></a>

### has\_profile\_result

```python
def has_profile_result() -> bool
```

Check if profile result is available.

- **Returns:**
  - **bool**
    True if the query was executed in PROFILE or EXPLAIN mode,
    False for normal queries.

<a id="neug.query_result.QueryResult.get_profile_text"></a>

### get\_profile\_text

```python
def get_profile_text() -> str
```

Get human-readable PROFILE/EXPLAIN text output.

Suitable for CLI output and debugging. Returns empty string if
no profile result is available.

- **Returns:**
  - **str**
    Formatted execution tree with operator timings and row counts.
    Returns empty string if no profile result available.

<a id="neug.query_result.QueryResult.get_profile_metrics"></a>

### get\_profile\_metrics

```python
def get_profile_metrics() -> dict
```

Return detailed PROFILE or EXPLAIN metrics as a Python dictionary.

The result contains complete execution plan metrics, including timing
and output information for each operator in the query execution tree.
Returns an empty dict if no profile result is available.

```python

    {
        "total_elapsed_ms": float,
        "total_output_rows": int,
        "operators": [
            {
                "operator_id": int,
                "parent_id": int,
                "operator_name": str,
                "elapsed_ms": float,
                "output_rows": int,
                "child_ids": [int],
            }
        ],
    }

```

<a id="neug.query_result.QueryResult.to_arrow"></a>

### to\_arrow

```python
def to_arrow()
```

Convert the result to an Arrow table.

- **Returns:**
  - **pyarrow.Table**
    The result converted to an Arrow table.
