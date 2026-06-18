# Return Clause

Return and With provide similar functionality, both for further aggregation or projection of data. The difference is that Return needs to return and display the processed results. Here we won't go into too much detail about Return's functionality itself, as you can refer to the usage in the [With Section](../with_clause). We mainly focus on result output and some common Return usage patterns.

## Return Nodes

### Return Nodes with Single Label
```
MATCH (a:Person) RETURN a;
```

The output shows each person node's internal ID (assigned by the graph database), label, and all properties:
```
+-------------------------------------------------------+
| a                                                     |
+=======================================================+
| {_ID: 0, _LABEL: Person, name: marko, age: 29} |
+-------------------------------------------------------+
| {_ID: 1, _LABEL: Person, name: vadas, age: 27} |
+-------------------------------------------------------+
| {_ID: 2, _LABEL: Person, name: josh, age: 32}  |
+-------------------------------------------------------+
| {_ID: 3, _LABEL: Person, name: peter, age: 35} |
+-------------------------------------------------------+
```

### Return Nodes with Multiple Labels
```
MATCH (a) RETURN a;
```

The output shows each node's internal ID, label, and all properties in its own node type:
```
+-----------------------------------------------------------------------------+
| a                                                                           |
+=============================================================================+
| {_ID: 0, _LABEL: Person, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: Person, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: Person, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: Person, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: Software, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: Software, name: ripple, lang: java} |
+-----------------------------------------------------------------------------+
```

## Return Relationships

```
MATCH (a:Person)-[k]->(b)
RETURN k;
```

The output includes the relationship's internal ID, label, all properties, and the source and destination node labels and IDs:
```
+--------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                    |
+======================================================================================================================================+
| {_ID: 1, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103806595072, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692224, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692225, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103809740800, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2} |
+--------------------------------------------------------------------------------------------------------------------------------------+
```

## Return Paths

### Return Repeated Paths

```
MATCH (a:Person)-[k*1..2]->(c)
RETURN k;
```

<!-- todo: add output here. -->

### Return All Nodes/Rels in Paths

```
MATCH (a:Person)-[k*1..2]->(c)
RETURN nodes(k) AS nodes, rels(k) AS rels;
```

<!-- todo: nodes or rels are unsupported yet -->

### Return Properties of Node/Rels in Paths
```
MATCH (a:Person)-[k*1..2]->(c)
RETURN properties(nodes(k), 'name') AS names, properties(rels(k), 'weight') AS weights;
```

<!-- todo: properties is unsupported yet -->

## Return with TopK

Return, OrderBy, Limit combination used for outputting TopK query results
```
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name
ORDER BY a.name ASC, b.name ASC
LIMIT 2;
```

output:
```
+-------------+-------------+
| _0_a.name   | _2_b.name   |
+=============+=============+
| marko       | josh        |
+-------------+-------------+
| marko       | vadas       |
+-------------+-------------+
```

## Return with Aggregation
Output aggregation results
```
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN label(a) AS a_label, label(b) AS b_label, count(*) AS cnt;
```

<!-- todo: output is incorrect -->

## Return with Distinct
Output non-duplicate results
```
MATCH (a)
RETURN DISTINCT label(a);
```

<!-- todo: label is not included in current pip package -->