# Return Clause

Return and With provide similar functionality, both for further aggregation or projection of data. The difference is that Return needs to return and display the processed results. Here we won't go into too much detail about Return's functionality itself, as you can refer to the usage in the [With Section](../with_clause). We mainly focus on result output and some common Return usage patterns.

## Return Nodes

### Return Nodes with Single Label
```
Match (a:person) Return a;
```

The output shows each person node's internal ID (assigned by the graph database), label, and all properties:
```
+-------------------------------------------------------+
| a                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29} |
+-------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27} |
+-------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}  |
+-------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35} |
+-------------------------------------------------------+
```

### Return Nodes with Multiple Labels
```
Match (a) Return a;
```

The output shows each node's internal ID, label, and all properties in its own node type:
```
+-----------------------------------------------------------------------------+
| a                                                                           |
+=============================================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: software, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: software, name: ripple, lang: java} |
+-----------------------------------------------------------------------------+
```

## Return Relationships

```
Match (a:person)-[k]->(b)
Return k;
```

The output includes the relationship's internal ID, label, all properties, and the source and destination node labels and IDs:
```
+--------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                    |
+======================================================================================================================================+
| {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103806595072, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692224, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692225, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103809740800, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2} |
+--------------------------------------------------------------------------------------------------------------------------------------+
```

## Return Paths

### Return Repeated Paths

```
Match (a:person)-[k*1..2]->(c)
Return k;
```

<!-- todo: add output here. -->

### Return All Nodes/Rels in Paths

```
Match (a:person)-[k*1..2]->(c)
Return nodes(k) as nodes, rels(k) as rels;
```

<!-- todo: nodes or rels are unsupported yet -->

### Return Properties of Node/Rels in Paths
```
Match (a:person)-[k*1..2]->(c)
Return properties(nodes(k), 'name') as names, properties(rels(k), 'weight') as weights;
```

<!-- todo: properties is unsupported yet -->

## Return with TopK

Return, OrderBy, Limit combination used for outputting TopK query results
```
Match (a:person)-[:knows]->(b:person)
Return a.name, b.name
Order By a.name ASC, b.name ASC
Limit 2;
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
Match (a:person)-[:knows]->(b:person)
Return label(a) as a_label, label(b) as b_label, count(*) as cnt;
```

<!-- todo: output is incorrect -->

## Return with Distinct
Output non-duplicate results
```
Match (a)
Return distinct label(a);
```

<!-- todo: label is not included in current pip package -->