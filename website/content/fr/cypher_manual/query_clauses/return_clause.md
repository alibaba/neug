# Clause Return

Return et With fournissent des fonctionnalités similaires, toutes deux permettant une agrégation ou une projection supplémentaire des données. La différence réside dans le fait que Return doit retourner et afficher les résultats traités. Nous n'entrerons pas ici dans les détails concernant la fonctionnalité de Return elle-même, car vous pouvez vous référer à son utilisation dans la [Section With](with_clause.md). Nous nous concentrerons principalement sur la sortie des résultats et quelques modèles d'utilisation courants de Return.

## Return Nodes

### Retourner les Nodes avec un Seul Label
```
Match (a:person) Return a;
```

La sortie affiche l'ID interne de chaque node person (assigné par la base de données graphe), le label, et toutes les propriétés :
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

### Retourner des Nodes avec plusieurs Labels
```
Match (a) Return a;
```

La sortie affiche l'ID interne de chaque node, son label, et toutes ses propriétés dans son propre type de node :
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

## Retourner les relations

```
Match (a:person)-[k]->(b)
Return k;
```

La sortie inclut l'ID interne de la relation, son label, toutes ses propriétés, ainsi que les labels et IDs des nœuds source et destination :

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

Combinaison de Return, OrderBy et Limit utilisée pour retourner les résultats d'une requête TopK

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

## Return avec Aggregation
Renvoie les résultats de l'agrégation
```
Match (a:person)-[:knows]->(b:person)
Return label(a) as a_label, label(b) as b_label, count(*) as cnt;
```

<!-- todo: output is incorrect -->

## Return avec Distinct
Renvoie les résultats sans doublons
```
Match (a)
Return distinct label(a);
```

<!-- todo: label is not included in current pip package -->