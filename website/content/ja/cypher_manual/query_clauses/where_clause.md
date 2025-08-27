# WHERE句

WHERE句は、述語またはサブクエリに基づいて、前のクエリ操作で生成された結果をさらにフィルタリングするために使用されます。フィルタリングは主に論理式に基づいて行われ、これについては[式のセクション](../expression.md)で詳しく紹介します。WHERE句は指定された条件を満たすデータのみを出力します。

## プロパティによるフィルタリング

前の章では、`(a:person {name: 'marko'})`のような式を通じてノードとリレーションシップのプロパティのキー・値のペアを制限する方法を紹介しました。ここでは、同じ効果をWHERE句を通じて表現する方法をさらに補足します。

### ノードのプロパティでフィルタリング
```cypher
MATCH (a:person) 
WHERE a.name = 'marko' OR a.age > 27
RETURN a.name, a.age;
```

出力:
```
+-------------+------------+
| _0_a.name   |   _0_a.age |
+=============+============+
| marko       |         29 |
+-------------+------------+
| josh        |         32 |
+-------------+------------+
| peter       |         35 |
+-------------+------------+
```

### ノード/リレーションシップのプロパティでフィルタリング
```cypher
MATCH (a:person)-[b:knows]->(c:person) 
WHERE a.name = 'marko' AND b.weight = 1.0
RETURN a.name, b.weight;
```

出力:
```
+-------------+---------------+
| _0_a.name   |   _4_b.weight |
+=============+===============+
| marko       |             1 |
+-------------+---------------+
```

### 関連プロパティによるフィルタリング
```cypher
MATCH (a:person)-[b:knows]->(c:person) 
WHERE a.name <> c.name AND a.age > c.age 
RETURN a.name, a.age, c.name, c.age;
```

出力:
```
+-------------+------------+-------------+------------+
| _0_a.name   |   _0_a.age | _2_c.name   |   _2_c.age |
+=============+============+=============+============+
| marko       |         29 | vadas       |         27 |
+-------------+------------+-------------+------------+
```

## NULL によるフィルタリング

NULL 値はグラフデータのストレージおよび計算プロセスにおいて避けられないものです。これらの NULL 値を保持または除外するには、WHERE 句で `IS NULL` または `IS NOT NULL` を使用できます。

### NULL を含むプロパティデータのフィルタリング
```cypher
MATCH (a) 
WHERE a.age IS NULL 
RETURN a.name;
```

```
+-------------+
| _0_a.name   |
+=============+
| lop         |
+-------------+
| ripple      |
+-------------+
```

### NULL で任意のデータをフィルタリング
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:knows]->(b) 
WHERE b IS NULL 
RETURN a.name;
```

<!-- todo: optional match is not included in current pip package -->

### IS NOT NULL で任意のデータを除外
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:knows]->(b) 
WHERE b IS NOT NULL 
RETURN a.name;
```

## サブクエリを使用した WHERE

WHERE 句は、より複雑なフィルタリング操作を実行するためにサブクエリと一緒に使用することもできます。

### Exists パターン
```cypher
MATCH (a) 
WHERE (a)-[:knows]->(b) 
RETURN a.name;
```
このクエリは、`knows` リレーションシップを持つすべての `a.name` 値を返します。

### Not Exists パターン
```cypher
MATCH (a) 
WHERE NOT (a)-[:knows]->(b) 
RETURN a.name;
```
このクエリは、`knows` リレーションシップを持たないすべての `a.name` 値を返します。これは SQL の ANTI_JOIN セマンティクスに相当します。

<!-- todo: where subquery is unsupported yet -->