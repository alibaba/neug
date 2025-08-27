# With 子句

WITH 主要用于对当前数据进行进一步的投影或聚合操作。接下来，我们将从这两个方面介绍相关的用法。

## 聚合操作

聚合操作类似于 SQL 中的 GROUP BY 操作，它根据属性对当前数据进行分组，并对每组数据执行相应的聚合函数操作。Neug 目前支持主流的聚合函数，包括：
- COUNT
- COUNT_STAR
- COLLECT
- SUM
- MIN
- MAX
- AVG

我们将在[聚合函数章节](../expression/agg_func.md)中详细介绍这些函数。

### 按单个属性聚合
```
Match (a) With label(a) as label, count(a.name) as cnt Return label, cnt;
```

<!-- todo: label is not included in current package -->

### 按多个属性聚合
```
Match (a)-[b:knows]->(c) With label(a) as a_label, label(c) as c_label, count(b) as cnt Return a_label, c_label, cnt;
```

### 应用多个聚合函数
```
Match (a)-[b:knows]->(c) With label(a) as a_label, label(c) as c_label, count(b) as cnt, sum(b.weight) as weights Return a_label, c_label, cnt, weights;
```

### 基于聚合结果进行过滤

```
Match (a:person) With label(a) as label, count(a.name) as cnt Where cnt > 2 Return label, cnt;
```

<!-- todo: 输出中 label 为 null -->

## 投影

WITH 的另一个常见用法是通过列进行进一步投影当前结果，这相当于 SQL 中的 Column Pruning，只输出后续查询所需的列。

## 投影顶点数据

```
Match (a:person {name: 'marko'})-[:knows]->(b:person)
With b
Match (b)-[:created]->(c)
Return c.name;
```

<!-- todo: 当前包中未包含 multiple match -->

## 投影顶点/边数据

```
Match (a:person {name: 'marko'})-[k:knows]->(b:person)
With b, k
Match (b)-[:created]->(c)
Return k.weight, c.name;
```

## 项目属性

```
Match (a:person {name: 'marko'})-[:knows]->(b:person)
With a, b.age as b_age
Match (a)-[:created]->(c)
Where b_age > 20
Return c.name;
```

投影第一个 Match 生成的 b 数据的属性，通过 Filter 过滤属性，最后输出所有满足条件的 c.name；