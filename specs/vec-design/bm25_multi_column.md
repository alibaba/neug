# 在 BM25 函数中支持多列属性

在 BM25 函数中支持多列属性，而不是在 Create FTS Index 语法中

BM25语法为：

```cypher
bm25([a.name, a.description], [10, 20], 'target')
```

bm25 同时支持两种表示方法：
- 单列（现状）：bm25(a.name, 'target')
- 多列（扩展）：bm25([a.name, a.description], [10, 20], 'target')

属性列表与权重列表必须一一对应。
权重列表可以是动态参数，例如：

```cypher
bm25([a.name, a.description], $weights, $target)
```

参数绑定后，权重数量必须与属性数量一致。

## 具体实现

在 FTSIndexScanFuncInput 保存本次查询的 property names，并增加 map 保存
property_name -> weight 映射。单列语法使用默认权重 1.0。

在 SearchImpl 中使用 FTS5 column filter，只 MATCH bm25 指定的单列或多列属性。
按照 Create FTS Index 指定的 Column 顺序绑定 bm25 weights：本次查询
指定的属性使用对应权重，索引中未指定的属性使用 0.0。

取消从 Create FTS Index options 获取 weight 的实现，修改为从 bm25 函数中获取
