# With 句

WITH は主に現在のデータに対してさらに projection または集約を行うために使用されます。次に、これら 2 つの観点から関連する使用方法を紹介します。

## 集約

集約は SQL の GROUP BY 操作と似ており、現在のデータをプロパティでグループ化し、各グループのデータに対して集約関数の対応する操作を実行します。Neug は現在、主流の集約関数をサポートしています。これには以下が含まれます：
- COUNT
- COUNT_STAR
- COLLECT
- SUM
- MIN
- MAX
- AVG

これらの関数については、[集約関数セクション](../expression/agg_func.md)で詳しく紹介します。

### 単一プロパティによる集約
```
Match (a) With label(a) as label, count(a.name) as cnt Return label, cnt;
```

<!-- todo: label is not included in current package -->

### 複数プロパティによる集約
```
Match (a)-[b:knows]->(c) With label(a) as a_label, label(c) as c_label, count(b) as cnt Return a_label, c_label, cnt;
```

### 複数の集約関数を適用する
```
Match (a)-[b:knows]->(c) With label(a) as a_label, label(c) as c_label, count(b) as cnt, sum(b.weight) as weights Return a_label, c_label, cnt, weights;
```

### 集約結果に基づいてフィルタリングする

```
Match (a:person) With label(a) as label, count(a.name) as cnt Where cnt > 2 Return label, cnt;
```

<!-- todo: 出力で label が null になる -->

## Projection

WITH のもう一つの一般的な使い方は、現在の結果をカラム単位でさらに projection することです。これは SQL での Column Pruning と同等で、後続のクエリで必要なカラムのみを出力します。

## Vertex データの Projection

```
Match (a:person {name: 'marko'})-[:knows]->(b:person)
With b
Match (b)-[:created]->(c)
Return c.name;
```

<!-- todo: 現在のパッケージには複数の match が含まれていない -->

## Vertex/Edge データの Projection

```
Match (a:person {name: 'marko'})-[k:knows]->(b:person)
With b, k
Match (b)-[:created]->(c)
Return k.weight, c.name;
```

## プロジェクトプロパティ
```
Match (a:person {name: 'marko'})-[:knows]->(b:person)
With a, b.age as b_age
Match (a)-[:created]->(c)
Where b_age > 20
Return c.name;
```

最初の Match で生成された b データのプロパティを投影し、Filter を通じてプロパティをフィルタリングし、最後に条件を満たすすべての c.name を出力する；