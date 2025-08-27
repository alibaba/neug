# Union 句

Neug における Union 演算子は、複数のサブクエリの結果を 1 つの結果セットに結合するために使用されます。参加するすべてのサブクエリは、一貫した出力スキーマ（列数、列名、データ型が一致している）を生成する必要があります。

現在、Neug は重複排除を行わず結果を連結する `UNION ALL` のみをサポートしています。利用可能な構文は以下の 2 種類です：

- **標準 Union**：[Kùzu](https://docs.kuzudb.com/cypher/query-clauses/union/) の標準構文と同様。
- **Call Union**：[Neo4j](https://neo4j.com/docs/cypher-manual/current/subqueries/call-subquery/#call-post-union) にインスパイアされた拡張形式で、より柔軟なクエリ構成を可能にします。

## 標準的な Union

標準的な使用方法では、`UNION ALL` は複数のサブクエリの出力をマージするために使用されます。Union は終端のオペレータとして出現し、それ以前のすべてのブランチの出力を結合します。

```cypher
MATCH (n {name: 'marko'}) RETURN n.age
UNION ALL
MATCH (n {name: 'josh'}) RETURN n.age;
```

## Call Union

[Neo4j](https://neo4j.com/docs/cypher-manual/current/subqueries/call-subquery/#call-post-union)にインスパイアされ、Neugはパラメータ化された入力を持つ`CALL {}`ブロックを通じてunionのセマンティクスを拡張し、より表現力豊かでモジュール化されたクエリ構成を可能にします。この構造により以下のことが可能になります：
- union後の追加ロジックの実行
- unionブランチ間での事前計算されたコンテキスト（例：バインド変数）の共有

例：
```cypher
MATCH (person:person {id: 123})
WITH person
CALL (person) {
  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight > 1.0
  RETURN friend

  UNION ALL

  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight < 1.0
  RETURN friend
}
RETURN friend.id, friend.name

```

このクエリは3つのステージに分解できます：
- **PreQuery**: `CALL {}`ブロックより前に実行される（例：MATCH (person)）、unionサブクエリ間で共有される事前計算コンテキストを実行します
- **Union Subqueries**: `CALL {}`ブロック内で定義される。各ブランチは共有コンテキスト（例：person）にアクセスできます
- **PostQuery**: `CALL {}`の後に実行され、統合された結果セットを消費します（例：RETURN friend.id, friend.name）

`CALL (person)`構文は外部変数をunionスコープに注入し、各サブクエリが共有コンテキストにアクセスして操作することを可能にします。このパターンは、同じ入力エンティティに対して複数のフィルタリングやトラバーサル戦略を適用する際に特に有用です。