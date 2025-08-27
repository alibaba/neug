# シンタックス概要

## Cypherとは？

Cypherは、グラフデータベース専用に設計された宣言型グラフクエリ言語です。グラフデータのクエリ、操作、管理を直感的かつ表現力豊かに行うための方法を提供します。我々の実装は、グラフクエリ言語のオープンスタンダードである[OpenCypher](https://opencypher.org/)仕様に基づいています。

## SQLとの主な違い

SQLがテーブルと行を持つリレーショナルデータベース向けに設計されているのに対し、Cypherはノード、リレーションシップ、プロパティを持つグラフデータベースに最適化されています：

- **構造**: SQLはテーブルとJOINを使用する；Cypherはノード、リレーションシップ、パターンを使用する
- **パターンマッチング**: SQLは明示的なJOINを必要とする；Cypherはパターンマッチング構文を使用する
- **トラバーサル**: SQLはマルチホップクエリに複雑なJOINを必要とする；Cypherはパストラバーサルを自然にサポートする
- **可読性**: CypherのASCIIアート構文により、グラフパターンが視覚的に直感的に表現できる

## Neug で Cypher を使ってできること

Neug では、Cypher クエリのことを **Statement** と呼びます。Statement は複数の **Clause** で構成されます。例えば、以下のクエリでは：

```cypher
MATCH (p:person)
WHERE p.age = '29'
RETURN p.name as name;
```

`MATCH`、`WHERE`、`RETURN` の各コンポーネントは Clause と呼ばれており、これらはグラフデータベース操作の基本的な論理ユニットです。

OpenCypher をベースに、Neug のグラフデータベースを管理するための一連の Statement 構文を定義しています。これには以下が含まれます：

### スキーマ管理 (DDL)

Neug は主に Schema-Strict なグラフデータシナリオをターゲットとしており、すべてのデータが事前に定義されたスキーマ仕様に準拠している必要があります。これは従来の SQL に似ていますが、グラフデータではノードとリレーションシップの構造がより複雑であり、それらもまた事前に定義されたスキーマ要件に従う必要があります。

例えば、以下のスキーマグラフを考えてみましょう：

<img src="figures/modern_schema.png" alt="Modern Schema Graph" style="display: block; margin: 2em auto; max-width: 500px;">

上記のスキーマグラフは、以下の Cypher ステートメントによって作成できます：

```cypher
// スキーマ定義の例
CREATE NODE TABLE person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);

CREATE NODE TABLE software (
    name STRING,
    lang STRING,
    PRIMARY KEY (name)
);

CREATE REL TABLE knows (
    FROM person TO person,
    weight DOUBLE
);

CREATE REL TABLE created (
    FROM person TO software,
    weight DOUBLE
);
```

**スキーマ準拠のクエリ：**  
以下のクエリでは、頂点ラベル `person` およびエッジラベル `(person-knows->person)` はともに上記で定義されたスキーマ制約に準拠しています。`person` ノードは `age` および `name` のプロパティを持ち、`age` プロパティは INT32 型であり、定数 18 と比較可能です。したがって、このクエリはすべてのスキーマ制約を満たしており、有効です：

```cypher
MATCH (p:person)-[:knows]->(f:person)
WHERE p.age > 18
RETURN p.name, f.name;
```

**スキーマに準拠しないクエリ（エラーになる）：**  
このクエリで指定されているエッジラベル `(person-follows->person)` はスキーマ上に存在しないため、無効であり、「Table `follows` does not exist」というエラーになります。

```cypher
MATCH (p:person)-[:follows]->(m:person)
RETURN p.name;
```

上記のようにスキーマグラフを作成するための一連の構文を、我々は DDL（Data Definition Language）と呼びます。これ以降のすべてのデータ更新およびクエリ操作は、現在の DDL によって定義されたスキーマ仕様に準拠している必要があります。詳細については、[DDL セクション](ddl_clause.md)で紹介します。

### データクエリ (DQL)

また、Transactional Processing (TP) と Analytical Processing (AP) の両方のクエリ要件を満たすことができるクエリ構文も定義しています。

例えば、以下のクエリを使ってグラフデータベース内のすべての三角形パターンをクエリできます：

```cypher
MATCH (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(c:person)
WHERE a.name < c.name
RETURN a.name, b.name, c.name;
```

それぞれの `MATCH`、`WHERE`、`RETURN` は Clause と呼ばれます。これらはグラフデータ操作の基本単位です。ここで、`MATCH` 操作は主に三角形パターンを構成するすべてのデータをマッチングし、`WHERE` はパターンデータをさらにフィルタリングして重複排除を保証し、`RETURN` 操作は名前の投影を行い最終結果を出力します。`MATCH` 操作は主にグラフパターンマッチングを完了させ、`WHERE`/`RETURN` 操作は主に SQL に似たリレーショナル操作を実行します。これらの Clause については、[DQL セクション](query_clauses.md)で詳しく紹介します。

Clause 操作のデータに対する正当性をさらに保証するため、Neug がサポートするデータ型の境界と、これらのデータ型に基づく式操作も定義しています。これらについては、[データ型](data_types.md)セクションと[式セクション](expression.md)で詳しく紹介します。

### データ管理 (DML)

Neug は DQL と DDL に加えて、データ更新機能もサポートしています。これを DML (Data Manipulation Language) と呼びます。DML 操作は、バルクロードまたはインクリメンタル更新を通じて実行できます。

**バルクインポートの例:**
```cypher
COPY person FROM `person.csv`;
COPY knows FROM `knows.csv`;
```

上記の 2 つの Statement は、まず person.csv から label `person` の node データをバルクロードし、次に knows.csv から label `person-[knows]->person` の edge データをバルクロードします。

**インクリメンタル更新の例:**

グラフデータをインクリメンタルに更新するためのインクリメンタル書き込み構文も提供しています。

**Node 作成の例:**
```cypher
CREATE (p:person {name: 'Bob', age: 30});
```

**Relationship 作成の例:**
```cypher
MATCH (a:person {name: 'Bob'}), (b:person {name: 'marko'})
CREATE (a)-[:knows {weight: 3.0}]->(b);
```

**Node 削除の例:**
```cypher
MATCH (p:person {name: 'Bob'})
DELETE p;
```

これらの DML 操作については、[DML section](dml_clause.md) で詳しく紹介します。