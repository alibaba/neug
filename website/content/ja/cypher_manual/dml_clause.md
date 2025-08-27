# DML句

DML（Data Manipulation Language）は、グラフデータベースにおけるデータの挿入、削除、更新操作を提供します。Neugは、バルクデータ操作（COPY FROMなど）と個別データ操作（CREATE、SET、DELETEなど）の両方をサポートしています。本ドキュメントでは、各操作タイプの例と説明を提供します。

## COPY FROM

COPY FROMコマンドを使用すると、外部データソースからデータを一括読み込み、グラフストレージ内に頂点と辺を構築できます。現在サポートされているデータソースには、CSV形式が含まれます。

### 頂点データの読み込み

CSVファイルからperson頂点データを読み込みます。CSVの各行は頂点にマッピングされ、列はpersonスキーマで定義された頂点プロパティに対応します。

**person.csv:**
```
name,age
marko,39
vadas,27
josh,32
peter,35
```

**コマンド:**
```cypher
COPY person FROM "person.csv"
```

### エッジデータの読み込み

Load は CSV ファイルからエッジデータを読み込みます。最初の2列はソースとターゲットの頂点の主キーを指定し、追加の列はエッジのプロパティを定義します。

**knows.csv:**
```
src_name,dst_name,weight
marko,josh,1.0
marko,vadas,0.5
josh,peter,0.8
```

**Command:**
```cypher
COPY knows FROM "knows.csv"
```

## CREATE

CREATE 句は、グラフに新しい頂点と関係を挿入するために使用されます。

### 頂点の作成

指定されたプロパティを持つ新しい頂点を作成します。同じ主キーを持つ頂点が既に存在する場合、エラーが報告されます。

```cypher
CREATE (a:person {name: 'taylor', age: 25}), (b:person {name: 'julie', age: 30})
```

### 頂点と関係の作成

単一のステートメントで頂点と関係を作成します。これは、頂点とそれらの間の関係の両方を作成する必要がある場合に便利です。

```cypher
CREATE (a:person {name: 'mars', age: 28})-[:knows {weight: 16.0}]->(b:person {name: 'jennie', age: 26})
```

### 既存の頂点間の関係を作成する

まず既存の頂点をマッチさせ、それらの間に関係を作成します。

```cypher
MATCH (a:person {name: 'taylor'}), (b:person {name: 'julie'})
CREATE (a)-[:knows {weight: 20.0}]->(b)
```

## SET

SET句は、既存の頂点や関係のプロパティを更新するために使用されます。

### 頂点のプロパティを更新する

特定の頂点のプロパティを更新します。

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
SET a.age = 37, a.city = 'New York'
RETURN a.*
```

### 関係のプロパティを更新する

特定の関係のプロパティを更新します。

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
SET k.weight = 10.0, k.since = '2023-01-01'
RETURN k.*
```

## DELETE

DELETE句は、グラフから頂点や関係を削除するために使用されます。

### 頂点の削除

グラフから頂点を削除します。デフォルトでは、孤立したエッジを作成しないように、関係を持たない頂点のみを削除できます。

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DELETE a
```

### 関係を持つ頂点の削除 (DETACH DELETE)

DETACH DELETE を使用して、頂点とそのすべての関係を強制的に削除します。これにより、既存の関係を持つ頂点を削除しようとしたときにエラーが発生するのを防ぎます。

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DETACH DELETE a
```

### 関係の削除

頂点をそのまま保持しながら、頂点間の特定の関係を削除します。

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
DELETE k
```