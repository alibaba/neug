# Neug データ型

このドキュメントでは、Neug でサポートされているすべてのデータ型について詳しく説明します。

## データ型一覧表

以下の表は、Neug がサポートするすべてのデータ型と、それらの Neo4j との違いを示しています。

| カテゴリ | 型 | Neug の例 | Neo4j の例 |
|----------|------|--------------|---------------|
| Primitive | INT32 | `Return CAST(42, 'INT32')` | `Return 42` |
| Primitive | UINT32 | `Return CAST(42, 'UINT32')` | unsupported |
| Primitive | INT64 | `Return 9223372036854775807` | `Return 9223372036854775807` |
| Primitive | UINT64 | `Return CAST(9223372036854775807, 'UINT64')` | unsupported |
| Primitive | FLOAT | `Return CAST(3.14, 'FLOAT')` | `Return 3.14f` |
| Primitive | DOUBLE | `Return 3.14159265359` | `Return 3.14159265359d` |
| Primitive | BOOL | `Return true` | `Return true` |
| Primitive | NULL | `Return null` | `Return null` |
| String | VARCHAR | `Return 'Hello World'` | `Return 'Hello World'` |
| Temporal | DATE | `Return date('2022-06-06')` | `Return date('2022-06-06')` |
| Temporal | DATETIME | `Return timestamp('2022-06-06 12:00:00')` | `Return datetime('2022-06-06T12:00:00')` |
| Temporal | INTERVAL | `Return interval('1 year 2 days')` | `Return duration('P1Y2D')` |
| Composite | LIST | `Return [1, 2, 3]` | `Return [1, 2, 3]` |
| Pattern | NODE | `{_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}` | `(:person {name: 'Alice', age: 30})` |
| Pattern | REL | `{_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}` | `[:knows {weight: 1.0}]` |
| Pattern | REPEATED PATH | `{_ID: 0, _LABEL: person}, {_ID: 4294967298, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: person}, {_ID: 4297064449, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: software}` | `(:Person {name: "Kiefer", id: 4, age: 1992})-[:FOLLOWS]->(:Person {name: "Jack", id: 3, age: 1979})-[:FOLLOWS]->(:Person {name: "Kevin", id: 5, age: 1997})` |

## 詳細な紹介

### プリミティブ型

#### INT32
- **説明**: 32ビット符号付き整数型
- **範囲**: [2,147,483,648, 2,147,483,647]
- **クエリ例**: `Return CAST(42, 'INT32') as int32_value;`

#### UINT32
- **説明**: 32ビット符号なし整数型
- **範囲**: [0, 4,294,967,295]
- **クエリ例**: `RETURN CAST(42, 'UINT32') AS uint32_value;`

#### INT64
- **説明**: 64ビット符号付き整数型、整数値のデフォルト型
- **範囲**: [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]
- **クエリ例**: `RETURN 9223372036854775807 AS int64_value;`

#### UINT64
- **説明**: 64ビット符号なし整数型
- **範囲**: [0, 18,446,744,073,709,551,615]
- **クエリ例**: `RETURN CAST(18446744073709551615, 'UINT64') AS uint64_value;`

#### FLOAT
- **説明**: 単精度浮動小数点数
- **精度**: ~7桁の10進数
- **クエリ例**: `RETURN CAST(3.14, 'FLOAT') AS float_value;`

#### DOUBLE
- **Description**: 倍精度浮動小数点数、float値のデフォルト型
- **Precision**: ~15-17桁の10進数精度
- **Query Example**: `RETURN 3.14159265359 AS double_value;`

#### BOOL
- **Description**: trueまたはfalseの値を表すBoolean型
- **Values**: `true`, `false`
- **Query Example**: `RETURN true AS bool_value;`

#### NULL
- **Description**: 欠損値または未定義の値を表す
- **Query Example**: `RETURN null AS null_value;`

### String Types

#### VARCHAR
- **Description**: UTF-8エンコーディングの可変長文字列
- **Query Example**: `RETURN 'Hello World' AS string_value;`
- **Length**: 可変長、システム制約による制限あり、デフォルトは`65536`

### Temporal Types

#### DATE
- **Description**: カレンダー日付を格納するための日付型
- **Format**: YYYY-MM-DD
- **Query Example**: `RETURN date('2022-06-06') AS date_value;`

#### DATETIME
- **Description**: 日付と時刻の組み合わせ型
- **Format**: YYYY-MM-DD HH:MM:SS
- **Query Example**: `RETURN timestamp('2022-06-06 12:00:00') AS datetime_value;`

#### INTERVAL
- **Description**: 期間または時間間隔型で、[kuzuの仕様]()に従います。
- **Query Example**: `RETURN interval('1 year 2 days') AS interval_value;`

### Composite Types

#### LIST
- **説明**: 異なる型を含む値の順序付きコレクション
- **クエリ例**: `RETURN [1, 2, 3] AS list_value;`

以下の表は、LIST がサポートするすべてのコンポーネント型を示しています：

| カテゴリ | 型 | 例 |
|----------|------|---------|
| 数値 | INT32, INT64, UINT32, UINT64, DOUBLE, FLOAT | `RETURN [1, 2, 3.0];` |
| 文字列 | VARCHAR | `RETURN ['marko', 'josh'];` |
| 日付 | DATE, DATETIME | `RETURN [date('2011-01-25'), timestamp('2011-01-25 11:20:33')];` |
| ブール値 | BOOL | `RETURN [true, false];` |
| 複合型 | LIST | `RETURN [[1, 2], [4, 5]];` |

**LIST コンポーネント型に関する重要な注意点**：

Neug は tuple データ型を通じてリストをサポートしており、複合型は異種混合（heterogeneous）可能です。以下にいくつかの例を示します：

単一リスト内で異なるプリミティブ型を混在させる：
```cypher
RETURN ['marko', 2];
```

ノードから取得した異なるプロパティ型をリストにまとめる：
```cypher
MATCH (n:person) RETURN [n.name, n.age];
```

ネストされたリスト構造のサポート：
```cypher
MATCH (n:person) RETURN [["name", n.name], ["age", n.age]];
```

**主な技術的詳細**：
- Neug のリストは異なるデータ型の要素を含むことができる（heterogeneous lists）
- これは内部的な tuple データ型のサポートにより実現されている
- 型変換は可能な場合自動で行われる
- 複雑なデータ構造に対応するため、ネストされたリストも完全にサポートされている
- システムはリスト構成の柔軟性を保ちながらも型安全性を維持している

### パターンタイプ

#### NODE
- **説明**: グラフ内の頂点を表す
- **内部構造**: `_ID` (内部識別子)、`_LABEL` (ノードラベル)、およびプロパティフィールドを含む
- **クエリ例**: `MATCH (n:person) RETURN n AS node_value;`
- **Neugフォーマット**: `{_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}`

#### REL (Relationship)
- **説明**: グラフ内のエッジを表す
- **内部構造**: `_SRC` (ソースノードのIDとLABEL)、`_DST` (デスティネーションノードのIDとLABEL)、`_ID` (リレーションシップの内部識別子)、`_LABEL` (リレーションシップタイプ)、およびプロパティフィールドを含む
- **クエリ例**: `MATCH ()-[r:knows]->() RETURN r AS rel_value;`
- **Neugフォーマット**: `{_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}`

#### REPEATED PATH
- **説明**: グラフ内で繰り返されるエッジから構成されるパスを表します
- **内部構造**: 開始ノードと終了ノードを含む、パスに沿った各ノードとリレーションシップを格納します
- **クエリ例**: `MATCH (a:person)-[p*1..2]->(c) RETURN p AS path_value;`
- **Neug 形式**: `{_ID: 0, _LABEL: person}, {_ID: 4294967298, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: person}, {_ID: 4297064449, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: software}`