# NeuG を始める

このガイドでは、最初のグラフデータベースの作成、基本操作の実行、そして embedded モードと service モードの両方について説明します。

## 前提条件

開始する前に、NeuG がインストールされていることを確認してください。まだインストールしていない場合は、[インストールガイド](../installation/installation.md)に従ってください。

## NeuG アーキテクチャの理解

NeuG には、独立して動作する2つの異なるアーキテクチャ概念があります：

### データベースストレージモード
データの保存方法：
- **In-Memory Database**: データを RAM のみに保存（最速、一時的）
- **Persistent Database**: データをディスクに保存（永続化、再起動後も維持）

### 接続モード
データベースへのアクセス方法：
- **Embedded Mode**: プロセス内直接アクセス（分析、シングルユーザー）
- **Service Mode**: ネットワークベースのアクセス（マルチユーザー、同時接続）

これらのモードは独立しています。ユースケースに応じて任意の組み合わせが可能です：

**一般的な組み合わせ：**
- **Persistent + Embedded**: 大規模データセットでのデータサイエンス分析、ETL処理、グラフ研究
- **Persistent + Service**: 本番Webアプリケーション、マルチユーザーシステム、マイクロサービスアーキテクチャ
- **In-Memory + Embedded**: 単体テスト、プロトタイピング、一時的な計算、アルゴリズム開発
- **In-Memory + Service**: 高速キャッシングレイヤー、セッションベースの分析、一時的なマルチユーザーワークロード

## データベースストレージモード

### Persistent Database
- **ユースケース**: 本番アプリケーション、データ分析、長期ストレージ
- **耐久性**: アプリケーション再起動後もデータが保持される

```python

# Persistent mode examples

```markdown
# データベースディレクトリが存在し、ユーザーによる書き込みが可能であることを確認してください。
db_persistent = neug.Database(db_path="/path/to/database")
```

### In-Memory Database
- **Use case**: 一時的な計算、テスト、プロトタイピング
- **Durability**: プロセス終了時にデータが失われる

```python

# Memory mode の例
db_memory = neug.Database(db_path="")
```

```{note}
現在、NeuG の in-memory mode では、プロセス終了時に自動的にクリーンアップされる一時的なデータベースディレクトリが作成されます。
```

## Connection Modes

NeuG では、データベースにアクセスするための2つのモードを提供しています：

### Embedded Mode
プロセス内から直接アクセス - シングルユーザー向けに最も簡単：

```python
import neug

# データベースを作成し、直接接続
db = neug.Database(db_path="./neug/db")  # または db_path="" で in-memory
conn = db.connect()

print("Connect to NeuG in embedded mode")

conn.close()
db.close()
```

### Service Mode
ネットワークベースのアクセス - マルチユーザー向けアプリケーションに最適：

**サービスを起動：**
```python
import neug
```

# NeuGをサービスとして起動
db = neug.Database("./neug/db")
service = db.serve(host="localhost", port=10000)
print("NeuG service started on localhost:10000")
```

**クライアントから接続:**
```python
from neug import Session

# サービスに接続
session = Session("http://localhost:10000/")

session.close()
```

## 基本操作

以下の操作は、データベースモード（インメモリまたは永続化）や接続モード（埋め込みまたはサービス）を選択しても同じように動作します。この例では、埋め込みモードの永続化データベースを使用していると仮定します。

```python
import neug

# データベースを作成し、接続を確立
db = neug.Database(db_path="./neug/db")
conn = db.connect()
```

### ノードとエッジの作成

データを挿入する前に、ノード型とエッジ型でグラフスキーマを定義する必要があります：

```python

# ノードテーブルの作成
conn.execute("""
    CREATE NODE TABLE Person(
        id INT64 PRIMARY KEY,
        name STRING,
        age INT64,
        email STRING
    )
""")

conn.execute("""
    CREATE NODE TABLE Company(
        id INT64 PRIMARY KEY,
        name STRING,
        industry STRING,
        founded_year INT64
    )
""")

# エッジテーブルの作成
conn.execute("""
    CREATE REL TABLE WORKS_FOR(
        FROM Person TO Company,
        position STRING,
        start_date DATE,
        salary DOUBLE
    )
""")

conn.execute("""
    CREATE REL TABLE KNOWS(
        FROM Person TO Person,
        since_year INT64,
        relationship_type STRING
    )
""")

print("Graph schema created successfully!")
```

### データの挿入

では次に、グラフにデータを追加しましょう：

```python

# ノードの挿入
conn.execute("""
    CREATE (p:Person {id: 1, name: 'Alice Johnson', age: 30, email: 'alice@example.com'})
""")

conn.execute("""
    CREATE (p:Person {id: 2, name: 'Bob Smith', age: 35, email: 'bob@example.com'})
""")

conn.execute("""
    CREATE (c:Company {id: 1, name: 'TechCorp', industry: 'Technology', founded_year: 2010})
""")

# リレーションシップの挿入
conn.execute("""
    MATCH (p:Person), (c:Company) WHERE p.id = 1 AND c.id = 1
    CREATE (p)-[:WORKS_FOR {position: 'Software Engineer', start_date: date('2020-01-15'), salary: 75000.0}]->(c)
""")

conn.execute("""
    MATCH (p1:Person {id: 2}), (p2:Person {id: 1})
    CREATE (p1)-[:KNOWS {since_year: 2018, relationship_type: 'colleague'}]->(p2)
""")

print("Data inserted successfully!")
```

### データのクエリ

いくつかのクエリでグラフを探索してみましょう：

```python

```python
# シンプルなノードクエリ
result = conn.execute("MATCH (p:Person) RETURN p.name, p.age")
for record in result:
    print(record)
    # ['Alice Johnson', 30]
    # ['Bob Smith', 35]

# リレーションシップクエリ
result = conn.execute("""
    MATCH (p:Person)-[w:WORKS_FOR]->(c:Company)
    RETURN p.name, w.position, c.name
""")
for record in result:
    print(f"{record[0]} works as {record[1]} at {record[2]}")
    # Alice Johnson works as Software Engineer at TechCorp

# 複雑なパターンクエリ
result = conn.execute("""
    MATCH (p1:Person)-[:KNOWS]->(p2:Person)-[:WORKS_FOR]->(c:Company)
    RETURN p1.name as person1, p2.name as person2, c.name as company
""")
for record in result:
    print(f"{record[0]} knows {record[1]} who works at {record[2]}")
    # Bob Smith knows Alice Johnson who works at TechCorp
```

### コネクションとデータベースをクローズ

```python
conn.close()
db.close()
```

### ビルトインデータセットの使用

NeuG では、グラフ分析、学習、テストをすぐに開始できるよう、複数のビルトインデータセットを提供しています。これらのデータセットはすぐに使用可能で、セットアップは不要です。

#### 利用可能なデータセット

利用可能なすべてのビルトインデータセットを一覧表示できます：

```python
from neug.datasets import get_available_datasets

# 利用可能なすべてのデータセットを一覧表示
datasets = get_available_datasets()
for dataset in datasets:
    print(f"{dataset.name}: {dataset.description}")
```

現在利用可能なビルトインデータセットは以下の通りです：
- modern_graph

#### ビルトインデータセットの読み込み

ビルトインデータセットを使用する方法はいくつかあります：

**方法1: データセットから新しいデータベースを作成する**

```python
from neug import Database

# ビルトインデータセットから直接データベースを作成
db = Database.from_builtin_dataset(dataset_name="modern_graph")
conn = db.connect()
```

# ロードしたデータセットを探索する
result = conn.execute("MATCH (n) RETURN count(n) as node_count")
print(f"Total nodes: {result.__next__()[0]}") # 6 になるはず
```

**方法 2: 既存のデータベースにデータセットをロードする**

```python
import neug

# 空のデータベースを作成
db = neug.Database("./my_analysis.db")

# ビルトインデータセットをロード
db.load_builtin_dataset(dataset_name="modern_graph")
```

既存のデータベースにビルトインデータセットをインポートする際は、スキーマの衝突がないことを確認してください。つまり、ラベル名が `person` および `software` の頂点、ラベル名が `knows` および `created` のエッジが存在しないことを確認してください。

**方法 3: 便利な関数を使用する**

```python
from neug.datasets.loader import load_dataset

# または特定のパスにロード
db = load_dataset("modern_graph", "/tmp/modern_graph.db")

conn = db.connect()

# ... データセットを操作
conn.close()
db.close()
```

## 次のステップ

おめでとうございます！NeuGの基本を学習しました。次に取り組める内容は以下の通りです：

1. **[データのインポート/エクスポート](import_graph.md)**: 大規模データセットのインポート方法を学ぶ
2. **[高度なCypherクエリ](cypher_query.md)**: 複雑なグラフパターンをマスターする
3. **[データモデリング](data_model.md)**: 効率的なグラフスキーマを設計する
4. **[パフォーマンス最適化](../performance/index.md)**: データベースを最大限にチューニングする