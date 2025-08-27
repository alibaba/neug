### キー コンセプト
NeuG における Database、Connection、Session は基本的な概念です。

## Database

NeuG における **Database** は、頂点と辺から構成されるグラフを表します。デフォルトでは、データは指定されたディレクトリにディスク上に永続化されます。NeuG は **in-memory** モードも提供しており、このモードではデータはメモリ内にのみ存在し、ディスクには保存されません。インメモリデータベースを作成するには、`db_path` を空文字列に設定します：

```python
from neug import Database

# インメモリデータベースを作成
db = Database(db_path="")
```

### アクセスモード

NeuGデータベースは、`read_write`と`read_only`の2つのモードでアクセスできます。

- **read_write**: ディレクトリを**read_write**モードで開くと、そのプロセスに排他的アクセス権が付与され、ディレクトリがロックされて他のプロセスが同時にアクセスできなくなります。
- **read_only**: **read_only**モードで開いた場合、複数のプロセスが同時に**read_only**モードでディレクトリにアクセスできます。

インメモリデータベースは`read_only`モードで開くことができないことに注意してください。

## コネクション

**Connection**は、組み込みデータベースとやり取りするための主要なインターフェースとして機能し、クエリの実行とデータ管理を可能にします。

### Databaseからの接続確立

**Connection** オブジェクトはデータベースにアクセスするための経路として機能し、その動作はデータベースのアクセスモードに影響されます。

- **read_write** モードでは、Connectionは読み取りクエリ（MATCH）と書き込みクエリ（CREATE、COPY FROM）の両方を実行できます。データ整合性を維持するために、read_writeデータベースに対して作成できる接続オブジェクトは1つだけです。
  
- **read_only** モードでは、Connectionは読み取りクエリの実行に限定されます。

## サービスモードとセッション

NeuGは**サービスモード**でも動作可能で、データベースがサーバーとして動作し、リモートからの接続を受け付けます。このモードは頻繁な同時アクセスや分散アクセスを必要とするアプリケーションに最適です。

NeuGをサービスモードで起動するには、ホストとポートを指定します：

```python
import neug

# NeuGをサービスとして起動する
```python
db = neug.Database("./neug/db")
service = db.serve(host="localhost", port=10000)
print(f"NeuG service started on {service}")
```

サーバーが起動したら、クライアントは`Session`オブジェクトを使って接続できます：

```python
from neug import Session

# NeuGサービスへの接続を確立
session = Session("http://localhost:10000/")

# クエリを実行
session.execute('MATCH(n) RETURN count(n)')

session.close()
```

複数のプロセスで複数のセッションをインスタンス化して、並行してクエリを送信することができます。データベースサーバーは、データベースのACID特性が全体を通して維持されるようにします。