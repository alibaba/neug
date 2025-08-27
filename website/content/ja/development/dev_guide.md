# 開発ガイド

### ソースからのビルド

NeuG をソースからコンパイルするには、特定の依存関係とツールをインストールする必要があります。

#### 開発コンテナでのビルド

x86_64 および arm64 プラットフォーム用の Docker イメージを提供しており、NeuG をソースからビルドするために必要なすべての依存関係が含まれています。

```bash
docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev
docker run --it --name neug-dev registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev bash

# コンテナ内で環境変数を設定します。
source ~/.neug_env
```

#### ローカルでのビルド

以下のスクリプトを使用して、ローカル環境に必要なすべての依存関係をセットアップすることもできます：

```bash
bash scripts/install_deps.sh --brpc --install-prefix /opt/neug
```

インストールが完了したら、環境変数を設定します：

```bash
source ~/.neug_env
```

### NeuG のビルド

環境が整ったら、NeuG のビルドに進むことができます。

#### 開発用

NeuG Python パッケージを development mode でビルドするには、以下を実行してください：

```bash
make python-dev
```

これにより、NeuG エンジンと Python client が development mode でコンパイルされます。`tools/python_bind` ディレクトリから Python スクリプトを実行することで、NeuG パッケージを使用できます。

```bash
cd tools/python_bind
python3
>>> import neug
```

他のディレクトリから NeuG を使用するには、`sys.path` に `tools/python_bind` を追加してください：

```python
import sys
sys.path.append('/path/to/neug/tools/python_bind')
```

#### Wheel パッケージのビルド

wheel パッケージをコンパイルするには、以下を実行してください：

```bash
make python-wheel
```

実行後、wheel パッケージは `tools/python_bind/dist` に生成されます。

#### ビルドオプション

以下の環境変数を指定することで、ビルドプロセスをカスタマイズできます：

```bash
export BUILD_EXECUTABLES=ON/OFF # ユーティリティ実行ファイルのビルドを切り替える
export BUILD_HTTP_SERVER=ON/OFF # NeuGでのHTTPサーバーサポートを有効化または無効化する
export BUILD_ALL_IN_ONE=ON/OFF # NeuGライブラリを単一の動的ライブラリとしてビルドするか、複数の別々のライブラリとしてビルドするかを制御する
export WITH_MIMALLOC=ON/OFF # glibcのデフォルトmallocの代わりにmimallocを使用するかどうかを決定する
export ENABLE_BACKTRACES=ON/OFF # 例外発生時に詳細なスタックトレースを出力するために、NeuGライブラリをcpptraceとリンクする
export BUILD_TYPE=DEBUG/RELEASE # CMakeのビルドタイプを設定する
export BUILD_TEST=ON/OFF # テストスイートのビルドを切り替える
```

#### デバッグ

デフォルトでは、C++のロギングは無効になっています。ロギングを有効にするには、以下を使用します：

```bash
export DEBUG=ON
```

より詳細なロギングが必要な場合は、以下でglogのverbosityレベルを調整してください：

```bash
export GLOG_v=10 # グローバルに設定
GLOG_v=10 python3 ... # 単一コマンドに対して設定
```

セグメンテーションフォルトやその他の複雑な問題をさらに調査するには、gdb/lldbの使用を推奨します：

```bash
GLOG_v=10 gdb --args python3 -m pytest -sv tests/test_db_query.py
GLOG_v=10 lldb -- python3 -m pytest -sv tests/test_db_query.py
```

追加のデバッグ手法については、それぞれgdbおよびlldbのドキュメントを参照してください。

### FAQ

#### macOS で e2e クエリの実行に失敗する

```
➜  e2e git:(main) ✗ ./scripts/run_embed_test.sh modern_graph /tmp/modern_graph basic_test
Running command: python3 -m pytest -sv -m neug_test --query_dir=/Users/zhanglei/code/nexg/tests/e2e/scripts/../queries/basic_test --dataset modern_graph --db_dir=/tmp/modern_graph --read_only --html="/Users/zhanglei/code/nexg/tests/e2e/scripts/../report/modern_graph/basic_test/test_report.html" --alluredir="/Users/zhanglei/code/nexg/tests/e2e/scripts/../report/modern_graph/basic_test/allure-results"
INFO:neug:Found build directories: ['lib.macosx-15.0-arm64-cpython-313']
INFO:neug:Selected build directory: lib.macosx-15.0-arm64-cpython-313
INFO:neug:Selected build directory: lib.macosx-15.0-arm64-cpython-313
INFO:neug:Build directory: /Users/zhanglei/code/nexg/tests/e2e/../../tools/python_bind/neug/../build/lib.macosx-15.0-arm64-cpython-313
INFO:neug:Adding build directory to sys.path: /Users/zhanglei/code/nexg/tests/e2e/../../tools/python_bind/neug/../build/lib.macosx-15.0-arm64-cpython-313
ImportError while loading conftest '/Users/zhanglei/code/nexg/tests/e2e/conftest.py'.
conftest.py:31: in <module>
    from neug import Session
../../tools/python_bind/neug/__init__.py:149: in <module>
    raise ImportError(
E   ImportError: NeuG is not installed. Please install it using pip or build it from source: dlopen(/Users/zhanglei/code/nexg/tests/e2e/../../tools/python_bind/neug/../build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so, 0x0002): Library not loaded: @rpath/libboost_filesystem.dylib
E     Referenced from: <F7ACB223-7AD2-4427-B5F6-5BA4505CFA4F> /Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so
E     Reason: tried: '/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/opt/homebrew/lib/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/opt/homebrew/lib/libboost_filesystem.dylib' (no such file), '/opt/homebrew/lib/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/opt/homebrew/lib/libboost_filesystem.dylib' (no such file)
```

この問題を解決するには、rpath を手動でインストールしてください。

```bash
cd tools/python_bind/
install_name_tool -add_rpath /opt/neug/lib ./build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so
```