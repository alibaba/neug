# NeuG のインストール

NeuG は、高性能なグラフ分析とリアルタイムトランザクション処理向けに設計された HTAP グラフデータベースです。複数のプラットフォームとプログラミング言語をサポートしています。

## システム要件

### サポートするオペレーティングシステム
- **Linux**: Ubuntu 13.10+, Debian 8+, Fedora 19+, RHEL 7+
- **macOS**: macOS 11.0+ (Big Sur 以降)

### サポートするアーキテクチャ
- **x86_64** (Intel/AMD 64-bit)
- **ARM64** (Apple Silicon, ARM ベースのサーバー)

### ハードウェア要件
- **CPU**: 最適なパフォーマンスのため、マルチコアプロセッサを推奨

## Python でのインストール

NeuG の Python パッケージは PyPI で利用可能で、Python 3.8+ をサポートしています。

```{note}
**要件**: Python >= 3.8
```

### 仮想環境の作成

```bash
python3 -m venv neug-env
source neug-env/bin/activate
```

### PyPI からのインストール

```bash

# 最新バージョンをインストール
pip install neug

# 特定のインデックスを指定してインストール（必要に応じて）
pip install neug -i https://pypi.org/simple
```

```markdown
# Aliyun ミラーからインストール（中国ユーザー向け）
pip install neug -i https://mirrors.aliyun.com/pypi/simple/
```

### インストールの確認

```python
import neug

# バージョン確認
print(neug.__version__)

# シンプルなインメモリデータベースを作成
db = neug.Database(db_path="")
conn = db.connect()
print("NeuG installation successful!")
```

## C++ インストール

### 手動インストール

現在のところ、ソースコードからのビルドのみをサポートしています。ビルド環境の準備については [Development](../development/dev_guide.md) を参照してください。
環境が準備できたら、cmake を使用して NeuG をビルド・インストールします。

```bash
git clone https://github.com/GraphScope/neug.git
cd neug
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/opt/neug # その他の cmake オプションについては CMakeLists.txt を参照
make -j$(proc)
make install
```

### インストールの確認

あなたの cmake プロジェクトで、以下のコマンドを使って Neug ライブラリを検索・リンクしてください：

```cmake
cmake_minimum_required (VERSION 3.10)
project (
  NeugTest
  VERSION 0.1
  LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)


find_package(neug REQUIRED)
add_executable(test test.cc)
include_directories(${NEUG_INCLUDE_DIRS})
target_link_libraries(test ${NEUG_LIBRARIES})
```

サンプルの test.cc は以下のようになります：

```cpp
#include <neug/main/neug_db.h>
#include <iostream>

int main() {
  gs::NeugDB db("test_db");
  auto conn = db.connect();
  std::cout << "NeuG C++ client installation successful!" << std::endl;
  return 0;
}
```

テストをビルド・実行します：

```bash
mkdir build && cd build
cmake .. -DCMAKE_PREFIX_PATH=/opt/neug
./test
```

## Command Line Interface (CLI)

NeuG CLI ツールは、データベース操作のためのインタラクティブシェルを提供します。

### インストール

```bash

# pip 経由でインストール（CLI ツールを含む）
pip install neug

```markdown
# または CLI のみをインストール
pip install neug-cli
```

### 使用方法

```bash

# インメモリデータベースで対話型シェルを起動
neug-cli

# 組み込みローカルデータベースに接続

# 組み込みモードで実行されます
neug-cli /path/to/database

# リモートデータベースサービスに接続

# まずサービスを起動する必要があります
neug-cli neug://localhost:8080

# クエリを直接実行
neug-cli -c "MATCH (n) RETURN count(n)" /path/to/database
```

### インストールの確認

```bash
neug-cli --version
```

## プラットフォーム固有の注意事項

### Linux
- 必要なシステムライブラリがインストールされていることを確認してください
- 古いディストリビューションでは、新しいバージョンの glibc をインストールする必要があるかもしれません

### macOS
- Intel と Apple Silicon の両方をサポート
- 開発には Homebrew でのインストールを推奨

## トラブルシューティング

### よくある問題

#### Python インストールに関する問題
```bash

# 権限エラーが発生した場合
pip install --user neug

```

#### 依存関係の不足
```bash
```

```bash
# Linux: build essentials をインストール
sudo apt-get install build-essential

# macOS: Xcode command line tools をインストール
xcode-select --install
```

#### Version Conflicts
```bash

# インストール済みのバージョンを確認
pip show neug

# 最新バージョンにアップグレード
pip install --upgrade neug
```

### Getting Help

インストール中に問題が発生した場合:
1. [troubleshooting guide](../troubleshooting/index.md) を確認
2. [GitHub issues](https://github.com/graphscope/neug/issues) を参照

## Next Steps

NeuG のインストールが完了したら、以下のことができます:
1. [Getting Started guide](../getting-started/index.md) に従う
2. [connection modes](../connection-modes/index.md) を確認
3. [tutorials](../tutorials/index.md) を試す