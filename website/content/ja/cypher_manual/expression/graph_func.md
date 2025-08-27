# Graph Pattern Function

これまでに紹介した各種リレーショナルデータベースベースの関数操作に加えて、Neugはグラフパターンによって生成されたノード、エッジ、パスデータを操作するための特別な関数セットもサポートしています。

## Node Function

関数 | 説明 | 例
---------|-------------|--------
ID() | ノード/エッジの内部IDを取得 | Return (a) Return ID(a)
LABEL() | ノード/エッジのラベルを取得 | Match (a) Return LABEL(a)

## Rel Function

ID関数とLABEL関数に加えて、以下のようなエッジベースの関数操作もあります：

関数 | 説明 | 例
---------|-------------|--------
START_NODE() | エッジデータの開始ノードを返す | Match ()-[b]->() Return START_NODE(b);
END_NODE() | エッジデータの終了ノードを返す | Match ()-[b]->() Return END_NODE(b);

## 繰り返しパス関数

関数 | 説明 | 例
-----|------|----
NODES | 再帰関係からすべてのノードを返す | Match (a)-[b*2..3]->() Return NODES(b);
RELS | 再帰関係からすべての関係を返す | Match (a)-[b*2..3]->() Return RELS(b);
PROPERTIES | ノード/関係から指定されたプロパティを返す | Match (a)-[b*2..3]->() Return PROPERTIES(nodes(b), 'name'), PROPERTIES(rels(b), 'weight');
IS_TRAIL | パスに繰り返される関係が含まれているかチェックする | Match (a)-[b*2..3]->() Return IS_TRAIL(b);
IS_ACYCLIC | パスに繰り返されるノードが含まれているかチェックする | Match (a)-[b*2..3]->() Return IS_ACYCLIC(b);
LENGTH | 再帰関係における関係の数（パス長）を返す | Match (a)-[b*2..3]->() Return LENGTH(b);