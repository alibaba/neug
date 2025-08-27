# Query Clauses
この章では、Neug のクエリに関連する Clause 操作について主に紹介します。以下の表に、これらの操作のタイプと一般的な用途をまとめます：

Clause | 説明
-------|------------
[MATCH](query_clauses/match_clause.md) | グラフパターンの検索
[WHERE](query_clauses/where_clause.md) | プロパティに基づくフィルタリング
[WITH](query_clauses/with_clause.md) | プロパティに基づくプロジェクションまたは集約
[RETURN](query_clauses/return_clause.md) | プロジェクションまたは集約結果の出力
[ORDER](query_clauses/order_clause.md) | 中間結果または出力結果の追加ソート
[SKIP](query_clauses/limit_clause.md) | 結果の上位部分をスキップし、出力結果の下限を決定
[LIMIT](query_clauses/limit_clause.md) | 結果の切り詰め、出力結果の上限を決定
[UNION](query_clauses/union_clause.md) | 一貫したスキーマを持つ複数のブランチ結果をマージ
<!-- [UNWIND](query_clauses/unwind_clause.md) | リスト結果のアンネスト -->

[modern graph](https://tinkerpop.apache.org/docs/current/reference/#graph-computing) を例に、各 Clause が具体的に何を行うかを紹介します。
<!-- 以下は modern graph に対応するスキーマとデータ図です。 -->