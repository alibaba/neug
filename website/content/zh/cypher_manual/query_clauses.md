# 查询子句 (Query Clauses)

在本章中，我们主要介绍 Neug 中与查询相关的 Clauses 操作。下表总结了这些操作的类型及其主要用途：

Clause | Description
-------|------------
[MATCH](query_clauses/match_clause.md) | 查找图模式 (Find Graph Pattern)
[WHERE](query_clauses/where_clause.md) | 基于属性进行过滤 (Filter based on properties)
[WITH](query_clauses/with_clause.md) | 基于属性进行投影或聚合 (Projection or Aggregation based on properties)
[RETURN](query_clauses/return_clause.md) | 输出投影或聚合结果 (Output Projection or Aggregation results)
[ORDER](query_clauses/order_clause.md) | 对中间结果或输出结果进行排序 (Further sort intermediate or output results)
[SKIP](query_clauses/limit_clause.md) | 跳过结果的前部分，确定输出结果的下界 (Skip the top portion of results, determine the lower bound of output results)
[LIMIT](query_clauses/limit_clause.md) | 截断结果，确定输出结果的上界 (Truncate results, determine the upper bound of output results)
[UNION](query_clauses/union_clause.md) | 合并多个具有相同 schema 的分支结果 (Merge multiple branch results with consistent schema)
<!-- [UNWIND](query_clauses/unwind_clause.md) | 展开列表结果 (Unnest a List Result) -->

我们将使用 [modern graph](https://tinkerpop.apache.org/docs/current/reference/#graph-computing) 作为示例，介绍每个 Clause 的具体作用。
<!-- 下图是 modern graph 对应的 schema 和数据图。 -->