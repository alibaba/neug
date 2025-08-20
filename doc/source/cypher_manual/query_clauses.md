# Query Clauses
In this chapter, we mainly introduce Neug query-related Clauses operations. The following table summarizes the types and general purposes of these operations:

Clause | Description
-------|------------
[MATCH](query_clauses/match_clause.md) | Find Graph Pattern
[WHERE](query_clauses/where_clause.md) | Filter based on properties
[WITH](query_clauses/with_clause.md) | Projection or Aggregation based on properties
[RETURN](query_clauses/return_clause.md) | Output Projection or Aggregation results
[ORDER](query_clauses/order_clause.md) | Further sort intermediate or output results
[SKIP](query_clauses/limit_clause.md) | Skip the top portion of results, determine the lower bound of output results
[LIMIT](query_clauses/limit_clause.md) | Truncate results, determine the upper bound of output results
<!-- [UNION](query_clauses/union_clause.md) | Merge multiple branch results with consistent schema
[UNWIND](query_clauses/unwind_clause.md) | Unnest a List Result -->

We will use the [modern graph]() as an example to introduce what each Clause specifically does. Below is the schema and data diagram corresponding to the modern graph. 