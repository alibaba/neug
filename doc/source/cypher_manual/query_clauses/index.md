# Query Clauses
In this chapter, we mainly introduce NeuG query-related Clauses operations. The following table summarizes the types and general purposes of these operations:

Clause | Description
-------|------------
[MATCH](match_clause) | Find Graph Pattern
[WHERE](where_clause) | Filter based on properties
[WITH](with_clause) | Projection or Aggregation based on properties
[RETURN](return_clause) | Output Projection or Aggregation results
[ORDER](order_clause) | Further sort intermediate or output results
[SKIP](limit_clause) | Skip the top portion of results, determine the lower bound of output results
[LIMIT](limit_clause) | Truncate results, determine the upper bound of output results
[UNION](union_clause) | Merge multiple branch results with consistent schema
<!-- [UNWIND](unwind_clause) | Unnest a List Result -->

We will use the [modern graph](https://tinkerpop.apache.org/docs/current/reference/#graph-computing) as an example to introduce what each Clause specifically does. 
<!-- Below is the schema and data diagram corresponding to the modern graph.  -->