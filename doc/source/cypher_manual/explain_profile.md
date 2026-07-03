# EXPLAIN and PROFILE

Query analysis and performance profiling tools for understanding execution plans and collecting performance metrics.

## EXPLAIN

The EXPLAIN clause allows you to inspect the execution plan of a query without actually executing it. This is useful for understanding how NeuG will process your query and identifying potential optimization opportunities.

### Syntax

```cypher
EXPLAIN [LOGICAL | PHYSICAL] <query>
```

- **PHYSICAL** (default): Display the physical execution plan that will actually be executed
- **LOGICAL**: Display the logical execution plan before physical optimization

### EXPLAIN PHYSICAL

Displays the physical execution plan showing the exact operators and their execution strategy.

#### Example

```cypher
EXPLAIN MATCH (p:Person)-[:KNOWS]->(f:Person)
WHERE p.age > 30
RETURN p.name, f.name;
```

**Output:** A JSON representation of the physical execution plan including:
- Scan operators for each table access
- Filter operations and conditions
- Join strategies and ordering
- Projection operations

### EXPLAIN LOGICAL

Displays the logical execution plan before physical optimization, useful for understanding the semantic structure of your query.

#### Example

```cypher
EXPLAIN LOGICAL MATCH (p:Person)-[:KNOWS]->(f:Person)
WHERE p.age > 30
RETURN p.name, f.name;
```

**Output:** The logical plan representation showing:
- Pattern matching structure
- Filter conditions
- Aggregations (if any)
- Output projections

## PROFILE

The PROFILE clause executes a query and collects performance metrics including execution time, operator costs, and resource usage. This is essential for query performance tuning.

### Syntax

```cypher
PROFILE <query>
```

### Characteristics

- **Execution**: The query is fully executed; results are returned along with metrics
- **Metrics**: Timing information and cost estimates for each operator
- **Performance Analysis**: Use to identify bottlenecks and optimize queries

### Example: Simple Scan

```cypher
PROFILE MATCH (p:Person) RETURN p.name;
```

Output includes execution time and operator costs.

### Example: Join Query

```cypher
PROFILE MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE a.age > 25 AND b.age < 50
RETURN COUNT(*);
```

Output includes:
- Scan time for Person table
- Join time between KNOWS edges
- Filter execution time
- Aggregation cost

### Example: Complex Multi-Hop Pattern

```cypher
PROFILE MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
WHERE a.name = 'Alice'
RETURN b.name, c.name
ORDER BY b.name
LIMIT 10;
```

Performance metrics help identify:
- Which operators consume the most time
- Whether index usage is optimal
- Whether query reordering would help

## Use Cases

### Query Optimization

1. Run **EXPLAIN PHYSICAL** to understand the execution strategy
2. Check if indexes are being used
3. Verify join order is optimal
4. Use **PROFILE** to measure actual execution time

### Performance Investigation

When a query is slow:

```cypher
-- First, understand the plan
EXPLAIN MATCH (...) ... 

-- Then, measure actual performance
PROFILE MATCH (...) ...

-- Analyze metrics and adjust query if needed
```

### Schema and Index Planning

Use EXPLAIN to verify:
- Correct table access
- Index utilization
- Efficient join strategies

Before adjusting:
- CREATE INDEX statements
- Query structure
- Property cardinality

## Limitations and Notes

- EXPLAIN does not execute the query; PROFILE does
- EXPLAIN LOGICAL shows pre-optimization semantics
- EXPLAIN PHYSICAL may take time for complex queries
- PROFILE output includes cumulative metrics across all operators
- Memory constraints may apply to very large result sets with PROFILE

## Performance Tips

1. Use **EXPLAIN** first to understand query structure before running PROFILE
2. Profile with realistic data volumes to get accurate timing
3. Compare multiple query formulations using PROFILE
4. Use EXPLAIN to verify index usage when queries are slow
5. Profile individual subpatterns to isolate performance issues
