# Expression

Expressions are fundamental components in Cypher that allow you to compute values, perform calculations, and manipulate data within queries. Unlike traditional SQL which focuses on relational data operations, Cypher expressions are designed specifically for graph data structures and provide powerful capabilities for traversing edges, pattern matching, and graph-specific computations.

Cypher expressions serve several key purposes:

- **Value Computation**: Calculate derived values from node properties, edge attributes, or computed results
- **Condition Evaluation**: Create boolean expressions for filtering nodes, edges, or paths
- **Data Transformation**: Convert and format data types for display or further processing
- **Graph Traversal**: Navigate through graph structures using path expressions and pattern matching
- **Aggregation**: Perform calculations across collections of nodes or edges
- **String and Text Processing**: Manipulate text data for search, display, or analysis


Cypher expressions are built from two main categories of components:

1. **Operators**: Symbols and keywords that perform basic operations on operands
2. **Functions**: Predefined operations that encapsulate complex logic and take parameters

These components work together to create powerful expressions that can handle both simple calculations and complex graph operations.

## Operators

Operators in NeuG are symbols or keywords that perform operations on operands. They are fundamental building blocks for constructing expressions and queries.

### Supported Operator Types

| Type | Description |
|------|-------------|
| [Comparison](comparison_op) | Operators for comparing values (e.g., `=`, `<>`, `<`, `>`, `<=`, `>=`) |
| [Logical](logical_op) | Boolean operators for combining conditions (e.g., `AND`, `OR`, `NOT`) |
| [Arithmetic](arithmetic_op) | Mathematical operations (e.g., `+`, `-`, `*`, `/`, `%`) |
| [Null](null_op) | Operations for handling null values (e.g., `IS NULL`, `IS NOT NULL`) |
| [List](list_op) | Operations for working with list data structures (e.g., `IN`) |
<!-- | Bit | Bitwise operations (e.g., `&`, `|`, `^`, `<<`, `>>`) | -->
<!-- | Case When | Conditional expressions using `CASE WHEN` syntax | -->

## Functions

Functions in NeuG are predefined operations that take input parameters and return computed values. They provide specialized functionality for data manipulation and analysis.

### Supported Function Categories

| Category | Description |
|----------|-------------|
| [Aggregate](agg_func) | Functions that operate on collections of values and return a single result (e.g., `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`) |
| [Cast](cast_func) | Functions for converting data types between different formats |
| [Temporal](temporal_func) | Functions for working with date and time data |
| [Graph Pattern](graph_func) | Functions specifically designed for nodes, edges or path |
<!-- | Text | String manipulation and text processing functions |
| Numeric | Mathematical and numerical computation functions | -->
