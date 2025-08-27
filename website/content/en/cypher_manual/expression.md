# Expression

## Overview

Expressions are fundamental components in Cypher that allow you to compute values, perform calculations, and manipulate data within queries. Unlike traditional SQL which focuses on relational data operations, Cypher expressions are designed specifically for graph data structures and provide powerful capabilities for traversing relationships, pattern matching, and graph-specific computations.

Cypher expressions serve several key purposes:

- **Value Computation**: Calculate derived values from node properties, relationship attributes, or computed results
- **Condition Evaluation**: Create boolean expressions for filtering nodes, relationships, or paths
- **Data Transformation**: Convert and format data types for display or further processing
- **Graph Traversal**: Navigate through graph structures using path expressions and pattern matching
- **Aggregation**: Perform calculations across collections of nodes or relationships
- **String and Text Processing**: Manipulate text data for search, display, or analysis


Cypher expressions are built from two main categories of components:

1. **Operators**: Symbols and keywords that perform basic operations on operands
2. **Functions**: Predefined operations that encapsulate complex logic and take parameters

These components work together to create powerful expressions that can handle both simple calculations and complex graph operations.

## Operators

Operators in Neug are symbols or keywords that perform operations on operands. They are fundamental building blocks for constructing expressions and queries.

### Supported Operator Types

| Type | Description |
|------|-------------|
| [Comparison](./expression/comparison_op.md) | Operators for comparing values (e.g., `=`, `<>`, `<`, `>`, `<=`, `>=`) |
| [Logical](./expression/logical_op.md) | Boolean operators for combining conditions (e.g., `AND`, `OR`, `NOT`) |
| [Arithmetic](./expression/arithmetic_op.md) | Mathematical operations (e.g., `+`, `-`, `*`, `/`, `%`) |
<!-- | Bit | Bitwise operations (e.g., `&`, `|`, `^`, `<<`, `>>`) | -->
| [Null](./expression/null_op.md) | Operations for handling null values (e.g., `IS NULL`, `IS NOT NULL`) |
| [List](./expression/list_op.md) | Operations for working with list data structures (e.g., `IN`) |
<!-- | Case When | Conditional expressions using `CASE WHEN` syntax | -->

## Functions

Functions in Neug are predefined operations that take input parameters and return computed values. They provide specialized functionality for data manipulation and analysis.

### Supported Function Categories

| Category | Description |
|----------|-------------|
| [Aggregate](./expression/agg_func.md) | Functions that operate on collections of values and return a single result (e.g., `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`) |
| [Cast](./expression/cast_func.md) | Functions for converting data types between different formats |
| [Temporal](./expression/temporal_func.md) | Functions for working with date and time data |
| [Graph Pattern](./expression/graph_func.md) | Functions specifically designed for nodes, relationships or path |
<!-- | Text | String manipulation and text processing functions |
| Numeric | Mathematical and numerical computation functions | -->
