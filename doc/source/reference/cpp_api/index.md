# C++ API Reference

The NeuG C++ API provides high-performance, low-level access to graph database functionality. Designed for performance-critical applications, embedded systems, and advanced graph algorithms.

## Overview

The C++ API offers powerful capabilities for:

- **Database Management**: Open, configure, and manage NeuG database instances
- **Query Execution**: Execute Cypher queries with parameterized inputs
- **Result Processing**: Iterate over query results with type-safe access

## Core Classes

- **[NeugDB](neug_db)** - The main entry point for database operations
- **[Connection](connection)** - Execute Cypher queries against the database
- **[QueryResult](query_result)** - Container for query results with iterator access
- **[NeugDBService](service)** - HTTP service for high-throughput scenarios

## Quick Start

### Include Headers

```cpp
#include <neug/main/neug_db.h>
#include <neug/main/connection.h>
```

### Basic Usage

```cpp
#include <neug/main/neug_db.h>
#include <iostream>

int main() {
  // Create and open database
  neug::NeugDB db;
  db.Open("/path/to/graph", 4);  // 4 threads

  // Create connection and execute query
  auto conn = db.Connect();
  auto result = conn->Query("MATCH (n:Person) RETURN n.name LIMIT 10", "read");

  // Process results
  if (result.has_value()) {
    for (const auto& record : result.value()) {
      std::cout << record.ToString() << std::endl;
    }
  }

  // Close database
  db.Close();
  return 0;
}
```

## Error Handling

```cpp
auto result = conn->Query("INVALID QUERY", "read");
if (!result.has_value()) {
  std::cerr << "Query failed: " << result.error().message() << std::endl;
}
```

## Thread Safety

- `NeugDB`: Thread-safe for all operations
- `Connection`: NOT thread-safe; use one connection per thread
- `QueryResult`: Thread-safe (read-only after creation)

