# C++ API Reference

The NeuG C++ API provides low-level access to graph database functionality, designed for performance-critical applications and advanced graph operations.

## Key API Classes ⭐

The following classes are the primary entry points and most commonly used components:

## Namespace Organization

The C++ API is organized into the following namespaces:

- **[neug](neug)** -  (1498 classes)


### Namespace Categories

- **Core namespaces** (`gs`, `common`): Fundamental database operations and data structures
- **Query processing** (`algebra`, `cypher`, `physical`): Graph algorithms and query execution
- **Data management** (`results`, `schema`): Result handling and schema management  
- **Infrastructure** (`server`): Server and networking components

## Quick Start

### Include Headers

```cpp
#include <neug/neug.h>
```

### Basic Usage

```cpp
// Connect to database
gs::Database db("path/to/database");
auto session = db.connect();

// Execute query
auto result = session.execute("MATCH (n) RETURN n LIMIT 10");

// Process results
for (const auto& record : result) {
    // Handle each record
}
```

## Building

The C++ API requires:

- **C++17 or later**: Modern C++ features for type safety and performance
- **CMake 3.15+**: Build system configuration
- **Required dependencies**: See build documentation for complete list

### CMake Integration

```cmake
find_package(NeuG REQUIRED)
target_link_libraries(your_target NeuG::neug)
```

## Design Principles

The C++ API is designed with the following principles:

- **Performance**: Zero-cost abstractions and efficient memory usage
- **Type Safety**: Compile-time type checking for graph operations
- **Extensibility**: Plugin architecture for custom algorithms
- **Compatibility**: Standard C++ interfaces for easy integration

