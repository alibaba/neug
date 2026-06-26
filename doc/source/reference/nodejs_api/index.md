# NodeJS API Reference

The NeuG NodeJS API provides a high-performance, native JavaScript interface for interacting with NeuG graph databases. Built with N-API for seamless integration with Node.js applications.

## Overview

The NodeJS API offers a simple yet powerful way to:

- **Connect to databases**: Open local or in-memory NeuG databases
- **Execute queries**: Run Cypher queries with automatic result parsing
- **Manage transactions**: Handle ACID transactions for data consistency
- **Process results**: Work with graph data using familiar JavaScript patterns

> **Note:** The Node.js binding currently supports [**embedded mode**](../../overview/introduction) only. Service mode (HTTP server) is not available — it requires the C++ HTTP server component which is not exposed through the N-API binding. If you need service mode, use the [Python binding](../python_api) or the [C++ API](../cpp_api).

## Core Classes

- **[Database](database)** - The main entry point of the NeuG database
- **[Connection](connection)** - Connection represents a logical connection to a database
- **[QueryResult](query_result)** - QueryResult represents the result of a Cypher query


## Quick Start

### Installation

```bash
npm install @graphscope-neug/neug
```

### Basic Usage

```javascript
const { Database } = require('@graphscope-neug/neug');

// Connect to database
const db = new Database({ databasePath: '/path/to/database', mode: 'w' });
const conn = db.connect();

// Execute a simple query
const result = conn.execute('MATCH (n) RETURN n LIMIT 10');

// Process results
for (const record of result) {
  const node = record['n'];
  console.log(`Node ID: ${node.id}, Labels: ${node.labels}`);
}

// Close connection
conn.close();
db.close();
```

### In-Memory Database

```javascript
const { Database } = require('@graphscope-neug/neug');

// Open an in-memory database
const db = new Database({ databasePath: '', mode: 'w' });
const conn = db.connect();

conn.execute('CREATE NODE TABLE Person(id INT64, name STRING, age INT32, PRIMARY KEY(id));');
conn.execute("CREATE (p:Person {id: 1, name: 'Alice', age: 30});");

const result = conn.execute('MATCH (p:Person) RETURN p.id, p.name, p.age;');
for (const row of result) {
  console.log(`id=${row[0]}, name=${row[1]}, age=${row[2]}`);
}

conn.close();
db.close();
```

## Advanced Features

### Access Mode

The `execute` method accepts an optional access mode to hint the query type:

```javascript
// Specify access mode for the query
const result = conn.execute(
  'MATCH (p:Person) RETURN p.name, p.age',
  'read'
);
```

Supported modes: `'read'`/`'r'`, `'insert'`/`'i'`, `'update'`/`'u'`, `'schema'`/`'s'`.

### Parameterized Queries

```javascript
// Safe parameter passing
const result = conn.execute(
  'MATCH (p:Person) WHERE p.age > $min_age RETURN p.name, p.age',
  'read',
  { min_age: 25 }
);
```

## Error Handling

```javascript
try {
  const result = conn.execute('INVALID CYPHER QUERY');
} catch (e) {
  console.error(`Query error: ${e.message}`);
}
```
