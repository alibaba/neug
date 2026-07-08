/**
 * Minimal runnable example for NeuG Node.js bindings.
 *
 * Demonstrates:
 *   1. Creating an in-memory graph database
 *   2. Defining node and relationship schemas (CREATE NODE TABLE / REL TABLE)
 *   3. Inserting vertices and edges
 *   4. Querying with Cypher and printing results
 *
 * Prerequisites:
 *   - Build the native binding first:
 *       cd tools/nodejs_bind && make build
 *   - Then run:
 *       node example.js
 */

'use strict';

const { Database } = require('neug');

// 1. Open an in-memory database in read-write mode
const db = new Database({ databasePath: '', mode: 'w' });
const conn = db.connect();

// 2. Define schema: node table "person" and relationship table "knows"
conn.execute(
  'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
);
conn.execute(
  'CREATE REL TABLE knows(FROM person TO person, since INT64);'
);

// 3. Insert vertices
conn.execute("CREATE (p:person {id: 1, name: 'Alice',   age: 30});");
conn.execute("CREATE (p:person {id: 2, name: 'Bob',     age: 25});");
conn.execute("CREATE (p:person {id: 3, name: 'Charlie', age: 35});");

// 4. Insert edges
conn.execute(
  "MATCH (a:person), (b:person) WHERE a.name = 'Alice' AND b.name = 'Bob' " +
  "CREATE (a)-[:knows {since: 2020}]->(b);"
);
conn.execute(
  "MATCH (a:person), (b:person) WHERE a.name = 'Alice' AND b.name = 'Charlie' " +
  "CREATE (a)-[:knows {since: 2023}]->(b);"
);
conn.execute(
  "MATCH (a:person), (b:person) WHERE a.name = 'Bob' AND b.name = 'Charlie' " +
  "CREATE (a)-[:knows {since: 2021}]->(b);"
);

// 5. Query all vertices
console.log('\n=== All Vertices ===');
const vertices = conn.execute('MATCH (n:person) RETURN n.id, n.name, n.age ORDER BY n.id;');
for (const row of vertices) {
  console.log(`  id=${row[0]}, name=${row[1]}, age=${row[2]}`);
}

// 6. Query all edges
console.log('\n=== All Edges (knows) ===');
const edges = conn.execute(
  'MATCH (a:person)-[r:knows]->(b:person) RETURN a.name, r.since, b.name ORDER BY a.name, b.name;'
);
for (const row of edges) {
  console.log(`  ${row[0]} --[knows since ${row[1]}]--> ${row[2]}`);
}

// 7. Parameterized query
console.log('\n=== Parameterized Query (friends of Alice) ===');
const friends = conn.execute(
  "MATCH (a:person)-[:knows]->(b:person) WHERE a.name = $name RETURN b.name, b.age;",
  'read',
  { name: 'Alice' }
);
for (const row of friends) {
  console.log(`  ${row[0]}, age ${row[1]}`);
}

// 8. Aggregation query
console.log('\n=== Aggregation ===');
const agg = conn.execute(
  'MATCH (a:person)-[:knows]->() RETURN a.name, count(*) AS friend_count ORDER BY friend_count DESC;'
);
for (const row of agg) {
  console.log(`  ${row[0]} has ${row[1]} friend(s)`);
}

// 9. Cleanup
conn.close();
db.close();
console.log('Done.');
