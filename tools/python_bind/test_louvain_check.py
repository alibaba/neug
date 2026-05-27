#!/usr/bin/env python3
import os
import shutil
import sys

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "build/lib.macosx-14.0-arm64-cpython-314"),
)
from neug import Database

db_dir = "/tmp/test_louvain_check"
shutil.rmtree(db_dir, ignore_errors=True)
db = Database(db_path=db_dir, mode="w")
conn = db.connect()

conn.execute(
    "CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
)
conn.execute("CREATE REL TABLE Knows(FROM Person TO Person);")

for pid, nm, ag in [
    (0, "Alice", 30),
    (1, "Bob", 25),
    (2, "Charlie", 35),
    (3, "Dave", 28),
    (4, "Eve", 22),
    (5, "Frank", 40),
    (6, "Grace", 33),
    (7, "Hank", 29),
]:
    conn.execute(f'CREATE (p:Person {{id: {pid}, name: "{nm}", age: {ag}}});')

for i in range(4):
    for j in range(i + 1, 4):
        conn.execute(
            f"MATCH (a:Person {{id: {i}}}), (b:Person {{id: {j}}}) CREATE (a)-[:Knows]->(b);"
        )
        conn.execute(
            f"MATCH (a:Person {{id: {i}}}), (b:Person {{id: {j}}}) CREATE (b)-[:Knows]->(a);"
        )

for i in range(4, 8):
    for j in range(i + 1, 8):
        conn.execute(
            f"MATCH (a:Person {{id: {i}}}), (b:Person {{id: {j}}}) CREATE (a)-[:Knows]->(b);"
        )
        conn.execute(
            f"MATCH (a:Person {{id: {i}}}), (b:Person {{id: {j}}}) CREATE (b)-[:Knows]->(a);"
        )

conn.execute("MATCH (a:Person {id: 3}), (b:Person {id: 4}) CREATE (a)-[:Knows]->(b);")

conn.execute("load louvain")

result = conn.execute('CALL louvain(1.0, false, 1e-7, "name,age") RETURN *;')
rows = list(result)

print(f"{len(rows)} rows, {len(rows[0])} columns per row")
print(f"{'node_id':>8} {'community':>10}  {'properties'}")
print("-" * 62)
for r in rows:
    print(f"  {r[0]:>6} {r[1]:>10}  {r[2]}")

conn.close()
db.close()
shutil.rmtree(db_dir, ignore_errors=True)
