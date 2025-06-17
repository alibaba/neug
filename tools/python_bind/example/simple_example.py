import os

from neug.database import Database

# Open a empty graph.
db = Database("/tmp/csr-data-dir/", "r", 0, "dummy", "/tmp", "/tmp")
conn = db.connect()
res = conn.execute("MATCH(n) RETURN n;")
print(res)
