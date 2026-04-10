import os
import sys
import time

import neug

CYPHER_FILE = os.path.join(os.path.dirname(__file__), "load.cypher")
DB_DIR = os.path.join(os.getcwd(), "ldbc")
DATA_PATH = os.environ.get("DATA_PATH", "") or os.getcwd()


def load():
    total_start = time.time()
    db = neug.Database(DB_DIR)
    conn = db.connect()
    print(f"DATA_PATH = {DATA_PATH}")

    with open(CYPHER_FILE, "r") as f:
        buf = ""
        for line_no, line in enumerate(f, 1):
            stripped = line.strip()
            # skip empty lines and comments
            if not stripped or stripped.startswith("//"):
                continue
            buf += " " + stripped if buf else stripped
            if buf.rstrip().endswith(";"):
                query = buf.rstrip().replace("${DATA_PATH}", DATA_PATH)
                print(f"[line {line_no}] Executing: {query}")
                t0 = time.time()
                try:
                    conn.execute(query)
                    print(f"  -> OK ({time.time() - t0:.2f}s)")
                except Exception as e:
                    print(f"  -> FAILED: {e}", file=sys.stderr)
                buf = ""

    conn.close()
    db.close()
    total_elapsed = time.time() - total_start
    print(f"Done. Total time: {total_elapsed:.2f}s")


if __name__ == "__main__":
    load()
