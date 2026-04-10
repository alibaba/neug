import os

import neug

DB_DIR = os.path.join(os.getcwd(), "ldbc")

if __name__ == "__main__":
    db = neug.Database(DB_DIR, max_thread_num=192)
    print(f"Database opened at {DB_DIR}")
    uri = db.serve(port=57380, host="0.0.0.0", blocking=True)
    print(f"Serving at {uri}")
