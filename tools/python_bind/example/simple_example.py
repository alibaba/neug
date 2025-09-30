import os
import shutil
import sys

import neug
from neug.database import Database

if __name__ == "__main__":
    # expect 2 args, csv_data_dir and db_dir
    if len(sys.argv) != 3:
        print("Usage: python simple_example.py <csv_data_dir> <db_dir>")
        sys.exit(1)

    print(f"Neug version {neug.__version__}")
    data_dir = sys.argv[1]
    db_dir = sys.argv[2]
    shutil.rmtree(db_dir, ignore_errors=True)

    print(f"Loading data from {data_dir} into database {db_dir}")

    person_csv = os.path.join(data_dir, "person.csv")
    person_knows_person_csv = os.path.join(data_dir, "person_knows_person.csv")

    db = Database(db_dir, "w")
    conn = db.connect()
    # First create the graph schema
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")

    # Then load data.
    conn.execute(f'COPY person from "{person_csv}"')
    # TODO(zhanglei,xiaoli): support specifying the starting/ending label name
    conn.execute(
        f'COPY knows from "{person_knows_person_csv}" (from="person", to="person")'
    )

    res = conn.execute("MATCH (n)-[e]-(m) return count(e);")
    for record in res:
        print(record)


    db.close()
    db2 = Database("")
    db2.load_builtin_dataset(dataset_name="tinysnb")
    conn2 = db2.connect()
    res2 = conn2.execute("MATCH(n) return count(n);")
    print(res2.__next__()[0])

    conn2.close()
    db2.close()
