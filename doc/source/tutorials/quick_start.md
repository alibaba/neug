# Quick Start Guide

With `neug` installed, you can now dive into the world of graphs using Cypher queries.

````{note}
`Neug` adheres to a property graph model and is strictly typed. For vertex types, one (and only one) primary key is necessary, whereas edge types do not require a primary key. For more details on the graph schema, please refer to the [Neug Type System](../reference/type_system.md).
````

This tutorial will guide you through the creation of a simple `modern_graph`, demonstrate how to load data into it, and show you how to execute queries on the graph. Ensure you have the [neug-python-client](../installation/installation.md#python) installed.

## Dataset Preparation

You can find a simple `modern_graph` dataset in the [neug-repo](https://github.com/GraphScope/neug/tree/main/example_dataset/modern_graph). Download the required CSV files using wget or curl.

```bash
wget https://graphscope.oss-cn-beijing.aliyuncs.com/neug/person.csv
wget https://graphscope.oss-cn-beijing.aliyuncs.com/neug/person_knows_person.csv
```

## Graph Creation

Begin by opening an empty directory to use as the database location, and initialize a new graph. Neug will store all data in this designated directory. Any subsequent modifications to the graph will also be saved to this directory.

Pure-memory mode is also supported by neug, user could create the graph in pure-memory mode by specifying an empty db_path string.

````{note}
In pure-memory mode, the graph is not saved to disk, nor are any modifications recorded. 
This mode may be beneficial when you require temporary computations, have a large graph, or when handling complex queries.
````

```python
from neug.database import Database

# Open an empty directory.
db = Database(db_path = '/tmp/demo_db', "w")
# Or open the database in memory mode
# db = Database(db_path = '', mode = 'w')

# Establish a connection.
conn = db.connect()

# Create the graph schema.
conn.execute("CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));")
conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
```

## Data Import into Graph

Load the records from `person.csv` into the `person` table, and from `person_knows_person.csv` into the `knows` table.

```python
person_csv = 'person.csv'  # Path to the previously downloaded CSV file
person_knows_person_csv = 'person_knows_person.csv'

conn.execute(f'COPY person FROM "{person_csv}"')
# TODO: Add support for specifying the starting/ending label name
conn.execute(f'COPY knows FROM "{person_knows_person_csv}" (from="person", to="person")')
```

## Executing Cypher Queries

Now, you can interactively explore the graph using Cypher.

```python
res = conn.execute("MATCH (n) RETURN count(n);")
for record in res:
    print(record)
```