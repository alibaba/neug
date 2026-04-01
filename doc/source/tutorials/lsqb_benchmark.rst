LSQB Benchmark Tutorial
=======================

This tutorial demonstrates how to reproduce the LSQB (Labelled Subgraph Query Benchmark)
performance results presented in the NeuG blog article. We will use the LDBC SNB SF1
dataset to run 9 complex subgraph matching queries and compare NeuG's performance.

.. contents:: Table of Contents
   :local:
   :depth: 2

Prerequisites
-------------

- Python 3.10+ with ``pip``
- NeuG Python package: ``pip install neug``
- About 1.5GB disk space for the dataset

Download the Dataset
--------------------

The LDBC SNB SF1 dataset used in this benchmark is available on OSS:

.. code-block:: bash

   # Download and extract the dataset
   wget https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz
   tar -xzf ldbc-snb-sf1-lsqb.tar.gz

The extracted directory structure:

::

   social_network-sf1-CsvComposite-StringDateFormatter/
   ├── dynamic/
   │   ├── person_0_0.csv
   │   ├── person_knows_person_0_0.csv
   │   ├── comment_0_0.csv
   │   ├── post_0_0.csv
   │   └── ...
   └── static/
       ├── place_0_0.csv
       ├── tag_0_0.csv
       └── ...

Benchmark Script
----------------

The complete benchmark script is available in the NeuG repository at
``examples/lsqb_benchmark/run_neug_benchmark.py``. Here's a simplified version:

.. code-block:: python
   :caption: run_neug_benchmark.py (simplified)

   import json
   import time
   import statistics
   from neug.database import Database

   # LSQB-style queries for LDBC SNB schema
   LSQB_QUERIES = {
       # Q1: Long path traversal (9-hop chain)
       1: """
       MATCH (:PLACE {type: 'country'})<-[:ISPARTOF]-(:PLACE {type: 'city'})
             <-[:ISLOCATEDIN]-(:PERSON)<-[:HASMEMBER]-(:FORUM)
             -[:CONTAINEROF]->(:POST)<-[:REPLYOF]-(:COMMENT)
             -[:HASTAG]->(:TAG)-[:HASTYPE]->(:TAGCLASS)
       RETURN count(*) AS count
       """,

       # Q2: 2-hop with comment-post pattern
       2: """
       MATCH (person1:PERSON)-[:KNOWS]->(person2:PERSON),
             (person1)<-[:HASCREATOR]-(comment:COMMENT)
             -[:REPLYOF]->(post:POST)-[:HASCREATOR]->(person2)
       RETURN count(*) AS count
       """,

       # Q3: Triangle pattern in same country
       3: """
       MATCH (country:PLACE {type: 'country'})
       MATCH (person1:PERSON)-[:ISLOCATEDIN]->(city1:PLACE)-[:ISPARTOF]->(country)
       MATCH (person2:PERSON)-[:ISLOCATEDIN]->(city2:PLACE)-[:ISPARTOF]->(country)
       MATCH (person3:PERSON)-[:ISLOCATEDIN]->(city3:PLACE)-[:ISPARTOF]->(country)
       MATCH (person1)-[:KNOWS]->(person2)-[:KNOWS]->(person3)-[:KNOWS]->(person1)
       RETURN count(*) AS count
       """,

       # Q4: Multi-label with likes/replies
       4: """
       MATCH (:TAG)<-[:HASTAG]-(message:POST:COMMENT)-[:HASCREATOR]->(:PERSON),
             (message)<-[:LIKES]-(:PERSON),
             (message)<-[:REPLYOF]-(comment:COMMENT)
       RETURN count(*) AS count
       """,

       # Q5: Tag co-occurrence via comments
       5: """
       MATCH (tag1:TAG)<-[:HASTAG]-(message:POST:COMMENT)
             <-[:REPLYOF]-(comment:COMMENT)-[:HASTAG]->(tag2:TAG)
       WHERE id(tag1) <> id(tag2)
       RETURN count(*) AS count
       """,

       # Q6: 2-hop with interest tags
       6: """
       MATCH (person1:PERSON)-[:KNOWS]->(person2:PERSON)
             -[:KNOWS]->(person3:PERSON)-[:HASINTEREST]->(:TAG)
       WHERE id(person1) <> id(person3)
       RETURN count(*) AS count
       """,

       # Q7: Optional matches for likes/replies
       7: """
       MATCH (:TAG)<-[:HASTAG]-(message:POST:COMMENT)-[:HASCREATOR]->(:PERSON)
       OPTIONAL MATCH (message)<-[:LIKES]-(:PERSON)
       OPTIONAL MATCH (message)<-[:REPLYOF]-(:COMMENT)
       RETURN count(*) AS count
       """,

       # Q8: Tag pattern with NOT EXISTS
       8: """
       MATCH (tag1:TAG)<-[:HASTAG]-(message:POST:COMMENT)
             <-[:REPLYOF]-(comment:COMMENT)-[:HASTAG]->(tag2:TAG)
       WHERE NOT (comment)-[:HASTAG]->(tag1) AND id(tag1) <> id(tag2)
       RETURN count(*) AS count
       """,

       # Q9: 2-hop with NOT EXISTS
       9: """
       MATCH (person1:PERSON)-[:KNOWS]->(person2:PERSON)
             -[:KNOWS]->(person3:PERSON)-[:HASINTEREST]->(:TAG)
       WHERE NOT (person1)-[:KNOWS]->(person3) AND id(person1) <> id(person3)
       RETURN count(*) AS count
       """,
   }

   def run_query(conn, query: str):
       start = time.perf_counter()
       result = conn.execute(query)
       elapsed = (time.perf_counter() - start) * 1000
       data = json.loads(result.get_bolt_response())
       count = data.get("table", [{}])[0].get("count", 0)
       return count, elapsed

   def main():
       db = Database("lsqb_sf1.db")
       conn = db.connect()

       print("Running LSQB SF1 Benchmark...")
       print("-" * 50)

       for qid in range(1, 10):
           query = LSQB_QUERIES[qid]

           # Warmup
           for _ in range(2):
               try:
                   run_query(conn, query)
               except:
                   pass

           # Measure
           times = []
           for _ in range(5):
               count, elapsed = run_query(conn, query)
               times.append(elapsed)

           p50 = statistics.median(times)
           print(f"Q{qid}: {p50:.2f}ms (result: {count})")

       conn.close()
       db.close()

   if __name__ == "__main__":
       main()

Running the Benchmark
---------------------

.. code-block:: bash

   # 1. Create a virtual environment
   python -m venv neug-env
   source neug-env/bin/activate  # Linux/macOS
   # neug-env\Scripts\activate  # Windows

   # 2. Install NeuG
   pip install neug

   # 3. Run the benchmark
   cd neug/examples/lsqb_benchmark
   python run_neug_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb

   # Or with force to overwrite existing database
   python run_neug_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb --force

Expected Results
----------------

On an Apple Silicon Mac (M1/M2/M3), you should see results similar to:

============  ===========  ===================
Query         P50 (ms)     Result
============  ===========  ===================
Q1            ~2,600       179,510,748
Q2            ~140         498,997
Q3            ~370         0 (triangles are rare)
Q4            ~140         16,312,503
Q5            ~830         12,501,170
Q6            ~480         200,468,189
Q7            ~580         26,097,816
Q8            ~710         6,241,640
Q9            ~600         191,485,250
============  ===========  ===================

Performance Notes
-----------------

- NeuG runs single-threaded in embedded mode
- Results may vary based on hardware and system load
- The dataset contains ~3 million nodes and ~17 million edges
- First query execution may be slower due to query compilation

Comparing with LadybugDB
------------------------

To compare with LadybugDB, you need to:

1. Install LadybugDB: ``pip install real_ladybug``
2. Use the benchmark scripts in ``examples/lsqb_benchmark/``
3. LadybugDB supports multi-threading; we compare against best thread count (1-8)

The comparison script automatically runs both engines and generates a comparison report.

Reproducibility
---------------

The complete benchmark code, dataset, and results are available at:

- **Dataset**: https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz
- **Benchmark Scripts**: ``neug/examples/lsqb_benchmark/``
- **Results**: ``neug/examples/lsqb_benchmark/results/``

If you encounter any issues reproducing these results, please open an issue on
`GitHub <https://github.com/alibaba/neug/issues>`_.