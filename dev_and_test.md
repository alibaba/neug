# Dev and Test

## Install depdencies

Skip if you are in dev-container.

```bash
bash scripts/install_deps.sh
source ~/.graphscope_env
```

## Build Neug

```bash
cd tools/python_bind
python3 setup.py build_ext
```


## Run test


### Test batch loading

```bash
export FLEX_DATA_DIR="/path/to/examle_dataset/modern_graph"
cd tools/python_bind
python3 -m pytest -s tests/test_batch_loading.py
```


### Test run cypher query in dev mode


First load the graph
```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=DEBUG -DCMAKE_INSTALL_PREFIX=/opt/graphscope -DBUILD_EXECUTABLES=OFF -DBUILD_TEST=ON -DBUILD_EXECUTABLES=ON
./src/bin/bulk_loader -g ../example_dataset/modern_graph/graph.yaml -l ../example_dataset/modern_graph/bulk_load.yaml -d /tmp/csr-data
```

Then run the test.

```bash
export MODERN_GRAPH_DB_DIR=/tmp/csr-data
cd tools/python_bind
python3 -m pytest -s tests/test_query.py
```
