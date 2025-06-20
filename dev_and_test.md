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
pip3 install -r requirements.txt
pip3 install -r requirements_dev.txt
python3 setup.py build_proto
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
./src/bin/bulk_loader -g ../example_dataset/modern_graph/graph.yaml -l ../example_dataset/modern_graph/import.yaml -d /tmp/csr-data
```

Then run the test.

```bash
export MODERN_GRAPH_DB_DIR=/tmp/csr-data
cd tools/python_bind
python3 -m pytest -s tests/test_query.py
```

### More End-to-End Cypher Tests

A comprehensive set of end-to-end Cypher tests is available in the `neug/tests/e2e/queries` directory, primarily using the `tinysnb` dataset.

#### 1. Load the tinysnb Graph

```bash
export FLEX_DATA_DIR="../example_dataset/tinysnb"
./src/bin/bulk_loader -g ../example_dataset/tinysnb/graph.yaml -l ../example_dataset/tinysnb/import.yaml -d /tmp/tinysnb
```

#### 2. Run the Tests

```bash
export LSQB_DATA_DIR=/tmp/lsqb
# Run all test cases in neug/tests/e2e/queries
cd neug/tests/e2e
./scripts/run_tinysnb_test.sh
```

Or, to run tests for a specific operator (for example, `filter`), use the operator's subdirectory name under `neug/tests/e2e/queries` as an argument:

```bash
./scripts/run_tinysnb_test.sh filter
```

#### 3. View the Test Report

Test reports are generated in the `neug/tests/e2e/report` directory. For example, if you run tests for `filter`, you can open `neug/tests/e2e/report/filter/test_report.html` directly in your browser for a summary.

For better visualization, you can also use Allure:

**Install Allure dependencies:**
    ```bash
    pip3 install allure-pytest
    ```

**View the report:**
    ```bash
    allure serve path/to/report/filter/allure-results
    ```
    Replace `path/to/report/filter/allure-results` with the actual path to the desired test results directory.
