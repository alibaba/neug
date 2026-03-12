# Dev and Test

## Build NeuG

```bash
cd tools/python_bind
make requirements
make build
```


## Run test


### Test batch loading

```bash
export FLEX_DATA_DIR="/path/to/example_dataset/modern_graph"
cd tools/python_bind
python3 -m pytest -s tests/test_batch_loading.py
```


### Test run cypher query in dev mode


First load the graph
```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=DEBUG -DCMAKE_INSTALL_PREFIX=/opt/neug -DBUILD_EXECUTABLES=OFF -DBUILD_TEST=ON -DBUILD_EXECUTABLES=ON
./tools/utils/bulk_loader -g ../example_dataset/modern_graph/graph.yaml -l ../example_dataset/modern_graph/import.yaml -d /tmp/csr-data
```

Then run the test.

```bash
export MODERN_GRAPH_DB_DIR=/tmp/csr-data
cd tools/python_bind
python3 -m pytest -s tests/test_query.py
```

### End-to-End Cypher Tests

A comprehensive set of end-to-end Cypher tests is available in the `neug/tests/e2e/queries` directory. The following instructions use the `tinysnb` dataset as an example to demonstrate how to run these tests.

#### 1. Load the tinysnb Graph

```bash
export FLEX_DATA_DIR="../example_dataset/tinysnb"
./tools/utils/bulk_loader -g ../example_dataset/tinysnb/graph.yaml -l ../example_dataset/tinysnb/import.yaml -d /tmp/tinysnb
```

#### 2. Run the Tests

To run all end-to-end Cypher tests for the `tinysnb` dataset:

```bash
cd neug/tests/e2e
./scripts/run_embed_test.sh tinysnb /tmp/tinysnb ""
```
This command executes all test cases in `neug/tests/e2e/queries` using the `tinysnb` dataset located at `/tmp/tinysnb`.

To run tests for a specific operator (for example, `filter`), specify the operator's subdirectory as the third argument:

```bash
./scripts/run_embed_test.sh tinysnb /tmp/tinysnb "filter"
```
This will only run tests in `neug/tests/e2e/queries/filter`.

#### 3. View the Test Report

Test reports are generated in the `neug/tests/e2e/report` directory. For example, if you run tests for `filter`, you can open `neug/tests/e2e/report/filter/test_report.html` directly in your browser for a summary.

For better visualization, you can also use Allure:

**Install Allure dependencies:**
    ```bash
    python3 -m pip  install allure-pytest
    ```

**View the report:**
    ```bash
    allure serve path/to/report/filter/allure-results
    ```
    Replace `path/to/report/filter/allure-results` with the actual path to the desired test results directory.

#### 4. Test File Structure

Test files are located under `neug/tests/e2e/queries/` and its subdirectories (e.g., `acc/`, `agg/`, etc.).
Each `.test` file contains multiple test cases, running on the specified dataset with `-DATASET CSV <DataSet>`. Each test case starts with `-LOG <TestName>`, followed by `-STATEMENT <Cypher Query>`, and expected output.

If a test fails and you plan to fix it soon, you can temporarily skip it by adding `-SKIP` after the `-LOG` line. For cases involving operators that are currently not intended for support, you can skip them by adding a `-UNSUPPORTED` mark.

#### 5. Custom Pytest Options

The test framework provides customized options to make test execution flexible and efficient, such as:
```
Custom options:
  --db_dir=DB_DIR       Directory for the database.
  --read_only           Open the database in read-only mode or not (default: False).
  --query_dir=QUERY_DIR
                        Root directory to search for test files.
  --dataset=DATASET     Specific <DataSet> to run tests or benchmarks on.
  --test_names=TEST_NAMES
                        Comma-separated list of <TestName> to run. If not set, run all tests.
  --include_skip_tests  Include tests that are marked as skip to run (default: False).
```

**Usage examples:**

* To run all tests in `neug/tests/e2e/queries/acc` using the `tinysnb` dataset:
```bash
cd neug/tests/e2e
python3 -m pytest -m neug_test --db_dir=/tmp/tinysnb --query_dir=neug/tests/e2e/queries/acc --dataset tinysnb
```

* To further include tests that are marked with -SKIP:
```bash
python3 -m pytest -m neug_test --db_dir=/tmp/tinysnb --query_dir=neug/tests/e2e/queries/acc --dataset tinysnb --include_skip_tests
```

* To run only specific test cases, such as `AspIntersect` and `AspBasic`:
```bash
python3 -m pytest -m neug_test --db_dir=/tmp/tinysnb --query_dir=neug/tests/e2e/queries/acc --dataset tinysnb --test_names AspIntersect,AspBasic
```

### End-to-End Cypher Benchmark

In addition to E2E Cypher tests, this framework also supports benchmarking. The following example demonstrates how to benchmark using the `lsqb` dataset.

#### 1. Load the lsqb Graph

```bash
export FLEX_DATA_DIR="../example_dataset/lsqb"
./tools/utils/bulk_loader -g ../example_dataset/lsqb/graph.yaml -l ../example_dataset/lsqb/import.yaml -d /tmp/lsqb
```

#### 2. Run the Benchmarks

To run end-to-end LSQB benchmarks for the `lsqb` dataset:

```bash
cd neug/tests/e2e
python3 -m pytest -m neug_benchmark --db_dir=/tmp/lsqb/ --query_dir=./queries/lsqb/ --dataset lsqb --read_only --benchmark-save=benchmark-lsqb
```

This command benchmarks all tests in `./queries/lsqb` using the `lsqb` dataset at `/tmp/lsqb` in read-only mode. The benchmark report is saved as `0001_benchmark-lsqb.json` by default.

#### 3. Custom Benchmark Options

Benchmarking uses the same framework and options as the E2E tests, with additional options for controlling benchmark runs:

```
Custom options:
  --iterations=ITERATIONS      Number of iterations for each benchmark
  --rounds=ROUNDS             Number of rounds for each benchmark
  --warmup_rounds=WARMUP_ROUNDS
                              Number of warmup rounds for each benchmark
```

### Generate Code Coverage Report
#### 1. Build with Gcov
```
cd tools/python_bind
export BUILD_TYPE=DEBUG
export ENABLE_GCOV=ON
make build
```

#### 2. Generate Coverage Report
After completing the tests, you can generate a coverage report by following these steps:
```
sudo apt update
sudo apt install lcov -y
python3 -m pip  install fastcov
cd tools/python_bind/build/neug_py_bind
make coverage
genhtml coverage_filtered.info --output-directory coverage_html --branch-coverage
```
You can view the coverage report by opening coverage_html/index.html in your browser.
## log level

When running python test, set environment variable `DEBUG` to `ON`, to display all c++ logs. All c++ logs are suppressed by default. See `setup_logging` method in `neug_bindings.cc`.


## Dev images

- neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-dev:v0.1.0-x86_64: dev container built upload ubuntu for x86_64
- neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-dev:v0.1.0-arm64: dev container built upload ubuntu for amr64

## Alter property on edge
1. Alter property on edge with single property
  Add property: Create a new `DualCsr<RecordView>`, copy the data from the original CSR to the new CSR
  Drop property:Create a new `DualCsr<grape::EmptyType>`, copy the data from the original CSR to the new CSR
2. Alter property on edge with no property
  Add property: Create a new `DualCsr<RecordView>`, copy the data from the original CSR to the new CSR
3. Alter property on edge with multi property
  Add/Drop property: Add/Drop property in table of `DualCsr<RecordView>`

Therefore, when there is only one edge property on edge, there may be two types of DualCsr, `DualCsr<T>` or `DualCsr<RecordView>`. 
When the `EdgeExpand` operator accesses an edge, it creates EdgeData based on the property type, and then retrieves property data according to the type of DualCsr.