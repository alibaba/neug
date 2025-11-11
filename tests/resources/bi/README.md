### Download SF100 dataset

To download and extract the LDBC BI-SF100 dataset, run the following commands:

```bash
wget https://datasets.ldbcouncil.org/bi-pre-audit/bi-sf100-composite-projected-fk.tar.zst
tar -I zstd -vxf bi-sf100-composite-projected-fk.tar.zst
```

### Data preprocessing

First, navigate to the initial snapshot directory and merge the individual .tar.gz data partitions using the provided script:

```bash
export GITHUB_WORKSPACE=${NEUG_DIR}
cd bi-sf100-composite-projected-fk/graphs/csv/bi/composite-projected-fk/initial_snapshot/
cp ${NEUG_DIR}/tests/resources/bi/tools/collect.sh .
bash collect.sh
```
Next, execute the precomputation for bi4:

```bash
cp ${NEUG_DIR}/tests/resources/bi/tools/bi4.cpp .
g++ bi4.cpp -o bi4 -O3
./bi4 .
```
Copy and compile the bi4.cpp precomputation tool:

```bash
cp ${NEUG_DIR}/tests/resources/bi/tools/bi6.cpp .
g++ bi6.cpp -o bi6 -O3
./bi6 .
```

### Compile neug

Navigate to the NEUG root directory and build the project using CMake:

```bash
cd ${NEUG_DIR} && mkdir build && cd build
cmake .. && make -j
```


### Load graph

Before loading, edit import.yaml to specify the data directory:

```yaml
loading_config:
  data_source:
    scheme: file
    location: /path/to/your/data 
```

```bash

export DATA_DIR=

./tools/utils/bulk_loader \
    -g ${NEUG_DIR}/tests/resources/bi/configs/graph.yaml \
    -l ${NEUG_DIR}/tests/resources/bi/configs/import.yaml \
    -d ${DATA_DIR} \
    -p 64
```

### Run Benchmark

Execute the benchmark using the SF100 configuration file:

```bash
./bin/benchmark -c ${NEUG_DIR}/tests/resources/bi/configs/sf100.cfg -m 3 -d ${DATA_DIR}
```