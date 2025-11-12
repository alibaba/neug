### Download SF100 dataset

To download and extract the LDBC BI-SF100 dataset, run the following commands:

```bash
wget https://datasets.ldbcouncil.org/bi-pre-audit/bi-sf100-composite-projected-fk.tar.zst
tar -I zstd -vxf bi-sf100-composite-projected-fk.tar.zst
```

### Data preprocessing

First, navigate to the initial snapshot directory and merge the individual .tar.gz data partitions using the provided script:

```bash
export NEUG_DIR=/path/to/neug
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

### Benchmark Results

query | elapsed (ms)
-- | --
bi1 | 9512.8 
bi2a | 2484.37
bi2b | 218.966
bi3 | 1158.86 
bi4 |  14181 
bi5 |  7.0567
bi6 | 25.4272
bi7 | 82.4952
bi8a | 254.522 
bi8b | 2.85174 
bi9 |  9124.9
bi10a | 1357.93
bi10b | 342.626
bi11 | 16184.7 
bi12 | 7494.93 
bi13 |  9904.53 
bi14a | 1568.88
bi14b | 2.43586 
bi15a | -
bi15b | -
bi16a | 706084
bi16b | 244.288
bi17 | -
bi18 | 7010.99 
bi19a | -
bi19b | -
bi20a | -
bi20b | -
