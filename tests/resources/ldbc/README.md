### Download SF300 dataset
```bash
wget https://repository.surfsara.nl/datasets/cwi/ldbc-snb-interactive-v1-datagen-v100/files/social_network-sf300-CsvComposite-StringDateFormatter.tar.zst
tar -I zstd -xvf social_network-sf300-CsvComposite-StringDateFormatter.tar.zst
```


### Data preprocessing
```bash
export PATH_TO_DATA=/path/to/data
export NEUG_DIER=/path/to/neug
cd $PATH_TO_DATA
pip3 install duckdb
cp {NEUG_DIR}/tests/resources/ldbc/tools/*.py .
python3 ./reorder_post.py ./dynamic/ ./dynamic/post_0_0.csv.rod
python3 ./reorder_comment.py ./dynamic/ ./dynamic/comment_0_0.csv.rod
cp ${NEUG_DIR}/tests/resources/ldbc/tools/add_column.cc .
g++ add_column.cc -o add_column -O3
./add_column ./dynamic/post_0_0.csv ./dynamic/post_hasCreator_person_0_0.csv ./dynamic/post_hasCreator_person_0_0.csv.creation_date
./add_column ./dynamic/comment_0_0.csv ./dynamic/comment_hasCreator_person_0_0.csv ./dynamic/comment_hasCreator_person_0_0.csv.creation_date
```

### Compile neug

Navigate to the NEUG root directory and build the project using CMake:

```bash
cd ${NEUG_DIR} && mkdir build && cd build
cmake .. -DBUILD_HTTP_SERVER=ON -DBUILD_EXECUTABLES=ON && make -j
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
    -g ${NEUG_DIR}/tests/resources/ldbc/configs/graph.yaml \
    -l ${NEUG_DIR}/tests/resources/ldbc/configs/import.yaml \
    -d ${DATA_DIR} \
    -p 64
```

### Run Benchmark

Execute the benchmark using the SF300 configuration file:

```bash
./bin/benchmark -c ${NEUG_DIR}/tests/resources/ldbc/configs/sf300.cfg -m 3 -d ${DATA_DIR}
```

### Benchmark Results

query | elapsed (us)
-- | --
ic1 | 13350.9 
ic2 | 779.042
ic3 | 31575.4 
ic4 | 1655.91
ic5 | 94661.8 
ic6 |  3159.9
ic7 | 48.3869 
ic8 | 19.2792
ic9 |  209177 
ic10 | 15575.2 
ic11 | 292.339
ic12 | 4238.39 
ic13 | 212.271 
ic14 | 52890.7 
