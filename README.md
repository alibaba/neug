# NeuG
A repo for maintaining the next-gen GraphScope.

[![Neug Test](https://github.com/GraphScope/neug/actions/workflows/neug-test.yml/badge.svg)](https://github.com/GraphScope/neug/actions/workflows/neug-test.yml)
[![Neug wheel packaging](https://github.com/GraphScope/neug/actions/workflows/build-wheel.yml/badge.svg)](https://github.com/GraphScope/neug/actions/workflows/build-wheel.yml)
[![Neug Doc](https://github.com/GraphScope/neug/actions/workflows/docs.yml/badge.svg)](https://github.com/GraphScope/neug/actions/workflows/docs.yml)


# Compiler 
Currently compiler are provided via jar. 

```bash
wget https://graphscope.oss-accelerate-overseas.aliyuncs.com/compiler/compiler-0.0.1-SNAPSHOT-shade.jar
java -Dgraph.schema=/workspaces/neug/example_dataset/modern_graph/graph.yaml \
     -Dgraph.statistics=/tmp/csr-data/statistics.json \
    -jar compiler-0.0.1-SNAPSHOT-shade.jar ./tests/engines/interactive_config_test_cbo.yaml
```

# Building Wheel

## Install the depdencies

```bash
bash scripst/install_dep.sh
source ~/.graphscope_env
```

Building wheels for all python version on this platform.

```bash
# install patchelf
https://github.com/NixOS/patchelf/archive/refs/tags/0.14.4.zip
unzip 0.14.4.zip
cd patchelf-0.14.4
./bootstrap.sh
./configure
make install
```

```bash
pip3 install cibuildwheel
cibuildwheel ./tools/python_bind
auditwheel repair ./wheelhouse/*.whl
```

Building for local development

```bash
cd tools/python_bind
pip3 install -r requirements.txt
pip3 install -r requirements_dev.txt
python3 setup.py build_proto
python3 setup.py build_ext
```

# Start Service

TODO: Currently we use the old interactive_server to provide TP service, in the future, we will use python-based server.


## Build server

```bash
mkdir build && cd build
cmake .. -DBUILD_EXECUTABLES=ON -DBUILD_HTTP_SERVER=ON -DCMAKE_BUILD_TYPE=DEBUG
make -j
```

## Load Graph

```bash
cd build
export FLEX_DATA_DIR=/workspaces/neug/example_dataset/modern_graph
GLOG_v=10 ./src/bin/bulk_loader -g ../example_dataset/modern_graph/graph.yaml -l ../example_dataset/modern_graph/import.yaml -d /tmp/csr-data-dir 
# You will find a statistics.json under /tmp/csr-data-dir 
```

## Start Compiler Service

```bash
java -Dgraph.schema=/workspaces/neug/example_dataset/modern_graph/graph.yaml \
     -Dgraph.statistics=/tmp/csr-data/statistics.json \
    -jar compiler-0.0.1-SNAPSHOT-shade.jar ./tests/engines/interactive_config_test_cbo.yaml
```
## Start NeuG(Interactive) Server

```bash
GLOG_v=10 ./src/bin/interactive_server -c ../tests/engines/interactive_config_test_cbo.yaml -g ../example_dataset/modern_graph/graph.yaml --data-path /tmp/csr-data-dir
```

## Start cypher shell, submit query

```bash
wget https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.19.zip
unzip cypher-shell-4.4.19.zip && cd cypher-shell
```
