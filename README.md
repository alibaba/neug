# nexg
A repo for maintaining the next-gen GraphScope.

# Compiler 
Currently compiler are provided via jar. 

```bash
wget https://graphscope.oss-accelerate-overseas.aliyuncs.com/compiler/compiler-0.0.1-SNAPSHOT-shade.jar
java -Dgraph.schema=/workspaces/nexg/example_dataset/modern_graph/graph.yaml -jar compiler-0.0.1-SNAPSHOT-shade.jar ./tests/engines/interactive_config_test.yaml
```

# Building Wheel

Building wheels for all python version on this platform.

```bash
pip3 install cibuildwheel
cibuildwheel ./tools/python_bind --no-deps
```

Building wheel for local environment

```bash
cd tools/python_bind
python3 setup.py build
```