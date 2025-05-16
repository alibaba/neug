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


```bash
export FLEX_DATA_DIR="/path/to/examle_dataset/modern_graph"
cd tools/python_bind
python3 -m pytest -s tests/test_batch_loading.py
```
