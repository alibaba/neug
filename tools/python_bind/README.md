# The python binding API for NexG

## Building

```bash
cd nexg
mkdir build && cd build && cmake .. -DBUILD_PYTHON=ON
make -j
```

Then you will find a python wheel package under `tools/python_bind/dist`, should be like `nexg-0.1.0-cp310-cp310-linux_x86_64.whl`.

```bash
cd tools/python_bind/dist
pip3 install ./*
```

To check whether it has been installed correctly, 

```python
import nexg
print(nexg.__version__)
```