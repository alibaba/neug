# nexg
A repo for maintaining the next-gen GraphScope.

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