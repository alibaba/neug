# The python binding API for NeuG

# Building the Wheel

## Develope

To build a wheel for the local environment, run:

```bash
source ~/.graphscope_env
cd tools/python_bind
export DEBUG=1
pip3 install -r requirements.txt
pip3 install -r requirements_dev.txt
python3 setup.py build_proto
python3 setup.py build_ext
python3 setup.py bdist_wheel
pip3 install dist/*
```

## Distribution

To build wheels for all supported Python versions on this platform, use the following commands:

```bash
pip3 install cibuildwheel
cd ${ROOT_DIR}
cibuildwheel ./tools/python_bind --output-dir wheelhouse
```

# Development Mode Setup

In development mode, wheels are not built or installed. Instead, the required dynamic library is built and copied to `tools/python_bind/build`. Any changes made to the Python codebase will take effect immediately, allowing for seamless reloading of files.

```bash
make develop
# run tests

python3 -m pytest tests/test_a.py
```

or 
```bash
python3 setup.py build_ext --inplace --build-temp=build
```