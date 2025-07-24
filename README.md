# NeuG

**NeuG** (pronounced "new-gee") is a Hybrid Transactional/Analytical Processing (HTAP) graph database designed for high-performance embedded graph analytics and real-time transaction processing.

[![NeuG Test](https://github.com/GraphScope/neug/actions/workflows/neug-test.yml/badge.svg)](https://github.com/GraphScope/neug/actions/workflows/neug-test.yml)
[![NeuG Wheel Packaging](https://github.com/GraphScope/neug/actions/workflows/build-wheel.yml/badge.svg)](https://github.com/GraphScope/neug/actions/workflows/build-wheel.yml)
[![NeuG Documentation](https://github.com/GraphScope/neug/actions/workflows/docs.yml/badge.svg)](https://github.com/GraphScope/neug/actions/workflows/docs.yml)

## Table of Contents

- [Installation](#installation)
- [Documentation](#documentation)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Installation

**NeuG** is available as a Python package and can be easily installed using `pip`:

```bash
pip3 install neug
```

Please note that `neug` requires `Python` version 3.8 or above and `pip` version 19.3 or higher. The package is developed and tested on major Linux distributions (Ubuntu 20.04+ / CentOS 7+) as well as macOS 11+ (Intel/Apple silicon). Windows users are encouraged to install Ubuntu via WSL2 to use this package.

For more detailed installation instructions, please refer to the [installation guide](./doc/source/installation/installation.md).

## Documentation

We are working on the documentation. For the latest information, visit [NeuG Documentation](https://github.com/GraphScope/neug).(TODO: release the doc)

You can also build the NeuG documentation locally:

```bash
cd doc
make dependencies
make html
python3 -m http.server --directory build/html 8080 # Access the documentation at http://localhost:8080
```

## Development

For instructions on building NeuG from source, as well as for development and debugging, consult the [Development Guide](./doc/source/development/dev_guide.md).

To test NeuG's functionality, see the [Testing](./doc/source/development/dev_and_test.md) section.

For information on the code style adhered to by NeuG, please refer to the [Code Style Guide](./doc/source/development/code_style_guide.md).

## Contributing

We deeply value any contributions you make! Please refer to the [Development](#development) and [Testing](doc/source/development/dev_and_test.md) sections for instructions on building and testing NeuG. Before contributing, be sure to read our [Contributing Guide](./CONTRIBUTING.md).

- To report bugs, [submit an issue on GitHub](https://github.com/GraphScope/neug/issues).
- Submit your contributions via [pull requests](https://github.com/GraphScope/neug/pulls).

## License

NeuG is distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). Please be aware that third-party libraries may have different licenses from GraphScope.