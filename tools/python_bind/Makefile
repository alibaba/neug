SHELL=/bin/bash

.PHONY: all wheel clean dev build requirements
all: wheel dev


.PHONY: requirements
requirements:  ## Install requirements
	@echo "Installing requirements..."
	python3 -m pip install -r requirements.txt
	python3 -m pip install -r requirements_dev.txt

.PHONY: proto
proto:  ## Compile protobuf files. proto is not included in dev since it may not be frequently changed
	@echo "Compiling protobuf files..."
	python3 setup.py build_proto

.PHONY: dev
dev: ## Compile neug and python bindings, and copy to build directory
	@echo "Building neug python package..."
	python3 setup.py build_ext

.PHONY: build
build: dev ## Build the neug python package, alias for dev

.PHONY: wheel
wheel: ## Build the neug python wheel package
	@echo "Building wheel package..."
	python3 setup.py bdist_wheel

.PHONY: clean
clean:  ## Clean up build artifacts
	@echo "Cleaning up..."
	rm -rf build/ dist/ *.egg-info
	rm -rf neug.egg-info/ __pycache__/ *.so

.PHONY: help
help:  ## Display this help information
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
