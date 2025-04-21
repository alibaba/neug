SHELL=/bin/bash

.PHONY: all wheel clean build requirements
all: wheel build


.PHONY: requirements
requirements:  ## Install requirements
	@echo "Installing requirements..."
	pip3 install -r requirements.txt 
	pip3 install -r requirements_dev.txt

.PHONY: build
build: ## Compile nexg and python bindings, and copy to build directory
	@echo "Building C++ and Python bindings..."
	python3 setup.py build_ext

.PHONY: wheel
wheel: ## Build the nexg python wheel package
	@echo "Building wheel package..."
	python3 setup.py bdist_wheel

.PHONY: clean
clean:  ## Clean up build artifacts
	@echo "Cleaning up..."
	rm -rf build/ dist/ *.egg-info
	rm -rf nexg.egg-info/ __pycache__/ *.so

.PHONY: help
help:  ## Display this help information
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
