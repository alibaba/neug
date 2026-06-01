SHELL=/bin/bash

# Path to the project root (2 levels up from tools/nodejs_bind/)
PROJECT_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)

.PHONY: all build dev clean install requirements pack help

all: dev

.PHONY: requirements
requirements:  ## Install Node.js dependencies
	@echo "Installing requirements..."
	npm install

.PHONY: install
install: requirements  ## Install Node.js dependencies (alias for requirements)

.PHONY: dev
dev: requirements  ## Compile neug and nodejs bindings via the main CMake project
	@echo "Building neug nodejs package..."
	@mkdir -p $(PROJECT_ROOT)/build
	cd $(PROJECT_ROOT)/build && \
		cmake .. -DBUILD_NODEJS=ON -DBUILD_HTTP_SERVER=ON -DCMAKE_BUILD_TYPE=$(or $(BUILD_TYPE),Release) && \
		$(MAKE) -j$(or $(CMAKE_BUILD_PARALLEL_LEVEL),8) neug_node_bind
	@mkdir -p build/Release
	@find $(PROJECT_ROOT)/build -name "neug_node_bind.node" -exec cp {} build/Release/ \; 2>/dev/null || true
	@echo "Build complete."

.PHONY: build
build: dev  ## Build the neug nodejs package, alias for dev

.PHONY: pack
pack: dev  ## Create npm package tarball
	@echo "Packing npm package..."
	npm pack
	@echo "Package created: neug-nodejs-*.tgz"

.PHONY: clean
clean:  ## Clean up build artifacts
	@echo "Cleaning up..."
	rm -rf build/ node_modules/
	@echo "Clean complete."

.PHONY: help
help:  ## Display this help information
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
