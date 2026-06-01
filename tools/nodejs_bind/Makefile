SHELL=/bin/bash

# Path to the project root (2 levels up from tools/nodejs_bind/)
PROJECT_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)

# Detect prebuild platform tag (linux-x64, linux-arm64, darwin-x64, darwin-arm64)
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_S),Linux)
  ifneq (,$(filter $(UNAME_M),x86_64 amd64))
    PREBUILD_PLATFORM := linux-x64
  else ifneq (,$(filter $(UNAME_M),aarch64 arm64))
    PREBUILD_PLATFORM := linux-arm64
  endif
else ifeq ($(UNAME_S),Darwin)
  ifneq (,$(filter $(UNAME_M),x86_64 amd64))
    PREBUILD_PLATFORM := darwin-x64
  else ifneq (,$(filter $(UNAME_M),arm64 aarch64))
    PREBUILD_PLATFORM := darwin-arm64
  endif
endif

PREBUILD_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))/prebuilds/$(PREBUILD_PLATFORM)

.PHONY: all build dev clean install requirements pack prebuild help

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

# Copy shared libraries (libneug.so, libmimalloc.so) into prebuilds/<platform>/
# alongside neug_node_bind.node, so the npm package is self-contained.
.PHONY: prebuild
prebuild: dev  ## Prepare prebuilds/<platform>/ with .node + shared libs for packaging
	@echo "Preparing prebuilds for platform: $(PREBUILD_PLATFORM)"
	@mkdir -p $(PREBUILD_DIR)
	@# Copy the .node binding
	@find $(PROJECT_ROOT)/build -name "neug_node_bind.node" -exec cp {} $(PREBUILD_DIR)/ \;
	@# Copy libneug.so (core shared library)
	@cp $(PROJECT_ROOT)/build/src/libneug.so $(PREBUILD_DIR)/
	@# Copy libmimalloc: libneug.so NEEDED libmimalloc.so.2 (soname).
	@# npm does not preserve symlinks, so copy the real file as libmimalloc.so.2
	@# to ensure the dynamic linker can resolve it at runtime.
	@cp $(PROJECT_ROOT)/build/third_party/mimalloc/libmimalloc.so.2.0 $(PREBUILD_DIR)/libmimalloc.so.2
	@echo "Prebuild complete. Contents of $(PREBUILD_DIR):"
	@ls -lh $(PREBUILD_DIR)/

.PHONY: pack
pack: prebuild  ## Create npm package tarball (includes shared libs in prebuilds/)
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
