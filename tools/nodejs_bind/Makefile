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

# Normalize BUILD_TYPE → CMake-style {Release,Debug}. Drives both the
# CMake CMAKE_BUILD_TYPE flag and the output directory name.
ifeq ($(filter $(BUILD_TYPE),DEBUG Debug debug),)
  BUILD_TYPE_NORM := Release
else
  BUILD_TYPE_NORM := Debug
endif

# Single source of truth for native artifacts: build/<Release|Debug>/<platform>/
# Used both for local dev (node example.js) and npm pack distribution.
# `make pack` overrides BUILD_TYPE=Release internally regardless of env.
PLATFORM_DIR := build/$(BUILD_TYPE_NORM)/$(PREBUILD_PLATFORM)

# Platform-specific shared library names
ifeq ($(UNAME_S),Darwin)
  NEUG_SHARED_LIB := libneug.dylib
  MIMALLOC_SRC_LIB := libmimalloc.2.0.dylib
  MIMALLOC_DST_LIB := libmimalloc.2.dylib
else
  NEUG_SHARED_LIB := libneug.so
  MIMALLOC_SRC_LIB := libmimalloc.so.2.0
  MIMALLOC_DST_LIB := libmimalloc.so.2
endif

.PHONY: all build dev clean install requirements pack help

all: dev

.PHONY: requirements
requirements:  ## Install Node.js dependencies
	@echo "Installing requirements..."
	npm install

.PHONY: install
install: requirements  ## Install Node.js dependencies (alias for requirements)

.PHONY: dev
dev: requirements  ## Compile neug + nodejs binding; stage all native artifacts into $(PLATFORM_DIR)
	@echo "Building neug nodejs package (BUILD_TYPE=$(BUILD_TYPE_NORM))..."
	@mkdir -p $(PROJECT_ROOT)/build
	cd $(PROJECT_ROOT)/build && \
		cmake .. -DBUILD_NODEJS=ON -DBUILD_HTTP_SERVER=ON -DCMAKE_BUILD_TYPE=$(BUILD_TYPE_NORM) && \
		$(MAKE) -j$(or $(CMAKE_BUILD_PARALLEL_LEVEL),8) neug_node_bind
	@mkdir -p $(PLATFORM_DIR)
	@# Stage .node + libneug + libmimalloc together so @loader_path resolves cleanly
	@# for both local `node example.js` and npm-packed tarballs.
	@find $(PROJECT_ROOT)/build -name "neug_node_bind.node" -exec cp {} $(PLATFORM_DIR)/ \;
	@cp $(PROJECT_ROOT)/build/src/$(NEUG_SHARED_LIB) $(PLATFORM_DIR)/
	@# mimalloc is shipped under its soname (libmimalloc.2.dylib / .so.2);
	@# npm does not preserve symlinks, so rename the real file to the soname.
	@cp $(PROJECT_ROOT)/build/third_party/mimalloc/$(MIMALLOC_SRC_LIB) $(PLATFORM_DIR)/$(MIMALLOC_DST_LIB)
	@echo "Build complete. Contents of $(PLATFORM_DIR):"
	@ls -lh $(PLATFORM_DIR)/

.PHONY: build
build: dev  ## Build the neug nodejs package, alias for dev

.PHONY: pack
pack: build  ## Create per-platform npm tarball (uses default Release build)
	@echo "Packing npm package for platform: $(PREBUILD_PLATFORM)"
	@# Temporarily inject os/cpu into package.json so npm rejects cross-platform installs.
	@cp package.json package.json.orig
	@node -e 'const fs=require("fs"); const p=JSON.parse(fs.readFileSync("package.json")); p.os=[process.platform]; p.cpu=[process.arch]; fs.writeFileSync("package.json", JSON.stringify(p,null,2)+"\n")'
	@VERSION=$$(node -p 'require("./package.json").version'); \
		npm pack && \
		mv neug-nodejs-$$VERSION.tgz neug-nodejs-$$VERSION-$(PREBUILD_PLATFORM).tgz; \
		PACK_RC=$$?; \
		mv package.json.orig package.json; \
		exit $$PACK_RC
	@echo "Package created: neug-nodejs-*-$(PREBUILD_PLATFORM).tgz"

.PHONY: clean
clean:  ## Clean up build artifacts
	@echo "Cleaning up..."
	rm -rf build/ node_modules/
	@echo "Clean complete."

.PHONY: help
help:  ## Display this help information
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
