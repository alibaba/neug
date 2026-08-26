SHELL := /bin/bash

REPO_ROOT  := $(abspath $(CURDIR)/../..)
ROOT_BUILD := $(REPO_ROOT)/build

BUILD_TYPE         ?= Release
NEUG_NATIVE_ARCH   ?= OFF
EXTRA_CMAKE_FLAGS  ?=
PACK_CMAKE_FLAGS   := -DNEUG_PACKAGE_BUILD=ON

# Detect platform: {os}_{arch}
#   OS:   Linux -> linux, Darwin -> osx
#   Arch: x86_64/amd64 -> x86_64, aarch64/arm64 -> arm64
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# OS tag
ifeq ($(UNAME_S),Linux)
  OS_TAG := linux
else ifeq ($(UNAME_S),Darwin)
  OS_TAG := osx
else
  OS_TAG := $(UNAME_S)
endif

# Arch tag
ifneq (,$(filter $(UNAME_M),x86_64 amd64))
  ARCH_TAG := x86_64
else ifneq (,$(filter $(UNAME_M),aarch64 arm64))
  ARCH_TAG := arm64
else
  ARCH_TAG := $(UNAME_M)
endif

PREBUILD_PLATFORM := $(OS_TAG)_$(ARCH_TAG)

# Parallel build jobs
NPROC := $(shell { command -v nproc >/dev/null 2>&1 && nproc; } 2>/dev/null \
              || { command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu; } 2>/dev/null \
              || echo 4)
JOBS   ?= $(NPROC)

# Platform-specific shared library name
ifeq ($(UNAME_S),Darwin)
  NEUG_SHARED_LIB := libneug.dylib
else
  NEUG_SHARED_LIB := libneug.so
endif

# Local staging directory for the addon and its shared-library dependencies
PLATFORM_DIR := build/$(BUILD_TYPE)/$(PREBUILD_PLATFORM)

.PHONY: help dev build stage bundle pack proto clean

.DEFAULT_GOAL := help

# One command does everything: install deps, configure cmake (if needed), build, stage.
dev:  ## Build neug_node_bind (auto-bootstrap if needed)
	npm install
	cmake -S $(REPO_ROOT) -B $(ROOT_BUILD) -DBUILD_NODEJS=ON -DBUILD_PYTHON=OFF \
	    -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
	    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
	    -DNEUG_NATIVE_ARCH=$(NEUG_NATIVE_ARCH) \
	    $(EXTRA_CMAKE_FLAGS)
	cmake --build $(ROOT_BUILD) --target neug_node_bind bulk_loader -j$(JOBS)
	@$(MAKE) stage

build: dev  ## Alias for dev

# Stage the Node.js addon and core NeuG shared library.
stage:
	@rm -rf "$(PLATFORM_DIR)"
	@mkdir -p $(PLATFORM_DIR)
	@find $(ROOT_BUILD) -name "neug_node_bind.node" -exec cp {} $(PLATFORM_DIR)/ \;
	@cp $(ROOT_BUILD)/src/$(NEUG_SHARED_LIB) $(PLATFORM_DIR)/
	@echo "Artifacts staged in $(PLATFORM_DIR):"
	@ls -lh $(PLATFORM_DIR)/

bundle:  ## Collect linked third-party libraries and replace build-machine RPATHs
ifeq ($(UNAME_S),Darwin)
	delocate-path --sanitize-rpaths -v -L .dylibs "$(PLATFORM_DIR)"
	@find "$(PLATFORM_DIR)" -type f \( -name '*.node' -o -name '*.dylib' \) \
		-exec otool -L {} \;
else
	@set -e; \
	lib_dir="$(PLATFORM_DIR)/.libs"; \
	mkdir -p "$$lib_dir"; \
	for binary in "$(PLATFORM_DIR)/neug_node_bind.node" "$(PLATFORM_DIR)/libneug.so"; do \
		ldd "$$binary"; \
	done | awk '$$2 == "=>" && $$3 ~ /^\// { print $$1 "|" $$3 }' | sort -u | \
	while IFS='|' read -r soname source; do \
		case "$$soname" in \
			linux-vdso.so.*|ld-linux*.so.*|libc.so.*|libm.so.*|libpthread.so.*|libdl.so.*|librt.so.*|libgcc_s.so.*|libstdc++.so.*|libutil.so.*|libresolv.so.*|libnsl.so.*) continue ;; \
		esac; \
		echo "Copying $$source -> $$lib_dir/$$soname"; \
		cp -L "$$source" "$$lib_dir/$$soname"; \
	done; \
	patchelf --set-rpath '$$ORIGIN:$$ORIGIN/.libs' "$(PLATFORM_DIR)/neug_node_bind.node"; \
	patchelf --set-rpath '$$ORIGIN:$$ORIGIN/.libs' "$(PLATFORM_DIR)/libneug.so"; \
	find "$$lib_dir" -type f -exec patchelf --set-rpath '$$ORIGIN' {} \;; \
	find "$(PLATFORM_DIR)" -type f \( -name '*.node' -o -name '*.so' -o -name '*.so.*' \) \
		-exec sh -c 'echo "$$1:"; patchelf --print-rpath "$$1"; ldd "$$1"' sh {} \;
endif

proto:  ## Regenerate lib/proto/error_pb.js from proto/error.proto
	node scripts/generate_proto.js

pack:  ## Create per-platform npm tarball
	@$(MAKE) dev NEUG_NATIVE_ARCH=OFF EXTRA_CMAKE_FLAGS="$(EXTRA_CMAKE_FLAGS) $(PACK_CMAKE_FLAGS)"
	@$(MAKE) bundle
	@echo "Packing npm package for platform: $(PREBUILD_PLATFORM)"
	@cp package.json package.json.orig
	@node -e 'const fs=require("fs"); const p=JSON.parse(fs.readFileSync("package.json")); p.name="@graphscope-neug/$(OS_TAG)-$(ARCH_TAG)"; p.os=[process.platform]; p.cpu=[process.arch]; fs.writeFileSync("package.json", JSON.stringify(p,null,2)+"\n")'
	@npm pack && \
		PACK_RC=$$?; \
		mv package.json.orig package.json; \
		exit $$PACK_RC
	@echo "Package created: graphscope-neug-$(OS_TAG)-$(ARCH_TAG)-*.tgz"

clean:  ## Clean Node.js build artifacts (does NOT touch $(ROOT_BUILD))
	rm -rf build/

help:  ## Display this help information
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | \
		sort
