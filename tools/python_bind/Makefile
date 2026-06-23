SHELL := /bin/bash

REPO_ROOT  := $(abspath $(CURDIR)/../..)
ROOT_BUILD := $(REPO_ROOT)/build
CACHE_FILE := $(ROOT_BUILD)/CMakeCache.txt

BUILD_TYPE         ?= Release
EXTRA_CMAKE_FLAGS  ?=

# `cmake --build -j` without N is unlimited parallelism — tanks memory-limited
# machines on heavy template instantiation. Default to detected CPU count;
# override with `JOBS=N`.
NPROC := $(shell { command -v nproc >/dev/null 2>&1 && nproc; } 2>/dev/null \
              || { command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu; } 2>/dev/null \
              || echo 4)
JOBS   ?= $(NPROC)

# Lock cmake's Python detection to a specific interpreter; without this
# `find_package(Python)` may pick a different version than the one running
# pytest, producing a wrong-ABI .so. Override with `PYTHON=/path/to/python3`.
PYTHON ?= $(shell command -v python3)

CMAKE_PARALLEL = CMAKE_BUILD_PARALLEL_LEVEL="$${CMAKE_BUILD_PARALLEL_LEVEL:-$(JOBS)}"

.PHONY: help requirements proto build dev dev-full wheel clean

.DEFAULT_GOAL := help

requirements:  ## Install Python deps (one-time)
	$(PYTHON) -m pip install -r requirements.txt -r requirements_dev.txt

proto:  ## Compile protobuf files
	$(PYTHON) setup.py build_proto

# Honors all BUILD_*/WITH_*/CMAKE_* env vars on every invocation. CI uses this.
# Run setup.py under $(PYTHON) so sys.executable matches the locked interpreter.
build:  ## Build via setup.py (honors all BUILD_* env vars; CI entry)
	$(CMAKE_PARALLEL) $(PYTHON) setup.py build_ext

# Falls through to dev-full when the cache is missing or wasn't configured
# with -DBUILD_PYTHON=ON (avoids cryptic "No rule to make target" errors).
dev:  ## Fast incremental rebuild via cmake (no env-var forwarding)
	@if [ ! -f "$(CACHE_FILE)" ] || ! grep -q "^BUILD_PYTHON:BOOL=ON" "$(CACHE_FILE)"; then \
		echo "Root build missing or BUILD_PYTHON=OFF; bootstrapping via dev-full..."; \
		$(MAKE) dev-full; \
	else \
		cmake --build $(ROOT_BUILD) --target neug_py_bind -j$(JOBS); \
	fi

dev-full:  ## Configure root build + build neug_py_bind from scratch
	cmake -S $(REPO_ROOT) -B $(ROOT_BUILD) -DBUILD_PYTHON=ON \
	    -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
	    -DOPTIMIZE_FOR_HOST=OFF \
	    -DPython_EXECUTABLE=$(PYTHON) -DPYTHON_EXECUTABLE=$(PYTHON) \
	    $(EXTRA_CMAKE_FLAGS)
	cmake --build $(ROOT_BUILD) --target neug_py_bind -j$(JOBS)

wheel:  ## Build the neug python wheel (honors BUILD_* env vars)
	$(CMAKE_PARALLEL) $(PYTHON) setup.py bdist_wheel

clean:  ## Clean Python build artifacts (does NOT touch $(ROOT_BUILD))
	rm -rf build/ dist/ *.egg-info neug.egg-info/ __pycache__/
	@find neug -maxdepth 1 \( -name 'neug_py_bind*.so' -o -name 'libneug*' \) -delete 2>/dev/null || true

help:  ## Display this help information
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | \
		sort
