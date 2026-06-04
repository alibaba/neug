SHELL := /bin/bash

ROOT_BUILD         := $(CURDIR)/build
BUILD_TYPE         ?= Release
EXTRA_CMAKE_FLAGS  ?=

NPROC := $(shell { command -v nproc >/dev/null 2>&1 && nproc; } 2>/dev/null \
              || { command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu; } 2>/dev/null \
              || echo 4)
JOBS  ?= $(NPROC)

.PHONY: help check-tools cpp-build cpp-test python-dev python-wheel python-clean clean dist-clean format-check full-check

.DEFAULT_GOAL := help

check-tools:
	@command -v cmake >/dev/null 2>&1 || { echo >&2 "CMake is required but not found."; exit 1; }

cpp-build: check-tools  ## Build C++ core only (no Python bindings)
	cmake -S . -B $(ROOT_BUILD) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DBUILD_PYTHON=OFF $(EXTRA_CMAKE_FLAGS)
	cmake --build $(ROOT_BUILD) -j$(JOBS)

cpp-test: check-tools  ## Build C++ with tests enabled + run ctest
	cmake -S . -B $(ROOT_BUILD) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DBUILD_PYTHON=OFF -DBUILD_TEST=ON $(EXTRA_CMAKE_FLAGS)
	cmake --build $(ROOT_BUILD) -j$(JOBS)
	ctest --test-dir $(ROOT_BUILD) --output-on-failure

python-dev: check-tools  ## Install Python dev environment (bootstraps root build)
	@cd tools/python_bind && \
	$(MAKE) requirements && \
	$(MAKE) dev-full

python-wheel: check-tools  ## Build the neug python wheel package
	@cd tools/python_bind && $(MAKE) wheel

python-clean:  ## Clean Python build artifacts (does NOT touch <repo>/build)
	@cd tools/python_bind && $(MAKE) clean

clean: python-clean  ## Clean Python build artifacts (alias for python-clean)

dist-clean: python-clean  ## Clean Python artifacts AND the root build dir
	rm -rf $(ROOT_BUILD)

format-check:  ## Run format checks only (C++ and Python)
	@bash scripts/pre_commit_check.sh --format-only

full-check:  ## Run full checks (format + build + tests)
	@bash scripts/pre_commit_check.sh --full

help:  ## Display this help information
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | \
		sort
