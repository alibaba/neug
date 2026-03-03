.PHONY: python-dev python-wheel python-clean clean format-check full-check help

# Check for required tools at the start of each target

check-tools:
	@command -v cmake >/dev/null 2>&1 || { echo >&2 "CMake is required but not found. Please install CMake."; exit 1; }

all: wheel build

.PHONY: python-dev
python-dev: check-tools  ## Install Python development environment
	@cd tools/python_bind && \
	make requirements && \
	make dev

.PHONY: python-wheel
python-wheel: check-tools ## Build the neug python wheel package
	@cd tools/python_bind && \
	make wheel

.PHONY: python-clean
python-clean: check-tools ## Clean up Python build artifacts
	@cd tools/python_bind && \
	make clean

.PHONY: clean
clean: ## Clean up all build artifacts
	make python-clean

.PHONY: format-check
format-check: ## Run format checks only (C++ and Python)
	@bash scripts/pre_commit_check.sh --format-only

.PHONY: full-check
full-check: ## Run full checks (format + build + tests)
	@bash scripts/pre_commit_check.sh --full

.PHONY: help
help:  ## Display this help information
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
