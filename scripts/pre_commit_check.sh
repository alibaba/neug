#!/bin/bash
################################################################################
# NeuG Pre-Commit Local Testing Script
#
# This script runs essential checks locally before pushing to GitHub,
# helping catch issues early and save CI resources.
#
# Usage:
#   ./scripts/pre_commit_check.sh [OPTIONS]
#
# Environment Variables:
#   PYTHON_CMD       Python command to use (default: python3)
#   PIP_CMD          Pip command to use (default: python3 -m pip )
#
# Options:
#   --quick          Run only format checks and quick compilation (default)
#   --full           Run format checks, compilation, and all tests
#   --format-only    Run only format checks (C++ and Python)
#   --build-only     Run only build/compilation checks
#   --test-only      Run only tests (skip format and build)
#   --help           Display this help message
#
# Examples:
#   ./scripts/pre_commit_check.sh              # Quick check (format + build)
#   ./scripts/pre_commit_check.sh --full       # Full check (format + build + tests)
#   ./scripts/pre_commit_check.sh --format-only # Only check code formatting
#
#   # Using custom Python environment
#   PYTHON_CMD=python3 ./scripts/pre_commit_check.sh
#   PYTHON_CMD=~/pyenv/neug-0.1.1/bin/python ./scripts/pre_commit_check.sh
#
# Or use Makefile shortcuts:
#   make format-check  # Fast format check (equivalent to --format-only)
#   make full-check    # Full check (equivalent to --full)
################################################################################

set -e  # Exit on error
set -o pipefail  # Catch errors in pipes

# Python configuration - can be overridden by environment variables
PYTHON_CMD="${PYTHON_CMD:-python3}"
PIP_CMD="${PIP_CMD:-python3 -m pip }"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default mode
MODE="quick"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            MODE="quick"
            shift
            ;;
        --full)
            MODE="full"
            shift
            ;;
        --format-only)
            MODE="format-only"
            shift
            ;;
        --build-only)
            MODE="build-only"
            shift
            ;;
        --test-only)
            MODE="test-only"
            shift
            ;;
        --help)
            cat << 'EOF'
NeuG Pre-Commit Local Testing Script

This script runs essential checks locally before pushing to GitHub,
helping catch issues early and save CI resources.

Usage:
  ./scripts/pre_commit_check.sh [OPTIONS]

Environment Variables:
  PYTHON_CMD       Python command to use (default: python3)
  PIP_CMD          Pip command to use (default: python3 -m pip )

Options:
  --quick          Run only format checks and quick compilation (default)
  --full           Run format checks, compilation, and all tests
  --format-only    Run only format checks (C++ and Python)
  --build-only     Run only build/compilation checks
  --test-only      Run only tests (skip format and build)
  --help           Display this help message

Examples:
  ./scripts/pre_commit_check.sh              # Quick check (format + build)
  ./scripts/pre_commit_check.sh --full       # Full check (format + build + tests)
  ./scripts/pre_commit_check.sh --format-only # Only check code formatting

  # Using custom Python environment
  PYTHON_CMD=python3 ./scripts/pre_commit_check.sh
  PYTHON_CMD=~/pyenv/neug-0.1.1/bin/python ./scripts/pre_commit_check.sh

Makefile Shortcuts:
  make format-check  # Fast format check (equivalent to --format-only)
  make full-check    # Full check (equivalent to --full)
EOF
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

################################################################################
# Helper Functions
################################################################################

print_header() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        print_error "$1 is required but not installed."
        return 1
    fi
    return 0
}

################################################################################
# Check Prerequisites
################################################################################

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    local missing_tools=()
    
    # Check for required tools
    if ! check_command "git"; then
        missing_tools+=("git")
    fi
    
    if ! check_command "cmake"; then
        missing_tools+=("cmake")
    fi
    
    if ! check_command "$PYTHON_CMD"; then
        missing_tools+=("$PYTHON_CMD")
    fi
    
    if ! check_command "clang-format"; then
        print_warning "clang-format not found. C++ format check will be skipped."
        print_info "Install with: sudo curl -L https://github.com/muttleyxd/clang-tools-static-binaries/releases/download/master-22538c65/clang-format-8_linux-amd64 --output /usr/local/bin/clang-format && sudo chmod +x /usr/local/bin/clang-format"
    fi
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        print_error "Missing required tools: ${missing_tools[*]}"
        exit 1
    fi
    
    print_success "All required tools are available"
}

################################################################################
# Format Checks
################################################################################

check_cpp_format() {
    print_header "C++ Format Check (clang-format)"
    
    if ! command -v clang-format &> /dev/null; then
        print_warning "clang-format not found, skipping C++ format check"
        return 0
    fi
    
    cd "$PROJECT_ROOT"
    
    # Run clang-format on source files (except protobuf generated files)
    # NOTE: ./include added to align with CI format-check.yml (PR #1711)
    print_info "Running clang-format on C++ files..."
    find ./include ./src ./tests ./tools -name "*.h" ! -name "*pb.h" ! -name "*pb.cc" -exec clang-format -i --style=file {} + 2>/dev/null || true
    
    # Check if clang-format made any changes (only in directories we formatted)
    local git_diff=$(git diff --ignore-submodules -- include/ src/ tests/ tools/ ':(exclude)*.md')
    if [[ -n $git_diff ]]; then
        print_warning "C++ format issues found and auto-fixed:"
        git diff --ignore-submodules --name-only -- include/ src/ tests/ tools/ ':(exclude)*.md'
        print_success "Files have been formatted in-place. Please review and commit the changes."
        return 1
    fi
    
    print_success "C++ format check passed"
    return 0
}

check_python_format() {
    print_header "Python Format Check (isort, black, flake8)"
    
    cd "$PROJECT_ROOT/tools/python_bind"
    
    # Check if Python dev requirements are installed
    if ! $PYTHON_CMD -c "import isort" 2>/dev/null; then
        print_info "Installing Python development requirements..."
        if ! $PIP_CMD install -r requirements_dev.txt -q 2>&1; then
            print_warning "Failed to install Python dev requirements"
            print_info "This is usually fine if you're using a different Python environment"
            print_info "Make sure you have isort, black, and flake8 installed in your environment"
            
            # Try to continue with system-installed tools
            if ! command -v isort &> /dev/null || ! command -v black &> /dev/null || ! command -v flake8 &> /dev/null; then
                print_error "Python format tools not found. Skipping Python format check."
                print_info "Install them with: $PIP_CMD install isort black flake8"
                return 0
            fi
        fi
    fi
    
    local format_failed=false
    
    # Run isort (auto-fix)
    print_info "Running isort..."
    if ! $PYTHON_CMD -m isort --check --diff . > /dev/null 2>&1; then
        $PYTHON_CMD -m isort . 2>&1
        print_warning "isort issues found and auto-fixed in tools/python_bind"
        format_failed=true
    else
        print_success "isort check passed"
    fi
    
    # Run black (auto-fix)
    print_info "Running black..."
    if ! $PYTHON_CMD -m black --check . > /dev/null 2>&1; then
        $PYTHON_CMD -m black . 2>&1
        print_warning "black issues found and auto-fixed in tools/python_bind"
        format_failed=true
    else
        print_success "black check passed"
    fi
    
    # Run flake8 (report only, cannot auto-fix)
    print_info "Running flake8..."
    if ! $PYTHON_CMD -m flake8 . 2>&1; then
        print_error "flake8 check failed in tools/python_bind (needs manual fix)"
        format_failed=true
    else
        print_success "flake8 check passed"
    fi
    
    # Check tests/e2e directory
    cd "$PROJECT_ROOT/tests/e2e"
    print_info "Checking tests/e2e directory..."
    
    if ! $PYTHON_CMD -m isort --check --diff . > /dev/null 2>&1; then
        $PYTHON_CMD -m isort . 2>&1
        print_warning "isort issues found and auto-fixed in tests/e2e"
        format_failed=true
    fi
    
    if ! $PYTHON_CMD -m black --check . > /dev/null 2>&1; then
        $PYTHON_CMD -m black . 2>&1
        print_warning "black issues found and auto-fixed in tests/e2e"
        format_failed=true
    fi
    
    if ! $PYTHON_CMD -m flake8 . 2>&1; then
        print_error "flake8 check failed in tests/e2e (needs manual fix)"
        format_failed=true
    fi
    
    if [ "$format_failed" = true ]; then
        print_success "Auto-fixable issues have been corrected. Please review and commit the changes."
        return 1
    fi
    
    print_success "Python format check passed"
    return 0
}

################################################################################
# Build Check
################################################################################

check_build() {
    print_header "Build Check"
    
    cd "$PROJECT_ROOT/tools/python_bind"
    
    # Check whether C++ sources have changed since last build.
    # If the .so already exists and no C++/cmake files are dirty, skip the
    # expensive cmake/compile step entirely (incremental shortcut).
    local so_file=$(find ./build -maxdepth 3 -name 'neug_py_bind*.so' 2>/dev/null | head -1)
    local cpp_diff=$(cd "$PROJECT_ROOT" && git diff --ignore-submodules --name-only HEAD -- include/ src/ tests/ tools/python_bind/src/ CMakeLists.txt cmake/ 2>/dev/null)
    
    if [[ -n "$so_file" && -z "$cpp_diff" && -d "build/neug_py_bind" ]]; then
        print_info "No C++/cmake changes detected and .so exists — skipping rebuild"
        print_success "Build check passed (cached)"
    else
        if [ -d "build/neug_py_bind" ]; then
            print_info "Build directory exists, performing incremental build..."
        else
            print_info "No previous build found, performing clean build..."
        fi
        
        # Install requirements if needed
        if ! $PYTHON_CMD -c "import pybind11" 2>/dev/null; then
            print_info "Installing Python requirements..."
            if ! $PIP_CMD install -r requirements.txt -q 2>&1; then
                print_error "Failed to install requirements"
                print_info "You may need to use a virtual environment or conda"
                return 1
            fi
        fi
        
        # Build using the configured Python (cmake is incremental)
        print_info "Building NeuG with $PYTHON_CMD ..."
        if $PYTHON_CMD setup.py build_ext 2>&1 | tee /tmp/neug_build.log; then
            print_success "Build successful"
        else
            print_error "Build failed"
            echo ""
            echo "Check /tmp/neug_build.log for details"
            return 1
        fi
    fi
    
    # Make neug importable for all subsequent tests via PYTHONPATH
    # (avoids the slow pip install -e . which re-triggers a full build)
    export PYTHONPATH="$PROJECT_ROOT/tools/python_bind${PYTHONPATH:+:$PYTHONPATH}"
    print_success "PYTHONPATH set — neug importable from tools/python_bind"
    return 0
}

################################################################################
# Test Checks
################################################################################

run_cpp_tests() {
    print_header "C++ Unit Tests (Quick)"
    
    cd "$PROJECT_ROOT/tools/python_bind"
    
    if [ ! -d "build/neug_py_bind" ]; then
        print_error "Build directory not found. Please run build first."
        return 1
    fi
    
    cd build/neug_py_bind
    
    # Set up environment variables
    export MODERN_GRAPH_DATA_DIR="${PROJECT_ROOT}/example_dataset/modern_graph"
    export COMPREHENSIVE_GRAPH_DATA_DIR="${PROJECT_ROOT}/example_dataset/comprehensive_graph"
    export FLEX_DATA_DIR="${PROJECT_ROOT}/example_dataset/modern_graph"
    export TEST_PATH="${PROJECT_ROOT}/tests"
    export TEST_RESOURCE="${PROJECT_ROOT}/tests/compiler"
    
    # Run a subset of quick tests
    local tests=(
        "utils_test"
        "schema_test"
    )
    
    for test in "${tests[@]}"; do
        print_info "Running $test..."
        if ctest -R "$test" --output-on-failure 2>&1; then
            print_success "$test passed"
        else
            print_error "$test failed — aborting remaining C++ tests"
            return 1
        fi
    done
    
    print_success "C++ tests passed"
    return 0
}

run_python_tests() {
    print_header "Python Unit Tests (Quick)"
    
    cd "$PROJECT_ROOT/tools/python_bind"
    
    # Install test requirements
    if ! $PYTHON_CMD -c "import pytest" 2>/dev/null; then
        print_info "Installing test requirements..."
        $PIP_CMD install -r requirements.txt -q
        $PIP_CMD install -r requirements_dev.txt -q
    fi
    
    # Set up environment variables
    export FLEX_DATA_DIR="${PROJECT_ROOT}/example_dataset/modern_graph"
    
    # Prepare test data: always recreate to ensure clean state
    print_info "Preparing test data at /tmp/modern_graph ..."
    rm -rf /tmp/modern_graph
    if ! $PYTHON_CMD -c "from neug.database import Database; db = Database('/tmp/modern_graph', mode='w'); db.load_builtin_dataset('modern_graph'); db.close()" 2>&1; then
        print_warning "Failed to prepare modern_graph test data. Skipping Python tests."
        return 0
    fi
    print_success "modern_graph test data prepared"

    print_info "Preparing test data at /tmp/tinysnb ..."
    rm -rf /tmp/tinysnb
    if ! $PYTHON_CMD -c "from neug.database import Database; db = Database('/tmp/tinysnb', mode='w'); db.load_builtin_dataset('tinysnb'); db.close()" 2>&1; then
        print_warning "Failed to prepare tinysnb test data. Skipping Python tests."
        return 0
    fi
    print_success "tinysnb test data prepared"
    
    # Tests that require the ldbc dataset (only available in CI)
    local ldbc_deselect=(
        "--deselect=tests/test_db_query.py::test_length"
        "--deselect=tests/test_db_query.py::test_nodes_rels"
        "--deselect=tests/test_db_query.py::test_case_expression"
        "--deselect=tests/test_db_query.py::test_to_tuple"
        "--deselect=tests/test_db_query.py::test_dummy_scan"
        "--deselect=tests/test_db_query.py::test_date_time_to_string"
        "--deselect=tests/test_db_query.py::test_start_end_node"
        "--deselect=tests/test_db_query.py::test_shortest_path"
        "--deselect=tests/test_db_query.py::test_properties"
        "--deselect=tests/test_db_query.py::test_internal_id_filter"
    )

    # Run a subset of quick tests
    local tests=(
        "tests/test_db_connection.py"
        "tests/test_db_query.py"
        "tests/test_tp_service.py"
        "tests/test_ddl.py"
        "tests/test_db_init.py"
        "tests/test_db_transaction.py"
    )
    
    for test in "${tests[@]}"; do
        if [ -f "$test" ]; then
            print_info "Running $test..."
            local deselect_args=()
            for ds in "${ldbc_deselect[@]}"; do
                if [[ "$ds" == *"$test"* ]]; then
                    deselect_args+=("$ds")
                fi
            done
            if $PYTHON_CMD -m pytest -sv "$test" "${deselect_args[@]}" 2>&1; then
                print_success "$test passed"
            else
                print_error "$test failed — aborting remaining Python tests"
                return 1
            fi
        fi
    done
    
    # Run complex_test.py (comprehensive capability test script)
    local complex_test="example/complex_test.py"
    if [ -f "$complex_test" ]; then
        print_info "Running $complex_test..."
        if $PYTHON_CMD "$complex_test" 2>&1; then
            print_success "$complex_test passed"
        else
            print_error "$complex_test failed"
            return 1
        fi
    fi
    
    print_success "Python tests passed"
    return 0
}

################################################################################
# Main Execution
################################################################################

main() {
    local start_time=$(date +%s)
    
    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║           NeuG Pre-Commit Local Testing Script                ║"
    echo "║                                                                ║"
    echo "║  Mode: $(printf '%-53s' "$MODE")║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    check_prerequisites
    
    local all_passed=true
    
    # Run checks based on mode
    case $MODE in
        format-only)
            check_cpp_format || all_passed=false
            check_python_format || all_passed=false
            ;;
        build-only)
            check_build || all_passed=false
            ;;
        test-only)
            run_cpp_tests || all_passed=false
            [ "$all_passed" = true ] && { run_python_tests || all_passed=false; }
            ;;
        quick)
            check_cpp_format || all_passed=false
            check_python_format || all_passed=false
            check_build || all_passed=false
            ;;
        full)
            check_cpp_format || all_passed=false
            check_python_format || all_passed=false
            check_build || all_passed=false
            run_cpp_tests || all_passed=false
            [ "$all_passed" = true ] && { run_python_tests || all_passed=false; }
            ;;
    esac
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo ""
    echo -e "${BLUE}========================================${NC}"
    if [ "$all_passed" = true ]; then
        echo -e "${GREEN}✓ All checks passed! (${duration}s)${NC}"
        echo -e "${GREEN}  You're good to commit and push!${NC}"
        echo -e "${BLUE}========================================${NC}"
        exit 0
    else
        echo -e "${RED}✗ Some checks failed! (${duration}s)${NC}"
        echo -e "${RED}  Please fix the issues before committing.${NC}"
        echo -e "${BLUE}========================================${NC}"
        exit 1
    fi
}

# Run main function
main
