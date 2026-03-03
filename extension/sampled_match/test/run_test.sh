#!/bin/bash
# Test script for subgraph matching
# 
# Usage:
#   ./run_test.sh              # Run standalone test
#   ./run_test.sh --server     # Start server for HTTP queries

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build"
DATA_DIR="${PROJECT_ROOT}/sampling/dataset/small"
PATTERN_FILE="${PROJECT_ROOT}/sampling/pattern/pattern_with_constraints.json"
HTTP_PORT=10000

print_header() {
    echo "============================================"
    echo "  $1"
    echo "============================================"
}

print_header "Subgraph Matching Test Script"
echo "Project root: ${PROJECT_ROOT}"
echo "Build directory: ${BUILD_DIR}"
echo "Data directory: ${DATA_DIR}"
echo "Pattern file: ${PATTERN_FILE}"
echo ""

# Parse arguments
RUN_SERVER=false
if [ "$1" == "--server" ]; then
    RUN_SERVER=true
fi

# Step 1: Check build directory
if [ ! -d "${BUILD_DIR}" ]; then
    echo "Build directory does not exist. Creating..."
    mkdir -p "${BUILD_DIR}"
fi

# Step 2: Build the project
print_header "Step 1: Building the project"
cd "${BUILD_DIR}"

# Configure if needed
if [ ! -f "CMakeCache.txt" ]; then
    echo "Configuring CMake..."
    cmake .. -DCMAKE_BUILD_TYPE=Release \
             -DBUILD_TEST=ON \
             -DBUILD_EXTENSIONS="sampled_match"
fi

# Build
echo "Building..."
if [ "$RUN_SERVER" = true ]; then
    make -j$(nproc) rt_server 2>&1 | tail -10
else
    make -j$(nproc) test_sample_match 2>&1 | tail -10
fi

echo ""
echo "Build completed."
echo ""

# Step 3: Check data
print_header "Step 2: Checking data"
if [ ! -f "${DATA_DIR}/graph.yaml" ]; then
    echo "ERROR: Data directory does not contain graph.yaml"
    echo "Please ensure the data is prepared at: ${DATA_DIR}"
    exit 1
fi

echo "Data found at: ${DATA_DIR}"
ls -la "${DATA_DIR}"/*.csv 2>/dev/null | head -5
echo ""

# Step 4: Run test or server
if [ "$RUN_SERVER" = true ]; then
    # Server mode
    print_header "Step 3: Starting NeuG Server"
    echo "Server will listen on port ${HTTP_PORT}"
    echo ""
    echo "To send a query, use:"
    echo "  curl -X POST http://localhost:${HTTP_PORT}/cypher \\"
    echo "    -H 'Content-Type: application/json' \\"
    echo "    -d '{\"query\": \"CALL SAMPLED_MATCH(\\\"${PATTERN_FILE}\\\")\", \"access_mode\": \"read\"}'"
    echo ""
    echo "Press Ctrl+C to stop the server."
    echo ""
    
    SERVER_EXECUTABLE="${BUILD_DIR}/bin/rt_server"
    if [ ! -f "${SERVER_EXECUTABLE}" ]; then
        echo "ERROR: Server executable not found at: ${SERVER_EXECUTABLE}"
        exit 1
    fi
    
    "${SERVER_EXECUTABLE}" \
        --data-path "${DATA_DIR}" \
        --http-port ${HTTP_PORT} \
        --memory-level 1
else
    # Standalone test mode
    print_header "Step 3: Running Standalone Test"
    
    TEST_EXECUTABLE="${BUILD_DIR}/extension/sample/test/test_sample_match"
    if [ ! -f "${TEST_EXECUTABLE}" ]; then
        echo "ERROR: Test executable not found at: ${TEST_EXECUTABLE}"
        echo "Please build the project first."
        exit 1
    fi
    
    echo "Running: ${TEST_EXECUTABLE} ${DATA_DIR} ${PATTERN_FILE}"
    echo ""
    
    "${TEST_EXECUTABLE}" "${DATA_DIR}" "${PATTERN_FILE}"
    
    echo ""
    print_header "Test completed successfully!"
fi
