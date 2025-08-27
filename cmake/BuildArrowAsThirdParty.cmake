#!/bin/bash
# Copyright 2020 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Reference: https://github.com/apache/iceberg-cpp/blob/main/cmake_modules/IcebergThirdpartyToolchain.cmake


include(FetchContent)

set(FC_DECLARE_COMMON_OPTIONS)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
    list(APPEND FC_DECLARE_COMMON_OPTIONS EXCLUDE_FROM_ALL TRUE)
endif()

set(ARROW_VERSION "18.0.0")
set(ARROW_SOURCE_URL "https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.tar.gz")

function(build_arrow_as_third_party)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)

    set(ARROW_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_STATIC ON CACHE BOOL "" FORCE)
    # Work around undefined symbol: arrow::ipc::ReadSchema(arrow::io::InputStream*, arrow::ipc::DictionaryMemo*)
    set(ARROW_WITH_UTF8PROC OFF CACHE BOOL "" FORCE)
    set(ARROW_CSV ON CACHE BOOL "" FORCE)
    set(ARROW_DATASET OFF CACHE BOOL "" FORCE)
    set(ARROW_COMPUTE ON CACHE BOOL "" FORCE)
    set(ARROW_CUDA OFF CACHE BOOL "" FORCE)
    set(ARROW_FILESYSTEM ON CACHE BOOL "" FORCE)
    set(ARROW_FLIGHT OFF CACHE BOOL "" FORCE)
    set(ARROW_GANDIVA OFF CACHE BOOL "" FORCE)
    set(ARROW_HDFS OFF CACHE BOOL "" FORCE)
    set(ARROW_ORC OFF CACHE BOOL "" FORCE)
    set(ARROW_JSON OFF CACHE BOOL "" FORCE)
    set(ARROW_PARQUET OFF CACHE BOOL "" FORCE)
    set(ARROW_PLASMA OFF CACHE BOOL "" FORCE)
    set(ARROW_PYTHON OFF CACHE BOOL "" FORCE)
    set(ARROW_S3 OFF CACHE BOOL "" FORCE)
    set(ARROW_WITH_BZ2 OFF CACHE BOOL "" FORCE)
    set(ARROW_WITH_LZ4 OFF CACHE BOOL "" FORCE)
    set(ARROW_WITH_SNAPPY OFF CACHE BOOL "" FORCE)
    set(ARROW_WITH_ZSTD OFF CACHE BOOL "" FORCE)
    set(ARROW_WITH_BROTLI OFF CACHE BOOL "" FORCE)
    set(ARROW_IPC ON CACHE BOOL "" FORCE)
    set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_INTEGRATION OFF CACHE BOOL "" FORCE)
    set(ARROW_ENABLE_TIMING_TESTS OFF CACHE BOOL "" FORCE)
    set(ARROW_FUZZING OFF CACHE BOOL "" FORCE)
    set(ARROW_USE_ASAN OFF CACHE BOOL "" FORCE)
    set(ARROW_USE_UBSAN OFF CACHE BOOL "" FORCE)
    set(ARROW_USE_TSAN OFF CACHE BOOL "" FORCE)
    set(ARROW_USE_JEMALLOC OFF CACHE BOOL "" FORCE)
    set(ARROW_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)
    set(ARROW_RUNTIME_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)
    set(ARROW_POSITION_INDEPENDENT_CODE ON CACHE BOOL "" FORCE)
    set(ARROW_DEPENDENCY_SOURCE "BUNDLED" CACHE STRING "" FORCE)
    set(ARROW_WITH_ZLIB OFF CACHE BOOL "" FORCE)

    fetchcontent_declare(Arrow
        ${FC_DECLARE_COMMON_OPTIONS}
        URL ${ARROW_SOURCE_URL}
        SOURCE_SUBDIR
        cpp
        FIND_PACKAGE_ARGS
        NAMES
        Arrow
        CONFIG)

    fetchcontent_makeavailable(Arrow)

    if(arrow_SOURCE_DIR)
        if(TARGET Arrow::arrow_static)
            message(STATUS "Arrow::arrow_static found")
            set(ARROW_LIB Arrow::arrow_static
                PARENT_SCOPE)
        elseif(TARGET arrow_static)
            message(STATUS "arrow_static found")
            set(ARROW_LIB arrow_static
                PARENT_SCOPE)
        else()
            message(FATAL_ERROR "Arrow::arrow_static or arrow_static target not found.")
        endif()

        message(STATUS "Arrow source directory found: ${arrow_SOURCE_DIR}")
        include_directories(SYSTEM ${arrow_SOURCE_DIR}/cpp/src
            ${arrow_BINARY_DIR}/src)


        if(TARGET arrow_bundled_dependencies)
            message(STATUS "arrow_bundled_dependencies found")
            # arrow_bundled_dependencies is only INSTALL_INTERFACE and will not be built by default.
            # We need to add it as a dependency to arrow_static so that it will be built.
            add_dependencies(arrow_static arrow_bundled_dependencies)
            # We cannot install an IMPORTED target, so we need to install the library manually.
            get_target_property(arrow_bundled_dependencies_location arrow_bundled_dependencies
                IMPORTED_LOCATION)
            install(FILES ${arrow_bundled_dependencies_location}
                DESTINATION lib)
        endif()
    else()
        # list(APPEND ICEBERG_SYSTEM_DEPENDENCIES Arrow)
        message(FATAL_ERROR "Arrow source directory not found. Please check the Arrow version and source URL.")
    endif()
    install_neug_target(ARROW_LIB)

endfunction()
