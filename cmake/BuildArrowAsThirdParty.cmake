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
set(ARROW_SOURCE_URL "https://graphscope.oss-cn-beijing.aliyuncs.com/apache-arrow-${ARROW_VERSION}.tar.gz")

function(build_arrow_as_third_party)
    check_cxx_compiler_flag("-Wno-sign-compare" COMPILER_SUPPORTS_SIGN_COMPARE_FLAG)
    check_cxx_compiler_flag("-Wno-deprecated-declarations" COMPILER_SUPPORTS_DEPRECATED_DECLARATIONS_FLAG)
    check_cxx_compiler_flag("-Wno-attributes" COMPILER_SUPPORTS_ATTRIBUTES_FLAG)
    check_cxx_compiler_flag("-Wno-error=stringop-overflow" COMPILER_SUPPORTS_STRINGOP_OVERFLOW_FLAG)
    check_cxx_compiler_flag("-Wno-array-bounds" COMPILER_SUPPORTS_ARRAY_BOUNDS_FLAG)
    check_cxx_compiler_flag("-Wno-error=uninitialized" COMPILER_SUPPORTS_NO_ERROR_UNINITIALIZED_FLAG)
    if (COMPILER_SUPPORTS_SIGN_COMPARE_FLAG)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare")
    endif()
    if (COMPILER_SUPPORTS_DEPRECATED_DECLARATIONS_FLAG)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
    endif()
    if (COMPILER_SUPPORTS_ATTRIBUTES_FLAG)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-attributes")
    endif()
    if (COMPILER_SUPPORTS_STRINGOP_OVERFLOW_FLAG)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=stringop-overflow")
    endif()
    if (COMPILER_SUPPORTS_ARRAY_BOUNDS_FLAG)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-array-bounds")
    endif()
    if (COMPILER_SUPPORTS_NO_ERROR_UNINITIALIZED_FLAG)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=uninitialized")
    endif()
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)

    set(ARROW_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_STATIC ON CACHE BOOL "" FORCE)
    # Work around undefined symbol: arrow::ipc::ReadSchema(arrow::io::InputStream*, arrow::ipc::DictionaryMemo*)
    set(ARROW_WITH_UTF8PROC OFF CACHE BOOL "" FORCE)
    set(ARROW_CSV ON CACHE BOOL "" FORCE)
    set(ARROW_ACERO ON CACHE BOOL "" FORCE)
    set(ARROW_DATASET ON CACHE BOOL "" FORCE)
    set(ARROW_COMPUTE ON CACHE BOOL "" FORCE)
    set(ARROW_CUDA OFF CACHE BOOL "" FORCE)
    set(ARROW_FILESYSTEM ON CACHE BOOL "" FORCE)
    set(ARROW_FLIGHT OFF CACHE BOOL "" FORCE)
    set(ARROW_GANDIVA OFF CACHE BOOL "" FORCE)
    set(ARROW_HDFS OFF CACHE BOOL "" FORCE)
    set(ARROW_ORC OFF CACHE BOOL "" FORCE)
    # ARROW_JSON is set by the main CMakeLists.txt if json extension is enabled
    # Only set it to ON if not already set
    if(NOT DEFINED ARROW_JSON)
        set(ARROW_JSON OFF CACHE BOOL "" FORCE)
    endif()
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
    # Use system RapidJSON instead of building it (project already has rapidjson in third_party)
    # RapidJSON configuration is set by the main CMakeLists.txt if json extension is enabled
    if(ARROW_JSON AND NOT DEFINED RapidJSON_SOURCE)
        set(RapidJSON_SOURCE "SYSTEM" CACHE STRING "" FORCE)
        # Point Arrow to use the project's RapidJSON
        set(RapidJSON_ROOT "${CMAKE_SOURCE_DIR}/third_party/rapidjson" CACHE PATH "" FORCE)
    endif()
    set(ARROW_WITH_ZLIB OFF CACHE BOOL "" FORCE)
    set(ARROW_ENABLE_THREADING ON CACHE BOOL "" FORCE)

    # Save original flags and set flags to suppress warnings for Arrow build
    # set(ORIGINAL_CXX_FLAGS "${CMAKE_CXX_FLAGS}" CACHE STRING "" FORCE)
    # set(CMAKE_CXX_FLAGS "${ORIGINAL_CXX_FLAGS} " CACHE STRING "" FORCE)

    # Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
	if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
		cmake_policy(SET CMP0135 NEW)
	endif()

    message(STATUS "Fetching Arrow ${ARROW_VERSION} from ${ARROW_SOURCE_URL}")

    fetchcontent_declare(Arrow
        ${FC_DECLARE_COMMON_OPTIONS}
        URL ${ARROW_SOURCE_URL}
        SOURCE_SUBDIR
        cpp)

    FetchContent_GetProperties(Arrow)
    if(NOT arrow_POPULATED)
        FetchContent_Populate(Arrow)
        add_subdirectory(${arrow_SOURCE_DIR}/cpp ${arrow_BINARY_DIR} EXCLUDE_FROM_ALL)
    endif()

    if(ARROW_SOURCE_URL)
        # Try different possible Arrow target names
        if(TARGET Arrow::arrow_static)
            message(STATUS "Found Arrow::arrow_static target")
            set(ARROW_LIB Arrow::arrow_static)
            set(ARROW_LIB ${ARROW_LIB} PARENT_SCOPE)
        elseif(TARGET arrow_static)
            message(STATUS "Found arrow_static target")
            set(ARROW_LIB arrow_static)
            set(ARROW_LIB ${ARROW_LIB} PARENT_SCOPE)
        elseif(TARGET Arrow::arrow)
            message(STATUS "Found Arrow::arrow target (using as fallback)")
            set(ARROW_LIB Arrow::arrow)
            set(ARROW_LIB ${ARROW_LIB} PARENT_SCOPE)
        elseif(TARGET arrow)
            message(STATUS "Found arrow target (using as fallback)")
            set(ARROW_LIB arrow)
            set(ARROW_LIB ${ARROW_LIB} PARENT_SCOPE)
        elseif(TARGET arrow_shared)
            message(WARNING "Only found arrow_shared target, but we prefer static linking")
            set(ARROW_LIB arrow_shared)
            set(ARROW_LIB ${ARROW_LIB} PARENT_SCOPE)
        else()
            # List Arrow-related targets for debugging
            message(STATUS "Searching for Arrow targets...")
            get_property(all_targets DIRECTORY ${arrow_SOURCE_DIR} PROPERTY BUILDSYSTEM_TARGETS)
            foreach(target ${all_targets})
                if(target MATCHES "arrow")
                    message(STATUS "Available Arrow target: ${target}")
                endif()
            endforeach()
            message(FATAL_ERROR "No suitable Arrow target found. Expected Arrow::arrow_static or arrow_static.")
        endif()

        # Try different possible Arrow Acero target names
        if(TARGET Arrow::acero_static)
            message(STATUS "Found Arrow::acero_static target")
            set(ARROW_ACERO_LIB Arrow::acero_static)
        elseif(TARGET arrow_acero_static)
            message(STATUS "Found arrow_acero_static target")
            set(ARROW_ACERO_LIB arrow_acero_static)
        elseif(TARGET Arrow::acero)
            message(STATUS "Found Arrow::acero target (using as fallback)")
            set(ARROW_ACERO_LIB Arrow::acero)
        elseif(TARGET arrow_acero)
            message(STATUS "Found arrow_acero target (using as fallback)")
            set(ARROW_ACERO_LIB arrow_acero)
        else()
            message(WARNING "Arrow acero target not found. Acero symbols may be unresolved.")
            set(ARROW_ACERO_LIB "")
        endif()

        if(ARROW_ACERO_LIB)
            list(APPEND ARROW_LIB ${ARROW_ACERO_LIB})
            set(ARROW_LIB ${ARROW_LIB} PARENT_SCOPE)
        endif()

        # Try different possible Arrow Dataset target names
        if(TARGET Arrow::dataset_static)
            message(STATUS "Found Arrow::dataset_static target")
            set(ARROW_DATASET_LIB Arrow::dataset_static)
        elseif(TARGET arrow_dataset_static)
            message(STATUS "Found arrow_dataset_static target")
            set(ARROW_DATASET_LIB arrow_dataset_static)
        elseif(TARGET Arrow::dataset)
            message(STATUS "Found Arrow::dataset target (using as fallback)")
            set(ARROW_DATASET_LIB Arrow::dataset)
        elseif(TARGET arrow_dataset)
            message(STATUS "Found arrow_dataset target (using as fallback)")
            set(ARROW_DATASET_LIB arrow_dataset)
        else()
            message(WARNING "Arrow dataset target not found. Dataset symbols may be unresolved.")
            set(ARROW_DATASET_LIB "")
        endif()

        if(ARROW_DATASET_LIB) 
            list(APPEND ARROW_LIB ${ARROW_DATASET_LIB})
            set(ARROW_LIB ${ARROW_LIB} PARENT_SCOPE)
        endif()

        message(STATUS "Arrow source directory found: ${arrow_SOURCE_DIR}")
        include_directories(SYSTEM ${arrow_SOURCE_DIR}/cpp/src
            ${arrow_BINARY_DIR}/src)

        # Set additional Arrow variables for compatibility
        set(ARROW_FOUND TRUE PARENT_SCOPE)
        set(ARROW_LIBRARIES ${ARROW_LIB} PARENT_SCOPE)
        set(ARROW_INCLUDE_DIRS ${arrow_SOURCE_DIR}/cpp/src ${arrow_BINARY_DIR}/src PARENT_SCOPE)
        set(ARROW_SOURCE_DIR ${arrow_SOURCE_DIR} PARENT_SCOPE)
        set(ARROW_BINARY_DIR ${arrow_BINARY_DIR} PARENT_SCOPE)

        # Handle bundled dependencies if they exist
        if(TARGET arrow_bundled_dependencies)
            message(STATUS "arrow_bundled_dependencies found")
            # Check if we have a static target to add dependencies to
            if(TARGET arrow_static)
                add_dependencies(arrow_static arrow_bundled_dependencies)
            elseif(TARGET Arrow::arrow_static)
                # Note: Cannot add dependencies to imported targets
                message(STATUS "Arrow::arrow_static is imported, cannot add dependencies")
            endif()
            
            # Try to get the location and install if it's a real file
            get_target_property(arrow_bundled_dependencies_location arrow_bundled_dependencies IMPORTED_LOCATION)
            if(arrow_bundled_dependencies_location AND EXISTS ${arrow_bundled_dependencies_location})
                install(FILES ${arrow_bundled_dependencies_location} DESTINATION lib)
            endif()
        endif()

        # install the headers of arrow to system
        install(DIRECTORY ${arrow_SOURCE_DIR}/cpp/src/arrow
            DESTINATION include
            FILES_MATCHING PATTERN "*.h"
            PATTERN "test" EXCLUDE
            PATTERN "testing" EXCLUDE)

        install(DIRECTORY ${arrow_BINARY_DIR}/src/arrow
            DESTINATION include
            FILES_MATCHING PATTERN "*.h"
            PATTERN "test" EXCLUDE
            PATTERN "testing" EXCLUDE)
        
    else()
        # list(APPEND ICEBERG_SYSTEM_DEPENDENCIES Arrow)
        message(FATAL_ERROR "Arrow source directory not found. Please check the Arrow version and source URL.")
    endif()

endfunction()
