cmake_minimum_required (VERSION 3.10)

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/NEUG_VERSION NEUG_VERSION)
# Strip trailing newline
string(REGEX REPLACE "\n$" "" NEUG_VERSION "${NEUG_VERSION}")

# Parse version components (allowing optional suffixes like +build)
string(REGEX MATCH "^([0-9]+)\.([0-9]+)\.([0-9]+)" _ "${NEUG_VERSION}")
if(NOT CMAKE_MATCH_COUNT)
    message(FATAL_ERROR "NEUG_VERSION must start with <major>.<minor>.<patch>")
endif()
set(NEUG_VERSION_MAJOR ${CMAKE_MATCH_1})
set(NEUG_VERSION_MINOR ${CMAKE_MATCH_2})
set(NEUG_VERSION_PATCH ${CMAKE_MATCH_3})
set(NEUG_VERSION_SEMVER "${NEUG_VERSION_MAJOR}.${NEUG_VERSION_MINOR}.${NEUG_VERSION_PATCH}")

message(STATUS "NeuG Version: ${NEUG_VERSION} (${NEUG_VERSION_SEMVER})")

# Generate version header
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/include/neug/version.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/include/neug/version.h
  @ONLY
)

# Add the generated header to include directories
include_directories(${CMAKE_CURRENT_BINARY_DIR}/include)

project (
  NeuG
    VERSION ${NEUG_VERSION_SEMVER}
  LANGUAGES C CXX)

option(BUILD_TEST "Whether to build test" OFF)
option(BUILD_DOC "Whether to build doc" OFF)
option(OPTIMIZE_FOR_HOST "Whether to optimize on host" ON) # Whether to build optimized code on host
option(BUILD_EXECUTABLES "When to build executors" OFF) # Whether to build executables, default is OFF
option(BUILD_HTTP_SERVER "Whether to build http server" OFF) # Whether to build http server, default is OFF
option(WITH_MIMALLOC "Whether to use mimalloc" ON) # Whether to use mimalloc, default is ON

# Options to control the building of tools
option(BUILD_PYTHON "Whether to build python embedded" ON)
option(BUILD_COMPILER "Whether to build compiler" ON)
#---------------------------------------------------------------------------------
#-------------------------------Compiler related options-------------------------------
option(NEUG_DEFAULT_REL_STORAGE_DIRECTION "Only store fwd direction in rel tables by default." BOTH)
if(NOT NEUG_DEFAULT_REL_STORAGE_DIRECTION)
    set(NEUG_DEFAULT_REL_STORAGE_DIRECTION BOTH)
endif()
option(NEUG_PAGE_SIZE_LOG2 "Log2 of the page size." 12)
if(NOT NEUG_PAGE_SIZE_LOG2)
    set(NEUG_PAGE_SIZE_LOG2 12)
endif()
message(STATUS "NEUG_PAGE_SIZE_LOG2: ${NEUG_PAGE_SIZE_LOG2}")
option(NEUG_VECTOR_CAPACITY_LOG2 "Log2 of the vector capacity." 11)
if(NOT NEUG_VECTOR_CAPACITY_LOG2)
    set(NEUG_VECTOR_CAPACITY_LOG2 11)
endif()
message(STATUS "NEUG_VECTOR_CAPACITY_LOG2: ${NEUG_VECTOR_CAPACITY_LOG2}")
set(CMAKE_POLICY_VERSION_MINIMUM "3.5" CACHE STRING "Minimum CMake version to let third-party libraries use")

# 64 * 2048 nodes per group
option(NEUG_NODE_GROUP_SIZE_LOG2 "Log2 of the vector capacity." 17)
if(NOT NEUG_NODE_GROUP_SIZE_LOG2)
    set(NEUG_NODE_GROUP_SIZE_LOG2 17)
endif()
message(STATUS "NEUG_NODE_GROUP_SIZE_LOG2: ${NEUG_NODE_GROUP_SIZE_LOG2}")
option(ENABLE_WERROR "Treat all warnings as errors" FALSE)
option(ENABLE_ADDRESS_SANITIZER "Enable address sanitizer." FALSE)
option(ENABLE_THREAD_SANITIZER "Enable thread sanitizer." FALSE)
option(ENABLE_UBSAN "Enable undefined behavior sanitizer." FALSE)
option(ENABLE_RUNTIME_CHECKS "Enable runtime coherency checks (e.g. asserts)" FALSE)
option(ENABLE_LTO "Enable Link-Time Optimization" FALSE)
option(AUTO_UPDATE_GRAMMAR "Automatically regenerate C++ grammar files on change." FALSE)
option(BUILD_EXTENSIONS "Semicolon-separated list of extensions to build." "")
option(BUILD_SINGLE_FILE_HEADER "Build single file header. Requires Python >= 3.9." FALSE)
option(ENABLE_BACKTRACES "Enable backtrace printing for exceptions and segfaults" FALSE)
option(BUILD_EXAMPLES "Build examples." ON)
option(ENABLE_GCOV "Enable gcov (debug mode only)" FALSE)
option(ENABLE_PROTOCOLS "Which protocols to enable, separated by semicolon, default is http. Or set it to all" "http")

# split ENABLE_PROTOCOLS into a list, or check if is all
if(NOT ENABLE_PROTOCOLS OR ENABLE_PROTOCOLS STREQUAL "OFF" OR ENABLE_PROTOCOLS STREQUAL "")
    set(ENABLE_PROTOCOLS "http")
    message(STATUS "ENABLE_PROTOCOLS not set or OFF, defaulting to: http")
endif()

string(REPLACE ";" " " ENABLE_PROTOCOLS_LIST "${ENABLE_PROTOCOLS}")
message(STATUS "Enabled protocols: ${ENABLE_PROTOCOLS_LIST}")
foreach(protocol IN LISTS ENABLE_PROTOCOLS)
    string(TOLOWER "${protocol}" protocol)
    if(protocol STREQUAL "http")
        message(STATUS "Enabling HTTP protocol")
        add_definitions(-DENABLE_HTTP_PROTOCOL)
    elseif(protocol STREQUAL "all")
        message(STATUS "Enabling all protocols")
        add_definitions(-DENABLE_HTTP_PROTOCOL)
        break()
    else()
        message(WARNING "Unknown protocol: ${protocol}")
    endif()
endforeach()

add_definitions(-DNEUG_CMAKE_VERSION="${CMAKE_PROJECT_VERSION}")

include(CheckCXXCompilerFlag)


set(DEFAULT_BUILD_TYPE "Release")
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE
      STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
               "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif ()

add_compile_definitions(NEUG_VERSION="${NEUG_VERSION}")

# reference: https://gitlab.kitware.com/cmake/community/-/wikis/doc/cmake/RPATH-handling#always-full-rpath
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
# set(CMAKE_CXX_VISIBILITY_PRESET hidden)
# set(CMAKE_C_VISIBILITY_PRESET hidden)
# set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
# set(CMAKE_FIND_PACKAGE_RESOLVE_SYMLINKS TRUE)
# set(CMAKE_POSITION_INDEPENDENT_CODE ON)
# set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
# On Linux, symbols in executables are not accessible by loaded shared libraries (e.g. via dlopen(3)). However, we need to export public symbols in executables so that extensions can access public symbols. This enables that behaviour.
set(CMAKE_ENABLE_EXPORTS TRUE)

if(ENABLE_WERROR)
    if (CMAKE_VERSION VERSION_GREATER "3.24.0" OR CMAKE_VERSION VERSION_EQUAL "3.24.0")
        set(CMAKE_COMPILE_WARNING_AS_ERROR TRUE)
    elseif (MSVC)
        add_compile_options(\WX)
    else ()
        add_compile_options(-Werror)
    endif()
endif()

if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
else ()
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lopen-pal")
    else ()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,-rpath,$ORIGIN")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    endif ()
endif ()
# Only add -fopenmp if not using clang
if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(STATUS "Using non-clang compiler, adding -fopenmp")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fopenmp")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fopenmp")
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -fPIC -Wno-psabi")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
check_cxx_compiler_flag("-std=c++20" COMPILER_SUPPORTS_CXX20)
if (NOT COMPILER_SUPPORTS_CXX20)
    check_cxx_compiler_flag("-std=c++2a" COMPILER_SUPPORTS_CXX2A)
    if (COMPILER_SUPPORTS_CXX2A)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++2a")
    else()
        message(WARNING "Compiler does not support C++20 or C++2a")
    endif()
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20")
endif()

include(CheckCXXSourceCompiles)
check_cxx_source_compiles("
#include <chrono>
int main() {
    std::chrono::year_month_day ymd{std::chrono::year{2024}, std::chrono::month{6}, std::chrono::day{9}};
    return 0;
}
" HAVE_CHRONO_YMD)

if (HAVE_CHRONO_YMD)
    add_definitions(-DENABLE_CHRONO_YMD)
endif()

# -Wno-psabi for no warning about returning a pair in c++ function
if (OPTIMIZE_FOR_HOST AND NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
#if compiler is gcc
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    # -Wno-missing-requires: protobuf v29.6 headers use C++20 concepts in port.h
    # that trigger this warning on GCC 13+, which becomes an error with -Werror.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-memaccess -Wno-maybe-uninitialized -Wno-missing-requires -Wno-ignored-attributes")
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-uninitialized")
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-nonnull ") # brought by arrow
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-Wno-restrict) # no restrict until https://gcc.gnu.org/bugzilla/show_bug.cgi?id=105651 is fixed
endif()


#------------------------Brought by compiler------------------------------------
if(${ENABLE_THREAD_SANITIZER})
    if(MSVC)
        message(FATAL_ERROR "Thread sanitizer is not supported on MSVC")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fno-omit-frame-pointer")
    endif()
endif()
if(${ENABLE_ADDRESS_SANITIZER})
    if(MSVC)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /fsanitize=address")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    endif()
endif()
if(${ENABLE_UBSAN})
    if(MSVC)
        message(FATAL_ERROR "Undefined behavior sanitizer is not supported on MSVC")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-omit-frame-pointer")
    endif()
endif()

if(${ENABLE_LTO})
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# if os is macos, 
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message("macos detected")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-conversion -fclang-abi-compat=14") # need fclang-abi-compat for clang compiler, due to absl problem: https://github.com/protocolbuffers/protobuf/issues/12693
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated")
check_cxx_compiler_flag("-Wno-sign-compare" COMPILER_SUPPORTS_SIGN_COMPARE_FLAG)
check_cxx_compiler_flag("-Wno-deprecated-declarations" COMPILER_SUPPORTS_DEPRECATED_DECLARATIONS_FLAG)
check_cxx_compiler_flag("-Wno-attributes" COMPILER_SUPPORTS_ATTRIBUTES_FLAG)
check_cxx_compiler_flag("-Wno-error=stringop-overflow" COMPILER_SUPPORTS_STRINGOP_OVERFLOW_FLAG)
check_cxx_compiler_flag("-Wno-array-bounds" COMPILER_SUPPORTS_ARRAY_BOUNDS_FLAG)
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

macro(install_neug_target target)
    if (NOT TARGET ${target})
        message(WARNING "Target ${target} does not exist, cannot be installed.")
        return()
    endif()

    get_target_property(actual_target ${target} ALIASED_TARGET)
    if(actual_target)
        if(NOT TARGET ${actual_target})
            message(WARNING "Aliased target ${target} points to ${actual_target}, which does not exist. Cannot be installed.")
            return()
        endif()
        set(_neug_install_target ${actual_target})
    else()
        set(_neug_install_target ${target})
    endif()

    get_target_property(_neug_public_headers ${_neug_install_target} PUBLIC_HEADER)
    get_target_property(_neug_target_source_dir ${_neug_install_target} SOURCE_DIR)
    if(_neug_public_headers AND NOT _neug_public_headers STREQUAL "_neug_public_headers-NOTFOUND" AND NOT _neug_public_headers STREQUAL "NOTFOUND")
        set(_neug_public_header_abs)
        foreach(_hdr IN LISTS _neug_public_headers)
            if(NOT IS_ABSOLUTE "${_hdr}" AND _neug_target_source_dir)
                list(APPEND _neug_public_header_abs "${_neug_target_source_dir}/${_hdr}")
            else()
                list(APPEND _neug_public_header_abs "${_hdr}")
            endif()
        endforeach()
        # Update the target property so install(TARGETS) can rely on it
        set_target_properties(${_neug_install_target} PROPERTIES PUBLIC_HEADER "${_neug_public_header_abs}")
        set(_neug_public_header_args PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    else()
        set(_neug_public_header_args)
    endif()

    install(TARGETS ${_neug_install_target}
        EXPORT neug-targets
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ${_neug_public_header_args}
    )
endmacro()

if (BUILD_HTTP_SERVER)
    add_compile_definitions(BUILD_HTTP_SERVER)
endif()

if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/yaml-cpp/CMakeLists.txt")
    execute_process(
        COMMAND git submodule update --init
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        RESULT_VARIABLE result
    )
    if(result)
        message(FATAL_ERROR "Failed to update git submodule: ${result}")
    endif()
endif()

if (WITH_MIMALLOC)
    set(MI_BUILD_TESTS OFF CACHE BOOL "Build mimalloc tests")
    set(MI_BUILD_SHARED ON CACHE BOOL "Build mimalloc as shared library" FORCE)
    set(MI_BUILD_STATIC OFF CACHE BOOL "Build mimalloc as static library")
    # Keep MI_OVERRIDE=ON to let it take effect.
    set(MI_OVERRIDE ON CACHE BOOL "Override malloc with mimalloc" FORCE)
    set(MI_USE_LIBATOMIC OFF CACHE BOOL "Use libatomic")
    # we should not use static mimalloc, the reason is that static mimalloc will override the default malloc.
    add_subdirectory(third_party/mimalloc)
    set_target_properties(mimalloc PROPERTIES DEBUG_POSTFIX "")
    include_directories(SYSTEM third_party/mimalloc/include)
    unset(PACKAGE_VERSION) # Unset PACKAGE_VERSION to avoid generate two PACKAGE_VERSION in config.h for libdwarf-lite.
endif()

add_subdirectory(third_party/fast_float)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/fast_float/include)

include(cmake/BuildAbseilThirdParty.cmake)
build_abseil_as_third_party()

if(BUILD_TEST)
    enable_testing()
    # Always use the bundled googletest to avoid discrepancies in system packages.
    add_subdirectory(third_party/gtest EXCLUDE_FROM_ALL)
    add_library(neug::GTest INTERFACE IMPORTED)
    target_link_libraries(neug::GTest INTERFACE gtest_main)
    target_link_libraries(neug::GTest INTERFACE gmock_main)
endif()

if (ENABLE_BACKTRACES)
    find_package(cpptrace)
    if (NOT cpptrace_FOUND)
        include(FetchContent)
        FetchContent_Declare(
          cpptrace
          GIT_REPOSITORY https://github.com/jeremy-rifkin/cpptrace.git
          GIT_TAG        v1.0.2
          GIT_SHALLOW    TRUE
        )
        FetchContent_MakeAvailable(cpptrace)
        include_directories(SYSTEM ${cpptrace_SOURCE_DIR}/include)
    endif()
    add_compile_definitions(NEUG_BACKTRACE)
endif()

#----------------------------------------------------------------------------------
# Detect OS and architecture, copied from DuckDB
set(OS_NAME "unknown")
set(OS_ARCH "amd64")

string(REGEX MATCH "(arm64|aarch64)" IS_ARM "${CMAKE_SYSTEM_PROCESSOR}")
if(IS_ARM)
  set(OS_ARCH "arm64")
elseif(FORCE_32_BIT)
  set(OS_ARCH "i386")
endif()

if(APPLE)
  set(OS_NAME "osx")
endif()
if(WIN32)
  set(OS_NAME "windows")
endif()
if(UNIX AND NOT APPLE)
  set(OS_NAME "linux") # sorry BSD
endif()

if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    message(STATUS "64-bit architecture detected")
    add_compile_definitions(__64BIT__)
elseif(CMAKE_SIZEOF_VOID_P EQUAL 4)
    message(STATUS "32-bit architecture detected")
    add_compile_definitions(__32BIT__)
    set(__32BIT__ TRUE)
endif()


#print options
message(STATUS "Build test: ${BUILD_TEST}")
message(STATUS "Build doc: ${BUILD_DOC}")

# ------------------------------------------------------------------------------
# cmake configs
# ------------------------------------------------------------------------------

include(CheckLibraryExists)
include(GNUInstallDirs)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include(CheckCXXSourceCompiles)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include/)
# pb files generated under src/utils
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src/utils)
# system_config.h generated under src/compiler
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src/compiler)

# Print cc and cxx compiler
message(STATUS "CMAKE_C_COMPILER: ${CMAKE_C_COMPILER}")
message(STATUS "CMAKE_CXX_COMPILER: ${CMAKE_CXX_COMPILER}")


function(add_neug_test TEST_NAME)
    find_package(OpenSSL REQUIRED)
    set(SRCS ${ARGN})
    add_executable(${TEST_NAME} ${SRCS})
    target_link_libraries(${TEST_NAME} PUBLIC neug::GTest neug   
        ${Protobuf_LIBRARIES} ${GLOG_LIBRARIES} ${ARROW_LIB} ${OPENSSL_LIBRARIES} 
        ${CRYPTO_LIB})
    if (ENABLE_GCOV)
        target_compile_options(${TEST_NAME} PRIVATE --coverage)
        target_link_options(${TEST_NAME} PRIVATE --coverage)
    endif()
    include(GoogleTest)
	add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
	gtest_add_tests(TARGET ${TEST_NAME} TEST_LIST ${TEST_NAME}_tests)
endfunction()


set(TEST_PROGRAM "
#define _GNU_SOURCE
#include <unistd.h>
#include <fcntl.h>

int main() {
  int fd_in = -1, fd_out = -1;
  struct stat stat_buf;
  copy_file_range(fd_in, NULL, fd_out, NULL, stat_buf.st_size, 0);
  return 0;
}
")

check_cxx_source_compiles("${TEST_PROGRAM}" HAVE_COPY_FILE_RANGE_COMPILES)

if(NOT HAVE_COPY_FILE_RANGE_COMPILES)
    message(WARNING "copy_file_range is not available or failed to compile!")
else()
    message(STATUS "copy_file_range compilation succeeded.")
endif()

if(HAVE_COPY_FILE_RANGE_COMPILES)
    add_definitions(-DUSE_COPY_FILE_RANGE)
endif()

find_package(Threads REQUIRED)
find_package(OpenSSL)
if (NOT OpenSSL_FOUND)
    message(FATAL_ERROR "Fail to find OpenSSL, try to install openssl development package. On macos: brew install openssl@3; on ubuntu: apt-get install libssl-dev")
endif()
include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})

if (BUILD_PYTHON)
    message("Build python embedded")
    # To build wheel in manylinux docker, we should not link to a specific python library.
    # Get environment variable PYBIND11_FINDPYTHON, if it is set to ON, we will set PYBIND11_FINDPYTHON to ON, otherwise, we will not set it, and let pybind11 find python by itself.
    set(PYBIND11_FINDPYTHON ON)
    if (DEFINED ENV{PYBIND11_FINDPYTHON}) 
        if ($ENV{PYBIND11_FINDPYTHON} STREQUAL "OFF")
            message(STATUS "Environment variable PYBIND11_FINDPYTHON is set to OFF")
            set(PYBIND11_FINDPYTHON OFF)
        elseif (NOT ( $ENV{PYBIND11_FINDPYTHON} STREQUAL "ON"))
            message(WARNING "Environment variable PYBIND11_FINDPYTHON is set to ${ENV{PYBIND11_FINDPYTHON}}, which is not ON or OFF, by default is ON")
        endif()
    else()
        message(STATUS "Environment variable PYBIND11_FINDPYTHON is not set, by default is ON")
    endif()
    add_subdirectory(third_party/pybind11)
endif()


if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()

include_directories(SYSTEM third_party/HowardHinnant/date/include)


include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)


# find Threads------------------------------------------------------------------
set(CMAKE_THREAD_PREFER_PTHREAD ON)
find_package(Threads REQUIRED)

# find gflags-------------------------------------------------------------------
# We need gflags for brpc, so we build it as a third party library
include(cmake/BuildGflagsAsThirdParty.cmake)
build_gflags_as_third_party()
include_directories(${GFLAGS_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/third_party/gflags/include)
message(STATUS "gflags libraries: ${GFLAGS_LIBRARIES}")
message(STATUS "gflags found: ${gflags_FOUND}")
message(STATUS "gflags dir: ${gflags_DIR}")

#find glog
include(cmake/BuildGlogAsThirdParty.cmake)
build_glog_as_third_party()
include_directories(${GLOG_INCLUDE_DIRS})
message(STATUS "glog libraries: ${GLOG_LIBRARIES}")

include(cmake/BuildYamlCppAsThirdParty.cmake)
build_yaml_cpp_as_third_party()
message(STATUS "yaml-cpp libraries: ${YAML_CPP_LIBRARIES}, include dir: ${YAML_CPP_INCLUDE_DIRS}")
include_directories(${YAML_CPP_INCLUDE_DIRS})

#find protobuf-------------------------------------------------------------------
include(cmake/BuildProtobufThirdParty.cmake)
build_protobuf_as_third_party()

include_directories(${Protobuf_INCLUDE_DIRS})
message(STATUS "protobuf include dir: ${Protobuf_INCLUDE_DIR}, ${Protobuf_INCLUDE_DIRS}")
message(STATUS "protobuf libraries: ${Protobuf_LIBRARIES}")

# Generate config header. Need to placed after protobuf find_package
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/include/neug/config.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/include/neug/config.h
  @ONLY
)


if (BUILD_HTTP_SERVER)
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build leveldb as static library" FORCE)
    set(LEVELDB_INSTALL OFF CACHE BOOL "Install leveldb")
    set(LEVELDB_BUILD_TESTS OFF CACHE BOOL "Build leveldb tests")
    set(LEVELDB_BUILD_BENCHMARKS OFF CACHE BOOL "Build leveldb benchmarks")
    # Apply third_party/leveldb.patch to leveldb to fix compilation issue for wunused-variable
    if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/leveldb.patch")
        message(FATAL_ERROR "leveldb.patch not found in third_party")
    endif()
    execute_process(COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/third_party/leveldb.patch
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/leveldb
                    RESULT_VARIABLE result
                    OUTPUT_VARIABLE output
                    ERROR_VARIABLE error_output)
    add_subdirectory(third_party/leveldb)
    target_compile_definitions(leveldb PRIVATE NDEBUG=1)
    set(LEVELDB_INCLUDE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/third_party/leveldb/include)
    set(LEVELDB_LIB leveldb)
    include_directories(${LEVELDB_INCLUDE_PATH})

    find_library(CRYPTO_LIB NAMES crypto)
    if (NOT CRYPTO_LIB)
        message(FATAL_ERROR "Fail to find crypto lib")
    endif()
    find_library(PROTOC_LIB name protoc)
    if (NOT PROTOC_LIB)
        message(FATAL_ERROR "Fail to find protoc lib")
    endif()
    message(STATUS "ProtoC lib: ${PROTOC_LIB}")

    if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
        include(cmake/BuildBRPCAsThirdParty.cmake)
        build_brpc_as_third_party(${APPLY_BRPC_PATCH})
    endif()
    include_directories(${BRPC_INCLUDE_PATH})
endif()

#find arrow----------------------------------------------------------------------

# Configure Arrow JSON support if json extension is enabled
if(BUILD_EXTENSIONS AND "json" IN_LIST BUILD_EXTENSIONS)
    set(ARROW_JSON ON CACHE BOOL "" FORCE)
    # Use system RapidJSON instead of building it (project already has rapidjson in third_party)
    set(RapidJSON_SOURCE "SYSTEM" CACHE STRING "" FORCE)
    # Point Arrow to use the project's RapidJSON
    set(RapidJSON_ROOT "${CMAKE_SOURCE_DIR}/third_party/rapidjson" CACHE PATH "" FORCE)
    message(STATUS "Arrow JSON support enabled for json extension")
endif()

# Configure Arrow Parquet support if parquet extension is enabled
if(BUILD_EXTENSIONS AND "parquet" IN_LIST BUILD_EXTENSIONS)
    set(ARROW_ENABLE_PARQUET ON CACHE BOOL "" FORCE)
    message(STATUS "Arrow Parquet support enabled for parquet extension")
endif()

set(NEUG_USE_SYSTEM_ARROW OFF)
if(DEFINED ENV{CI} AND "$ENV{CI}" STREQUAL "ON")
    set(NEUG_USE_SYSTEM_ARROW ON)
    message(STATUS "CI=ON detected – using system-installed Arrow")
endif()

if(NEUG_USE_SYSTEM_ARROW)
    find_package(Arrow CONFIG REQUIRED)
    set(ARROW_INCLUDE_DIRS ${ARROW_INCLUDE_DIR})
    macro(resolve_system_arrow_component component result_var)
        set(_component_result "")
        foreach(_candidate Arrow::${component}_static arrow_${component}_static Arrow::${component} arrow_${component})
            if (TARGET ${_candidate})
                set(_component_result ${_candidate})
                break()
            endif()
        endforeach()
        if (NOT _component_result AND _ARROW_LIB_DIR)
            if (_ARROW_TARGET_TYPE STREQUAL "STATIC_LIBRARY")
                set(_lib_prefix ${CMAKE_STATIC_LIBRARY_PREFIX})
                set(_lib_suffix ${CMAKE_STATIC_LIBRARY_SUFFIX})
            else()
                set(_lib_prefix ${CMAKE_SHARED_LIBRARY_PREFIX})
                set(_lib_suffix ${CMAKE_SHARED_LIBRARY_SUFFIX})
            endif()
            set(_lib_prefix "${_lib_prefix}")
            set(_lib_suffix "${_lib_suffix}")
            set(_candidate_file "${_lib_prefix}arrow_${component}${_lib_suffix}")
            set(_candidate_path "${_ARROW_LIB_DIR}/${_candidate_file}")
            if (EXISTS "${_candidate_path}")
                set(_component_result "${_candidate_path}")
            endif()
        endif()
        set(${result_var} ${_component_result})
    endmacro()

    set(_ARROW_BASE_TARGET "")
    if (TARGET Arrow::arrow_static)
        set(_ARROW_BASE_TARGET Arrow::arrow_static)
    elseif (TARGET arrow_static)
        set(_ARROW_BASE_TARGET arrow_static)
    elseif (TARGET Arrow::arrow_shared)
        set(_ARROW_BASE_TARGET Arrow::arrow_shared)
    elseif (TARGET arrow_shared)
        set(_ARROW_BASE_TARGET arrow_shared)
    endif()
    if (NOT _ARROW_BASE_TARGET)
        message(FATAL_ERROR "Failed to find Arrow library target.")
    endif()
    set(ARROW_LIB ${_ARROW_BASE_TARGET})

    set(_ARROW_LOCATION "")
    get_target_property(_ARROW_LOCATION ${_ARROW_BASE_TARGET} LOCATION)
    if (NOT _ARROW_LOCATION)
        get_target_property(_ARROW_CONFIGS ${_ARROW_BASE_TARGET} IMPORTED_CONFIGURATIONS)
        foreach(_cfg ${_ARROW_CONFIGS})
            string(TOUPPER "${_cfg}" _cfg_upper)
            get_target_property(_ARROW_LOCATION ${_ARROW_BASE_TARGET} IMPORTED_LOCATION_${_cfg_upper})
            if (_ARROW_LOCATION)
                break()
            endif()
        endforeach()
    endif()
    if (_ARROW_LOCATION)
        get_filename_component(_ARROW_LIB_DIR "${_ARROW_LOCATION}" DIRECTORY)
    endif()
    get_target_property(_ARROW_TARGET_TYPE ${_ARROW_BASE_TARGET} TYPE)
    if (NOT _ARROW_TARGET_TYPE)
        set(_ARROW_TARGET_TYPE "STATIC_LIBRARY")
    endif()

    resolve_system_arrow_component(dataset _ARROW_DATASET_LIB)
    resolve_system_arrow_component(acero _ARROW_ACERO_LIB)
    resolve_system_arrow_component(csv _ARROW_CSV_LIB)
    resolve_system_arrow_component(io _ARROW_IO_LIB)

    if (_ARROW_DATASET_LIB)
        list(APPEND ARROW_LIB ${_ARROW_DATASET_LIB})
        # Expose for extension subdirectories (e.g. parquet) that need to link
        # against the dataset library directly instead of arrow_dataset_objlib
        # (which only exists when Arrow is built from source).
        set(ARROW_DATASET_LIB ${_ARROW_DATASET_LIB})
    endif()
    if (_ARROW_ACERO_LIB)
        list(APPEND ARROW_LIB ${_ARROW_ACERO_LIB})
    endif()
    if (_ARROW_CSV_LIB)
        list(APPEND ARROW_LIB ${_ARROW_CSV_LIB})
    endif()
    if (_ARROW_IO_LIB)
        list(APPEND ARROW_LIB ${_ARROW_IO_LIB})
    endif()

    # Resolve Parquet when the parquet extension is enabled.
    # find_package(Parquet) must be called first to register Parquet::parquet_*
    # targets, which resolve_system_arrow_component can then locate.
    if(ARROW_ENABLE_PARQUET)
        find_package(Parquet CONFIG QUIET)
        if(NOT Parquet_FOUND)
            message(WARNING "System Parquet package not found; parquet extension headers may be missing")
        else()
            # Extract Parquet include directories so extension sources and tests
            # can find <parquet/properties.h> and <parquet/arrow/writer.h> when
            # compiling against system Arrow.
            #
            # Strategy 1: read INTERFACE_INCLUDE_DIRECTORIES from the imported
            # target. Skip generator expressions ($<...>) since
            # include_directories() cannot resolve them as plain paths.
            set(_parquet_inc "")
            foreach(_parquet_target Parquet::parquet_static Parquet::parquet parquet_static parquet)
                if(TARGET ${_parquet_target})
                    get_target_property(_parquet_inc ${_parquet_target} INTERFACE_INCLUDE_DIRECTORIES)
                    if(_parquet_inc AND NOT _parquet_inc MATCHES "\\$<")
                        list(APPEND ARROW_INCLUDE_DIRS ${_parquet_inc})
                        message(STATUS "Appended Parquet include dirs from ${_parquet_target}: ${_parquet_inc}")
                    endif()
                    break()
                endif()
            endforeach()
            # Strategy 2: find_path fallback — used when Strategy 1 found no
            # plain path (e.g. INTERFACE_INCLUDE_DIRECTORIES only has generator
            # expressions, or the target was not found).
            if(NOT _parquet_inc OR _parquet_inc MATCHES "\\$<")
                find_path(_PARQUET_INCLUDE_DIR
                    NAMES parquet/properties.h
                    HINTS ${ARROW_INCLUDE_DIR} /usr/include /usr/local/include
                    NO_DEFAULT_PATH)
                if(NOT _PARQUET_INCLUDE_DIR)
                    find_path(_PARQUET_INCLUDE_DIR NAMES parquet/properties.h)
                endif()
                if(_PARQUET_INCLUDE_DIR)
                    list(APPEND ARROW_INCLUDE_DIRS ${_PARQUET_INCLUDE_DIR})
                    message(STATUS "Appended Parquet include dir via find_path: ${_PARQUET_INCLUDE_DIR}")
                else()
                    message(WARNING "Could not locate parquet/properties.h; parquet extension may fail to compile")
                endif()
            endif()
        endif()
        resolve_system_arrow_component(parquet ARROW_PARQUET_LIB)
        if(ARROW_PARQUET_LIB)
            list(APPEND ARROW_LIB ${ARROW_PARQUET_LIB})
            message(STATUS "Found system parquet lib: ${ARROW_PARQUET_LIB}")
        endif()
    endif()
else()
    include(cmake/BuildArrowAsThirdParty.cmake)
    build_arrow_as_third_party()
endif()

# Add Arrow include directories after building
if(ARROW_INCLUDE_DIRS)
    include_directories(${ARROW_INCLUDE_DIRS})
    message(STATUS "Arrow include directories: ${ARROW_INCLUDE_DIRS}")
endif()

message(STATUS "Arrow library: ${ARROW_LIB}")

# Find Doxygen
if (BUILD_DOC)
    find_package(Doxygen)
    # Add a target to generate the documentation
    if(DOXYGEN_FOUND)
        set(DOXYGEN_IN ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in)
        set(DOXYGEN_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
        configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
        add_custom_target(doc ALL
                COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
                WORKING_DIRECTORY ..
                COMMENT "Generating API documentation with Doxygen"
                VERBATIM)
    endif(DOXYGEN_FOUND)
endif()


include_directories(SYSTEM third_party/rapidjson/include)

add_definitions(-DRAPIDJSON_HAS_CXX11=1)
add_definitions(-DRAPIDJSON_HAS_STDSTRING=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RVALUE_REFS=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RANGE_FOR=1)
add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)

include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/flat_hash_map)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/parallel-hashmap)

macro(install_without_export_neug_target target)
    install(TARGETS ${target}
            RUNTIME DESTINATION bin
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
    )
endmacro()


set(COMPILER_LIBRARIES antlr4_cypher antlr4_runtime fast_float utf8proc re2 ${YAML_CPP_LIBRARIES})
message(STATUS "head: Compiler libraries: ${COMPILER_LIBRARIES}")

# set(COMPILER_LIBRARIES ${COMPILER_LIBRARIES} Threads::Threads)
if(NOT WIN32)
        set(COMPILER_LIBRARIES dl ${COMPILER_LIBRARIES})
endif()
# Seems to be needed for clang on linux only
# for compiling std::atomic<T>::compare_exchange_weak
if ((NOT APPLE AND CMAKE_CXX_COMPILER_ID STREQUAL "Clang") AND NOT __WASM__)
        set(COMPILER_LIBRARIES atomic ${COMPILER_LIBRARIES})
endif()
if (ENABLE_BACKTRACES)
        set(COMPILER_LIBRARIES ${COMPILER_LIBRARIES} cpptrace::cpptrace)
endif()
message(STATUS "head: Compiler libraries: ${COMPILER_LIBRARIES}")

set(MACOS_DYLIBS 
"-framework CoreFoundation"
"-framework CoreGraphics"
"-framework CoreData"
"-framework CoreText"
"-framework Security"
"-framework Foundation"
"-Wl,-U,_MallocExtension_ReleaseFreeMemory"
"-Wl,-U,_ProfilerStart"
"-Wl,-U,_ProfilerStop"
"-Wl,-U,__Z13GetStackTracePPvii"
"-Wl,-U,_mallctl"
"-Wl,-U,_malloc_stats_print"
"-Wl,-U,_OBJC_CLASS_$_NSObject"
"-Wl,-U,_objc_autorelease"
"-Wl,-U,_objc_msgSend"
"-Wl,-U,_objc_retain"
"-Wl,-U,_objc_release"
"-Wl,-U,_objc_opt_class"
"-Wl,-U,_objc_opt_isKindOfClass"
)

# Define NEUG_EXTENSION_HOME_MACRO for core library (extension.cc) and extension libs.
# Must be before add_subdirectory(src) so getUserExtensionDir() is compiled with it.
if(DEFINED CMAKE_LIBRARY_OUTPUT_DIRECTORY AND CMAKE_LIBRARY_OUTPUT_DIRECTORY)
    get_filename_component(_neug_extension_home "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}" ABSOLUTE)
    add_compile_definitions(NEUG_EXTENSION_HOME_MACRO="${_neug_extension_home}")
    message(STATUS "NEUG_EXTENSION_HOME_MACRO is set to ${_neug_extension_home}")
endif()

include_directories(SYSTEM third_party/expected)
add_subdirectory(src)
add_subdirectory(tools)
if (BUILD_EXECUTABLES)
    add_subdirectory(bin)
endif()

if (BUILD_TEST)
    include_directories(tests)
    add_subdirectory(tests)
endif()
if (BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr4_cypher/include)


#-------------------Compiler's third party libraries-------------------
if (BUILD_COMPILER)
    if(NOT MSVC)
        add_compile_options(-Wno-extra)
    endif()
    add_subdirectory(third_party/antlr4_cypher)
    add_subdirectory(third_party/antlr4_runtime)
    add_subdirectory(third_party/glob)
    add_subdirectory(third_party/re2)
    add_subdirectory(third_party/utf8proc)
endif()

set(NEUG_LIBRARIES neug::neug)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/neug-config.in.cmake
               ${CMAKE_CURRENT_BINARY_DIR}/neug-config.cmake @ONLY)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/neug-config-version.in.cmake
               ${CMAKE_CURRENT_BINARY_DIR}/neug-config-version.cmake @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/neug-config.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/neug-config-version.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/neug)

install(EXPORT neug-targets
        FILE neug-targets.cmake
	    NAMESPACE neug::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/neug)

file(GLOB_RECURSE FILES_NEED_FORMAT
        "include/**/*.h"
        "src/**/*.h"
        "src/**/*.cc"
        "tests/**/*.h"
        "tests/**/*.cc"
        "tools/**/*.h"
        "tools/**/*.cc")
list(FILTER FILES_NEED_FORMAT EXCLUDE REGEX ".*\.act.h$|.*\.actg.h$|.*\.autogen.h$|.*\.autogen.cc$")
list(FILTER FILES_NEED_FORMAT EXCLUDE REGEX ".*/third_party/.*")
list(FILTER FILES_NEED_FORMAT EXCLUDE REGEX ".*\.pb.h$|.*\.pb.cc$")
list(FILTER FILES_NEED_FORMAT EXCLUDE REGEX ".*/arrow-src/.*")
list(FILTER FILES_NEED_FORMAT EXCLUDE REGEX ".*/deps/.*")
list(FILTER FILES_NEED_FORMAT EXCLUDE REGEX ".*/build/.*")
list(FILTER FILES_NEED_FORMAT EXCLUDE REGEX ".*\.clang-format$")

add_custom_target(neug_clformat
    COMMAND clang-format --style=file -i ${FILES_NEED_FORMAT}
    COMMENT "Running clang-format, using clang-format-8 from https://github.com/muttleyxd/clang-tools-static-binaries/releases"
    VERBATIM)

file(GLOB_RECURSE FILES_NEED_FORMAT
    "${CMAKE_SOURCE_DIR}/*.cc"
    "${CMAKE_SOURCE_DIR}/*.h")
list(FILTER FILES_NEED_FORMAT EXCLUDE REGEX "/(third_party|deps|build?/.*\\.pb\\.(cc|h))")

add_custom_target(neug_cpplint
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/cpplint.py ${FILES_NEED_LINT}
    COMMENT "Running cpplint"
    VERBATIM)

if (ENABLE_GCOV)
    add_custom_target(coverage
        COMMAND ${CMAKE_COMMAND} -E remove -f coverage.info coverage_filtered.info coverage_html/
        COMMAND fastcov -d ${CMAKE_BINARY_DIR} -l --output coverage.info
        COMMAND lcov --remove coverage.info 
            '/usr/include/*' 
            '/opt/*'
            '*/tests/*'
            '*/third_party/*'
            '*/external/*'
            '*/generated/*'
            '*/_deps/*'
            '/usr/lib/*'
            '*/compiler/*'
            --output-file coverage_filtered.info
        COMMAND find . -name "*.gcno" -delete
        COMMAND find . -name "*.gcda" -delete
    )
endif()

if (NOT DEFINED CPACK_PACKAGE_NAME)
  set(CPACK_PACKAGE_NAME "neug")
endif ()
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "neug is a embedded graph database")
set(CPACK_PACKAGE_VENDOR "GraphScope")
set(CPACK_PACKAGE_VERSION ${NEUG_VERSION_SEMVER})
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_CONTACT "graphscope@alibaba-inc.com")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "graphscope")
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)
set(CPACK_DEB_COMPONENT_INSTALL YES)


#install CMakeLists.txt.template to resources/

install(DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/flat_hash_map
            DESTINATION include/
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "*.hpp"
)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/re2/include/
            DESTINATION include/re2
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "*.hpp"
)
install(FILES ${PROJECT_SOURCE_DIR}/third_party/utf8proc/include/utf8proc_data.h
            ${PROJECT_SOURCE_DIR}/third_party/utf8proc/include/utf8proc.h   
            ${PROJECT_SOURCE_DIR}/third_party/utf8proc/include/utf8proc_wrapper.h
            DESTINATION include/utf8proc
)

if (NOT yaml-cpp_FOUND)
    install(DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/include/
            DESTINATION include/
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "*.hpp"
    )
endif()

if (NOT RapidJSON_FOUND)
    install(DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/rapidjson/include/
            DESTINATION include/
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "*.hpp"
    )
endif()

include(CPack)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
        DESTINATION include
        FILES_MATCHING
        PATTERN "*.h"
)

# Install the generated version header
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/include/neug/version.h
        DESTINATION include/neug
)

# Install the generated config header  
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/include/neug/config.h
        DESTINATION include/neug
)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/expected/
            DESTINATION include
            FILES_MATCHING
            PATTERN "*.hpp"
)
install(FILES ${PROJECT_SOURCE_DIR}/third_party/fast_float/include/fast_float.h
            DESTINATION include/
)



if (WITH_MIMALLOC)
	install_neug_target(mimalloc)
endif()

install_neug_target(${GLOG_LIBRARIES})
install_neug_target(${GFLAGS_LIBRARIES})

if(BUILD_EXTENSIONS)
    message(STATUS "Building extensions: ${BUILD_EXTENSIONS}")
    find_package(OpenSSL REQUIRED)
    add_subdirectory(extension)
endif()
