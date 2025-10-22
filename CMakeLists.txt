cmake_minimum_required (VERSION 3.10)

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/NEUG_VERSION NEUG_VERSION)
# Strip trailing newline
string(REGEX REPLACE "\n$" "" NEUG_VERSION "${NEUG_VERSION}")

# Parse version components
string(REGEX MATCH "^([0-9]+)\\.([0-9]+)\\.([0-9]+)" _ ${NEUG_VERSION})
set(NEUG_VERSION_MAJOR ${CMAKE_MATCH_1})
set(NEUG_VERSION_MINOR ${CMAKE_MATCH_2})
set(NEUG_VERSION_PATCH ${CMAKE_MATCH_3})

message(STATUS "NeuG Version: ${NEUG_VERSION} (${NEUG_VERSION_MAJOR}.${NEUG_VERSION_MINOR}.${NEUG_VERSION_PATCH})")

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
  VERSION ${NEUG_VERSION}
  LANGUAGES C CXX)

option(BUILD_TEST "Whether to build test" OFF)
option(BUILD_DOC "Whether to build doc" OFF)
option(BUILD_ODPS_FRAGMENT_LOADER "Whether to build odps fragment loader" OFF)
option(USE_PTHASH "Whether to use pthash" OFF)
option(OPTIMIZE_FOR_HOST "Whether to optimize on host" ON) # Whether to build optimized code on host
option(BUILD_WITH_OSS "Whether to build with oss support" OFF) # Whether to build with oss support, default is OFF
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
option(AUTO_UPDATE_GRAMMAR "Automatically regenerate C++ grammar files on change." TRUE)
option(BUILD_EXTENSIONS "Semicolon-separated list of extensions to build." "")
option(BUILD_SINGLE_FILE_HEADER "Build single file header. Requires Python >= 3.9." FALSE)
option(BUILD_TEST "Build C++ tests." TRUE)
option(BUILD_EXTENSION_TESTS "Build C++ extension tests." FALSE)
option(ENABLE_BACKTRACES "Enable backtrace printing for exceptions and segfaults" FALSE)
option(BUILD_EXAMPLES "Build examples." ON)


add_definitions(-DNEUG_CMAKE_VERSION="${CMAKE_PROJECT_VERSION}")
#TODO: Fixme
add_definitions(-DNEUG_EXTENSION_VERSION="0.1.0")
include(CheckCXXCompilerFlag)

macro(install_neug_target target)
    install(TARGETS ${target}
            EXPORT neug-targets
            RUNTIME DESTINATION bin
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
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
    set(MI_USE_LIBATOMIC OFF CACHE BOOL "Use libatomic")
    # we should not use static mimalloc, the reason is that static mimalloc will override the default malloc.
    add_subdirectory(third_party/mimalloc)
    set_target_properties(mimalloc PROPERTIES DEBUG_POSTFIX "")
    include_directories(SYSTEM third_party/mimalloc/include)
    unset(PACKAGE_VERSION) # Unset PACKAGE_VERSION to avoid generate two PACKAGE_VERSION in config.h for libdwarf-lite.
endif()

set(SKIP_PERFORMANCE_COMPARISON ON CACHE BOOL "Skip building performance sandbox comparison (requires boost)")
set(BUILD_SANDBOX OFF CACHE BOOL "Build sandbox examples")
add_subdirectory(third_party/cereal)
include_directories(SYSTEM ${CMAKE_CURRENT_SOURCE_DIR}/third_party/cereal/include)

if (BUILD_TEST)
    enable_testing()
    find_package(GTest)
    if (GTest_FOUND)
        message(STATUS "GTest found")

        add_library(neug::GTest INTERFACE IMPORTED)
        target_link_libraries(neug::GTest INTERFACE GTest::gtest_main)
        target_link_libraries(neug::GTest INTERFACE GTest::gmock_main)
    else()
        add_subdirectory(third_party/gtest EXCLUDE_FROM_ALL)

        add_library(neug::GTest INTERFACE IMPORTED)
        target_link_libraries(neug::GTest INTERFACE gtest_main)
        target_link_libraries(neug::GTest INTERFACE gmock_main)
    endif()
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

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-Wno-restrict) # no restrict until https://gcc.gnu.org/bugzilla/show_bug.cgi?id=105651 is fixed
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
message(STATUS "Build odps fragment loader: ${BUILD_ODPS_FRAGMENT_LOADER}")
message(STATUS "Use pthash indexer : ${USE_PTHASH}")

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

function(add_neug_test TEST_NAME)
    find_package(OpenSSL REQUIRED)
    set(SRCS ${ARGN})
    add_executable(${TEST_NAME} ${SRCS})
    target_link_libraries(${TEST_NAME} PUBLIC neug::GTest neug_libraries   
        ${GLOG_LIBRARIES} ${Protobuf_LIBRARIES} ${ARROW_LIB} ${OPENSSL_LIBRARIES} 
        ${CRYPTO_LIB} ${GFLAGS_LIBRARIES})
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

# if os is macos, 
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message("macos detected")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-conversion -fclang-abi-compat=14") # need fclang-abi-compat for clang compiler, due to absl problem: https://github.com/protocolbuffers/protobuf/issues/12693
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated")

if (BUILD_WITH_OSS)
    add_definitions(-DBUILD_WITH_OSS)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/aliyun-oss-cpp-sdk/sdk/include)
    set(TARGET_OUTPUT_NAME_PREFIX "alibabacloud-oss-" CACHE STRING "The target's output name prefix")
    add_subdirectory(third_party/aliyun-oss-cpp-sdk)
endif()

if (BUILD_PYTHON)
    message("Build python embedded")
    # To build wheel in manylinux docker, we should not link to a specific python library.
    set(PYBIND11_FINDPYTHON OFF)
    add_subdirectory(third_party/pybind11)
endif()


#find boost----------------------------------------------------------------------
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()

include_directories(SYSTEM third_party/boost/date_time/include)
include_directories(SYSTEM third_party/boost/container_hash/include)
include_directories(SYSTEM third_party/boost/algorithm/include)
include_directories(SYSTEM third_party/HowardHinnant/date/include)


if(USE_PTHASH)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/murmurhash)
endif()
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)

set(DEFAULT_BUILD_TYPE "Release")
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE
      STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
               "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif ()

add_compile_definitions(NEUG_VERSION="${NEUG_VERSION}")

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
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-memaccess -Wno-maybe-uninitialized")
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-uninitialized")
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-nonnull ") # brought by arrow


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

# find Threads------------------------------------------------------------------
set(CMAKE_THREAD_PREFER_PTHREAD ON)
find_package(Threads REQUIRED)

# find gflags-------------------------------------------------------------------
# We need gflags for brpc, so we build it as a third party library
include(cmake/BuildGflagsAsThirdParty.cmake)
build_gflags_as_third_party()
include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
message(STATUS "gflags libraries: ${GFLAGS_LIBRARIES}")

# find yaml-cpp-----------------------------------------------------------------
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)
set(YAML_CPP_INSTALL OFF CACHE BOOL "Install yaml-cpp")
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "Build yaml-cpp tools")
add_subdirectory(third_party/yaml-cpp)
include_directories(SYSTEM third_party/yaml-cpp/include)
set(YAML_CPP_LIBRARIES yaml-cpp)
set_target_properties(yaml-cpp PROPERTIES DEBUG_POSTFIX "")
message(STATUS "yaml-cpp libraries: ${YAML_CPP_LIBRARIES}")

#find protobuf-------------------------------------------------------------------
find_package(Protobuf)
if (NOT Protobuf_FOUND)
    include(cmake/BuildProtobufThirdParty.cmake)
    build_protobuf_as_third_party()
    set(USE_SYSTEM_PROTOBUF OFF CACHE BOOL "Use system protobuf")
else()
    set(USE_SYSTEM_PROTOBUF ON CACHE BOOL "Use system protobuf")
    add_compile_definitions(USE_SYSTEM_PROTOBUF)
endif()
include_directories(SYSTEM ${Protobuf_INCLUDE_DIRS})
message(STATUS "protobuf include dir: ${Protobuf_INCLUDE_DIR}, ${Protobuf_INCLUDE_DIRS}")
message(STATUS "protobuf libraries: ${Protobuf_LIBRARIES}")
message(STATUS "Use system protobuf: ${USE_SYSTEM_PROTOBUF}")

# Check protobuf version for conditional patching
if(Protobuf_FOUND AND Protobuf_VERSION)
    message(STATUS "protobuf version: ${Protobuf_VERSION}")
    # Convert version to comparable format (e.g., "31.0.0" becomes 31000000)
    string(REPLACE "." ";" VERSION_LIST ${Protobuf_VERSION})
    list(LENGTH VERSION_LIST VERSION_LIST_LENGTH)
    # It seems that Protobuf_VERSION is not correct, for example, 32.1 become 6.32.1
    if(VERSION_LIST_LENGTH GREATER 1)
        list(GET VERSION_LIST 1 PROTOBUF_MINOR)
    endif()
    # Only check the protobuf_minor version for now
    message(STATUS "PROTOBUF_MAJOR: ${PROTOBUF_MAJOR}, PROTOBUF_MINOR: ${PROTOBUF_MINOR}, PROTOBUF_PATCH: ${PROTOBUF_PATCH}")
    
    if(PROTOBUF_MINOR GREATER_EQUAL 31)
        set(APPLY_BRPC_PATCH TRUE)
        message(STATUS "protobuf version ${Protobuf_VERSION} >= 31.0, will apply brpc patch if needed")
    else()
        set(APPLY_BRPC_PATCH FALSE)
        message(STATUS "protobuf version ${Protobuf_VERSION} < 31.0, will not apply brpc patch")
    endif()
else()
    # If version is not available or protobuf is built from source, default to applying patch
    set(APPLY_BRPC_PATCH TRUE)
    message(STATUS "protobuf version not available, defaulting to applying brpc patch if needed")
endif()

# Generate config header. Need to placed after protobuf find_package, for macro USE_SYSTEM_PROTOBUF
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

    find_package(OpenSSL)
    if (NOT OpenSSL_FOUND)
        message(FATAL_ERROR "Fail to find OpenSSL, try to install openssl development package. On macos: brew install openssl@3; on ubuntu: apt-get install libssl-dev")
    endif()
    find_library(CRYPTO_LIB NAMES crypto)
    if (NOT CRYPTO_LIB)
        message(FATAL_ERROR "Fail to find crypto lib")
    endif()

    if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
            # Requires gflags, leveldb-dev to be installed.
            set(BUILD_BRPC_AS_THIRD_PARTY TRUE CACHE BOOL "Build brpc as third party")
            message(STATUS "Fail to find brpc, try build from source")
            set(WITH_DEBUG_SYMBOLS OFF CACHE BOOL "Build brpc with debug symbols")
            set(BUILD_BRPC_TOOLS OFF CACHE BOOL "Build brpc tools")
            set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build brpc as static library" FORCE)
            set(WITH_GLOG ON CACHE BOOL "Build brpc with glog")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare -Wno-deprecated-declarations -Wno-attributes")

            # We will always apply brpc_cmake.patch to fix the issue of dlsym issue related to packing wheel. 
            # https://github.com/GraphScope/neug/issues/365
            execute_process(COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc_cmake.patch
                        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc
                        RESULT_VARIABLE result
                        OUTPUT_VARIABLE output
                        ERROR_VARIABLE error_output)


            if (APPLY_BRPC_PATCH)
                if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc.patch")
                    message(FATAL_ERROR "brpc.patch not found in third_party")
                endif()
                message(STATUS "Applying brpc patch due to protobuf version >= 31.0")
                execute_process(COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc.patch
                            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc
                            RESULT_VARIABLE result
                            OUTPUT_VARIABLE output
                            ERROR_VARIABLE error_output)
            else()
                message(STATUS "Skipping brpc patch application due to protobuf version < 31.0")
            endif()

            add_subdirectory(third_party/brpc)
            set(BRPC_INCLUDE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc/include)
            set(BRPC_LIB brpc-static)
            include_directories(SYSTEM ${CMAKE_CURRENT_BINARY_DIR}/third_party/brpc/output/include)
    endif()
    include_directories(SYSTEM ${BRPC_INCLUDE_PATH})
endif()

#find arrow----------------------------------------------------------------------

include(cmake/BuildArrowAsThirdParty.cmake)
build_arrow_as_third_party()

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

if (BUILD_ODPS_FRAGMENT_LOADER)
    include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/odps/include)
endif()
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

include_directories(SYSTEM third_party/expected)
add_subdirectory(src)
add_subdirectory(tools)
if (BUILD_EXECUTABLES)
    add_subdirectory(bin)
endif()

if (BUILD_TEST)
    add_subdirectory(tests)
endif()
add_subdirectory(third_party/libgrape-lite)
if (BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/libgrape-lite)

include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr4_cypher/include)


#-------------------Compiler's third party libraries-------------------
if (BUILD_COMPILER)
    if(NOT MSVC)
        add_compile_options(-Wno-extra)
    endif()
    add_subdirectory(third_party/antlr4_cypher)
    add_subdirectory(third_party/antlr4_runtime)
    add_subdirectory(third_party/fast_float)
    add_subdirectory(third_party/glob)
    add_subdirectory(third_party/re2)
    add_subdirectory(third_party/utf8proc)
endif()

set(NEUG_LIBRARIES neug::neug_libraries)

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

file(GLOB_RECURSE FILES_NEED_LINT
        "include/neug/neug.h"
        "include/neug/execution/*.h"
        "include/neug/main/*.h"
        "include/neug/server/*.h"
        "include/neug/storage/*.h"
        "include/neug/transaction/*.h"
        "include/neug/utils/*.h"
        "src/execution/*.cc"
        "src/main/*.cc"
        "src/server/*.cc"
        "src/storage/*.cc"
        "src/transaction/*.cc"
        "src/utils/*.cc")
list(FILTER FILES_NEED_LINT EXCLUDE REGEX "\\.pb.h$|\\.pb.cc$")

add_custom_target(neug_cpplint
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/cpplint.py ${FILES_NEED_LINT}
    COMMENT "Running cpplint"
    VERBATIM)

if (NOT DEFINED CPACK_PACKAGE_NAME)
  set(CPACK_PACKAGE_NAME "neug")
endif ()
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "neug is a embedded graph database")
set(CPACK_PACKAGE_VENDOR "GraphScope")
set(CPACK_PACKAGE_VERSION ${NEUG_VERSION})
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_CONTACT "graphscope@alibaba-inc.com")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "graphscope")
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)
set(CPACK_DEB_COMPONENT_INSTALL YES)


#install CMakeLists.txt.template to resources/


if(USE_PTHASH)
    install(DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/murmurhash
                      ${PROJECT_SOURCE_DIR}/third_party/pthash
            DESTINATION include
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "*.hpp"
    )
endif()

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

install(DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/pthash
            DESTINATION include
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "*.hpp"
)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/string_view
            DESTINATION include
            FILES_MATCHING
            PATTERN "*.hpp"
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

# install all headers under include/neug/
if (USE_SYSTEM_PROTOBUF)
    #filter out all pb.h files
    install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
            DESTINATION include
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "*.pb.h" EXCLUDE
    )
else()
    install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
            DESTINATION include
            FILES_MATCHING
            PATTERN "*.h"
    )
endif()

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

if (NOT USE_SYSTEM_PROTOBUF)
    install_neug_target(libprotobuf)
    install_neug_target(libprotobuf-lite)
    install_neug_target(libprotoc)
endif()

if(BUILD_EXTENSIONS)
    string(REPLACE ";" " " EXTENSIONS_LIST "${BUILD_EXTENSIONS}")
    message(STATUS "Building extensions: ${EXTENSIONS_LIST}")
    
    foreach(EXTENSION ${BUILD_EXTENSIONS})
        string(STRIP "${EXTENSION}" EXTENSION)
        if(EXTENSION)
            set(EXTENSION_DIR "extension/${EXTENSION}")
            if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${EXTENSION_DIR}/CMakeLists.txt")
                message(STATUS "Adding extension: ${EXTENSION}")
                add_subdirectory(${EXTENSION_DIR})
            else()
                message(WARNING "Extension directory not found: ${EXTENSION_DIR}")
            endif()
        endif()
    endforeach()
endif()
