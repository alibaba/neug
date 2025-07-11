cmake_minimum_required (VERSION 3.10)

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/NEUG_VERSION NEUG_VERSION)
# Strip trailing newline
string(REGEX REPLACE "\n$" "" NEUG_VERSION "${NEUG_VERSION}")

project (
  NeuG
  VERSION ${NEUG_VERSION}
  LANGUAGES C CXX)

option(BUILD_ENGINE "Whether to build engine" ON)
option(BUILD_TEST "Whether to build test" OFF)
option(BUILD_DOC "Whether to build doc" OFF)
option(BUILD_ODPS_FRAGMENT_LOADER "Whether to build odps fragment loader" OFF)
option(USE_PTHASH "Whether to use pthash" OFF)
option(OPTIMIZE_FOR_HOST "Whether to optimize on host" ON) # Whether to build optimized code on host
option(USE_STATIC_ARROW "Whether to use static arrow" OFF) # Whether to link arrow statically, default is OFF
option(BUILD_WITH_OSS "Whether to build with oss support" OFF) # Whether to build with oss support, default is OFF
option(BUILD_EXECUTABLES "When to build executors" OFF) # Whether to build executables, default is OFF
option(BUILD_HTTP_SERVER "Whether to build http server" OFF) # Whether to build http server, default is OFF
set(HTTP_SERVER_TYPE "hiactor" CACHE STRING "The type of http server to use, could be 'hiactor' or 'brpc'")
if (HTTP_SERVER_TYPE STREQUAL "hiactor")
    add_definitions(-DHTTP_SERVER_TYPE_HIACTOR)
elseif (HTTP_SERVER_TYPE STREQUAL "brpc")
    add_definitions(-DHTTP_SERVER_TYPE_BRPC)
else()
    message(FATAL_ERROR "Invalid HTTP_SERVER_TYPE: ${HTTP_SERVER_TYPE}. Supported values are 'hiactor' or 'brpc'.")
endif()

# Options to control the building of tools
option(BUILD_PYTHON "Whether to build python embedded" ON)
option(BUILD_COMPILER "Whether to build compiler" ON)
option(BUILD_ALL_IN_ONE "Whether to build all in one" ON) # Whether to package all libraries into one dynamic library, default is OFF
#---------------------------------------------------------------------------------
#-------------------------------Compiler related options-------------------------------
option(KUZU_DEFAULT_REL_STORAGE_DIRECTION "Only store fwd direction in rel tables by default." BOTH)
if(NOT KUZU_DEFAULT_REL_STORAGE_DIRECTION)
    set(KUZU_DEFAULT_REL_STORAGE_DIRECTION BOTH)
endif()
option(KUZU_PAGE_SIZE_LOG2 "Log2 of the page size." 12)
if(NOT KUZU_PAGE_SIZE_LOG2)
    set(KUZU_PAGE_SIZE_LOG2 12)
endif()
message(STATUS "KUZU_PAGE_SIZE_LOG2: ${KUZU_PAGE_SIZE_LOG2}")
option(KUZU_VECTOR_CAPACITY_LOG2 "Log2 of the vector capacity." 11)
if(NOT KUZU_VECTOR_CAPACITY_LOG2)
    set(KUZU_VECTOR_CAPACITY_LOG2 11)
endif()
message(STATUS "KUZU_VECTOR_CAPACITY_LOG2: ${KUZU_VECTOR_CAPACITY_LOG2}")

# 64 * 2048 nodes per group
option(KUZU_NODE_GROUP_SIZE_LOG2 "Log2 of the vector capacity." 17)
if(NOT KUZU_NODE_GROUP_SIZE_LOG2)
    set(KUZU_NODE_GROUP_SIZE_LOG2 17)
endif()
message(STATUS "KUZU_NODE_GROUP_SIZE_LOG2: ${KUZU_NODE_GROUP_SIZE_LOG2}")
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

set (Seastar_SCHEDULING_GROUPS_COUNT "16" CACHE STRING "A positive number to set Seastar's reactor number of allowed different scheduling groups.")

add_definitions(-DNEUG_CMAKE_VERSION="${CMAKE_PROJECT_VERSION}")
#TODO: Fixme
add_definitions(-DNEUG_EXTENSION_VERSION="0.9.0")
include(CheckCXXCompilerFlag)

if (BUILD_HTTP_SERVER)
    add_compile_definitions(BUILD_HTTP_SERVER)
endif()


if (BUILD_TEST)
    enable_testing()
    find_package(GTest)
    if (GTest_FOUND)
        message(STATUS "GTest found")

        add_library(neug::GTest INTERFACE IMPORTED)
        target_link_libraries(neug::GTest INTERFACE GTest::gtest_main)
        target_link_libraries(neug::GTest INTERFACE GTest::gmock_main)
    endif()
endif()

if (ENABLE_BACKTRACES)
    find_package(cpptrace)
    if (NOT cpptrace_FOUND)
        message(STATUS "cpptrace not found, fetching cpptrace from github")
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
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
# pb files generated under src/utils
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src/utils)

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

function(add_compiler_test TEST_NAME)
    set(SRCS ${ARGN})
    add_executable(${TEST_NAME} ${SRCS})
    #target_link_libraries(${TEST_NAME} PRIVATE test_helper test_runner graph_test)
    #if (ENABLE_BACKTRACES)
    #    target_link_libraries(${TEST_NAME} PRIVATE register_backtrace_signal_handler)
    #endif()
    #target_include_directories(${TEST_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/test/include)
    if (BUILD_ALL_IN_ONE)
        target_link_libraries(${TEST_NAME} PUBLIC neug::GTest neug_libraries)
    else()
        target_link_libraries(${TEST_NAME} PUBLIC neug::GTest neug_compiler_shared neug_plan_proto)
    endif()
    target_include_directories(${TEST_NAME} PRIVATE src/include)

    include(GoogleTest)

    if (TEST_NAME STREQUAL "e2e_test")
        gtest_discover_tests(${TEST_NAME}
            DISCOVERY_TIMEOUT 600
            # DISCOVERY_MODE PRE_TEST
            DISCOVERY_MODE POST_BUILD
            TEST_PREFIX e2e_test_
        )
    else()
	    #    gtest_discover_tests(${TEST_NAME}
            #    gtesDISCOVERY_TIMEOUT 600
            #    gtes# DISCOVERY_MODE PRE_TEST
            #    gtesDISCOVERY_MODE POST_BUILD
	    #)
	add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
	gtest_add_tests(TARGET ${TEST_NAME} TEST_LIST ${TEST_NAME}_tests)
    endif()
endfunction()

if(USE_PTHASH)
    message("Use PTHash")
    add_definitions(-DUSE_PTHASH)
endif()

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

# find glog---------------------------------------------------------------------
include("cmake/FindGlog.cmake")
if (NOT GLOG_FOUND)
    message(FATAL_ERROR "glog not found, please install the glog library")
else ()
    include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
endif ()

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
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/pybind11/CMakeLists.txt")
    message(STAUTS "pybind11 not found, running git submodule update --init")
    execute_process(
        COMMAND git submodule update --init
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        RESULT_VARIABLE result
    )
    if(result)
        message(FATAL_ERROR "Failed to update git submodule: ${result}")
    endif()
endif()
    # To build wheel in manylinux docker, we should not link to a specific python library.
    set(PYBIND11_FINDPYTHON OFF)
    add_subdirectory(third_party/pybind11)
endif()


#find boost----------------------------------------------------------------------
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()
find_package(Boost REQUIRED COMPONENTS system filesystem
             # required by folly
             context program_options regex thread date_time)
add_definitions("-DBOOST_BIND_GLOBAL_PLACEHOLDERS")
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

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

# -Wno-psabi for no warning about returning a pair in c++ function
if (OPTIMIZE_FOR_HOST AND NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
#if compiler is gcc
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-memaccess")
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

if(${ENABLE_RUNTIME_CHECKS})
    add_compile_definitions(KUZU_RUNTIME_CHECKS)
endif()

if (${ENABLE_DESER_DEBUG})
    add_compile_definitions(KUZU_DESER_DEBUG)
endif()

if(${ENABLE_LTO})
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# find Threads------------------------------------------------------------------
set(CMAKE_THREAD_PREFER_PTHREAD ON)
find_package(Threads REQUIRED)

# find gflags-------------------------------------------------------------------
include("cmake/FindGFlags.cmake")
if (NOT GFLAGS_FOUND)
    message(STATUS "gflags not found, build without gflags")
    message(STATUS "gflags library ${GFLAGS_LIBRARIES}")
else ()
    include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
endif ()

# find yaml-cpp-----------------------------------------------------------------
find_package(yaml-cpp REQUIRED)
include_directories(SYSTEM ${yaml-cpp_INCLUDE_DIRS})
if (NOT yaml-cpp_FOUND)
    message(FATAL_ERROR "yaml-cpp not found, please install the yaml-cpp library")
endif ()
# In manylinux_2_28, it seems that yaml-cpp cannot be found via YAML_CPP_LIBRARIES, we adhere try to find via the absolute path.
set(YAML_CPP_LIBRARY_PATH /opt/graphscope/lib64/libyaml-cpp.so)
if (EXISTS ${YAML_CPP_LIBRARY_PATH})
        set(YAML_CPP_LIBRARIES ${YAML_CPP_LIBRARY_PATH})
endif()


#find hiactor----------------------------------------------------------------------
if (BUILD_HTTP_SERVER)
    if (HTTP_SERVER_TYPE STREQUAL "hiactor")        
        find_package(Hiactor REQUIRED)
        include_directories(SYSTEM ${Hiactor_INCLUDE_DIRS})
    elseif (HTTP_SERVER_TYPE STREQUAL "brpc")
        find_library(BRPC_LIB NAMES brpc)
        find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
        if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
                message(FATAL_ERROR "Fail to find brpc")
        endif()
        find_package(OpenSSL REQUIRED)
        find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
        find_library(LEVELDB_LIB NAMES leveldb)
        if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
                message(FATAL_ERROR "Fail to find leveldb")
        endif()
        include_directories(${LEVELDB_INCLUDE_PATH})
        find_library(PROTOC_LIB NAMES protoc)
        if(NOT PROTOC_LIB)
            message(FATAL_ERROR "Fail to find protoc lib")
        endif()
    endif()
endif()

#find protobuf-------------------------------------------------------------------
if (APPLE)
    find_package(absl REQUIRED)
endif()
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
message(STATUS "protobuf libraries: ${Protobuf_LIBRARIES}")

#find arrow----------------------------------------------------------------------
find_package(Arrow REQUIRED)
if (NOT Arrow_FOUND)
    message(FATAL_ERROR "arrow not found, please install the arrow library")
else ()
    include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
    if (USE_STATIC_ARROW)
        if (TARGET Arrow::arrow_static)
            set(ARROW_LIB Arrow::arrow_static)
        elseif (TARGET arrow_static) # For backward compatibility, see https://arrow.apache.org/docs/cpp/build_system.html#available-variables-and-targets
            set(ARROW_LIB arrow_static)
        else()
            message(FATAL_ERROR "Building with USE_STATIC_ARROW=ON, but arrow_static target not found")
        endif()
    else ()
        if (TARGET Arrow::arrow_shared)
            set(ARROW_LIB Arrow::arrow_shared)
        elseif (TARGET arrow_shared) # For backward compatibility
            set(ARROW_LIB arrow_shared)
        else()
            message(FATAL_ERROR "Building with USE_STATIC_ARROW=OFF, but arrow_shared target not found")
        endif()
    endif()
endif ()

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

find_package(RapidJSON REQUIRED)
if (DEFINED RapidJSON_INCLUDE_DIRS)  # rapidjson > 1.1.0
    include_directories(${RapidJSON_INCLUDE_DIRS})
else ()  # rapidjson <= 1.1.0
    include_directories(${RAPIDJSON_INCLUDE_DIRS})
endif ()

add_definitions(-DRAPIDJSON_HAS_CXX11=1)
add_definitions(-DRAPIDJSON_HAS_STDSTRING=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RVALUE_REFS=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RANGE_FOR=1)

if (BUILD_ODPS_FRAGMENT_LOADER)
    include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/odps/include)
endif()
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/flat_hash_map)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/parallel-hashmap)

macro(install_neug_target target)
    install(TARGETS ${target}
            EXPORT neug-targets
            RUNTIME DESTINATION bin
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
    )
endmacro()

macro(install_without_export_neug_target target)
    install(TARGETS ${target}
            RUNTIME DESTINATION bin
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
    )
endmacro()


set(COMPILER_LIBRARIES antlr4_cypher antlr4_runtime fast_float utf8proc re2 ${YAML_CPP_LIBRARIES})
message(STATUS "head: Compiler libraries: ${COMPILER_LIBRARIES}")

set(COMPILER_LIBRARIES ${COMPILER_LIBRARIES} Threads::Threads)
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

add_subdirectory(src)

if (BUILD_TEST)
  if (BUILD_ENGINE)
    add_subdirectory(tests)
  endif()
endif()
if (BUILD_ENGINE)
 add_subdirectory(tools)
endif()
add_subdirectory(third_party/libgrape-lite)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/libgrape-lite)

include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/re2/include)
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

file(GLOB_RECURSE FILES_NEED_LINT
        "src/**/*.h"
        "src/**/*.cc"
        "tests/**/*.h"
        "tests/**/*.cc"
        "tools/**/*.h"
        "tools/**/*.cc"
        EXCEPT "*.act.h" "*.actg.h" "*.autogen.h" "*.autogen.cc")
list(FILTER FILES_NEED_LINT EXCLUDE REGEX ".*\.act.h$|.*\.actg.h$|.*\.autogen.h$|.*\.autogen.cc$")
list(FILTER FILES_NEED_LINT EXCLUDE REGEX ".*/third_party/.*")

add_custom_target(neug_clformat
    COMMAND clang-format --style=file -i ${FILES_NEED_LINT}
    COMMENT "Running clang-format, using clang-format-8 from https://github.com/muttleyxd/clang-tools-static-binaries/releases"
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

include(CPack)
