add_compile_options(-w)

# set(CMAKE_CXX_STANDARD 20)


include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/re2/include)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr4_cypher/include)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr4_runtime/src)

include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/mbedtls/include)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/utf8proc/include)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/miniz)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/parquet)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/nlohmann_json)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/thrift)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/thrift/protocol)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr_cypher)


configure_file(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/templates/system_config.h.in ${CMAKE_CURRENT_BINARY_DIR}/common/system_config.h @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})



# if(NOT CMAKE_BUILD_TYPE)
#     set(CMAKE_BUILD_TYPE Release)
# endif()

# if(DEFINED ENV{PYBIND11_PYTHON_VERSION})
#     set(PYBIND11_PYTHON_VERSION $ENV{PYBIND11_PYTHON_VERSION})
# endif()

# if(DEFINED ENV{PYTHON_EXECUTABLE})
#     set(PYTHON_EXECUTABLE $ENV{PYTHON_EXECUTABLE})
# endif()

# find_program(CCACHE_PROGRAM ccache)
# if (CCACHE_PROGRAM)
#     set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
# else ()
#     find_program(CCACHE_PROGRAM sccache)
#     if (CCACHE_PROGRAM)
#         set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
#     endif ()
# endif ()

# set(INSTALL_LIB_DIR
#         lib
#         CACHE PATH "Installation directory for libraries")
# set(INSTALL_BIN_DIR
#         bin
#         CACHE PATH "Installation directory for executables")
# set(INSTALL_INCLUDE_DIR
#         include
#         CACHE PATH "Installation directory for header files")
# set(INSTALL_CMAKE_DIR
#         ${DEF_INSTALL_CMAKE_DIR}
#         CACHE PATH "Installation directory for CMake files")



# if(MSVC)
#     # Required for M_PI on Windows
#     add_compile_definitions(_USE_MATH_DEFINES)
#     add_compile_definitions(NOMINMAX)
#     add_compile_definitions(SERD_STATIC)
#     # This is a workaround for regex oom issue on windows in gtest.
#     add_compile_definitions(_REGEX_MAX_STACK_COUNT=0)
#     add_compile_definitions(_REGEX_MAX_COMPLEXITY_COUNT=0)
#     # Disable constexpr mutex constructor to avoid compatibility issues with
#     # older versions of the MSVC runtime library
#     # See: https://github.com/microsoft/STL/wiki/Changelog#vs-2022-1710
#     add_compile_definitions(_DISABLE_CONSTEXPR_MUTEX_CONSTRUCTOR)
#     # TODO (bmwinger): Figure out if this can be set automatically by cmake,
#     # or at least better integrated with user-specified options
#     # For now, hardcode _AMD64_
#     # CMAKE_GENERATOR_PLATFORM can be used for visual studio builds, but not for ninja
#     add_compile_definitions(_AMD64_)
#     # Non-english windows system may use other encodings other than utf-8 (e.g. Chinese use GBK).
#     add_compile_options("/utf-8")
#     # Enables support for custom hardware exception handling
#     add_compile_options("/EHa")
#     # Reduces the size of the static library by roughly 1/2
#     add_compile_options("/Zc:inline")
#     # Remove the default to avoid warnings
#     STRING(REPLACE "/EHsc" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
#     STRING(REPLACE "/EHs" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
#     # Store all libraries and binaries in the same directory so that kuzu_shared.dll is found at runtime
#     set(LIBRARY_OUTPUT_PATH "${CMAKE_BINARY_DIR}/src")
#     set(EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/src")
#     # This is a workaround for regex stackoverflow issue on windows in gtest.
#     set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /STACK:8388608")

#     string(REGEX REPLACE "/W[3|4]" "/w" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
#     add_compile_options($<$<CONFIG:Release>:/W0>)
# else()
#     add_compile_options(-Wall -Wextra)
#     # Disable warnings for unknown pragmas, which is used by several third-party libraries
#     add_compile_options(-Wno-unknown-pragmas)
# endif()

# if(${BUILD_WASM})
#     if(NOT __SINGLE_THREADED__)
#         add_compile_options(-pthread)
#         add_link_options(-pthread)
#         add_link_options(-sPTHREAD_POOL_SIZE=8)
#     endif()
#     add_compile_options(-s DISABLE_EXCEPTION_CATCHING=0)
#     add_link_options(-sSTACK_SIZE=4MB)
#     add_link_options(-sASSERTIONS=1)
#     add_link_options(-lembind)
#     add_link_options(-sWASM_BIGINT)

#     if(BUILD_TESTS OR BUILD_EXTENSION_TESTS)
#         add_link_options(-sINITIAL_MEMORY=3892MB)
#         add_link_options(-sNODERAWFS=1)
#     elseif(WASM_NODEFS)
#         add_link_options(-sNODERAWFS=1)
#         add_link_options(-sALLOW_MEMORY_GROWTH=1)
#         add_link_options(-sMODULARIZE=1)
#         add_link_options(-sEXPORTED_RUNTIME_METHODS=FS,wasmMemory)
#         add_link_options(-sEXPORT_NAME=kuzu)
#         add_link_options(-sMAXIMUM_MEMORY=4GB)
#     else()
#         add_link_options(-sSINGLE_FILE=1)
#         add_link_options(-sALLOW_MEMORY_GROWTH=1)
#         add_link_options(-sMODULARIZE=1)
#         add_link_options(-sEXPORTED_RUNTIME_METHODS=FS,wasmMemory)
#         add_link_options(-lidbfs.js)
#         add_link_options(-lworkerfs.js)
#         add_link_options(-sEXPORT_NAME=kuzu)
#         add_link_options(-sMAXIMUM_MEMORY=4GB)
#     endif()
#     set(__WASM__ TRUE)
#     add_compile_options(-fexceptions)
#     add_link_options(-s DISABLE_EXCEPTION_CATCHING=0)
#     add_link_options(-fexceptions)
#     add_compile_definitions(__WASM__)
# endif()

# if(ANDROID_ABI)
#     message(STATUS "Android ABI detected: ${ANDROID_ABI}")
#     add_compile_definitions(__ANDROID__)
#     set(__ANDROID__ TRUE)
# endif()

# if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
#     add_compile_options(-Wno-restrict) # no restrict until https://gcc.gnu.org/bugzilla/show_bug.cgi?id=105651 is fixed
# endif()

# if(${ENABLE_THREAD_SANITIZER} AND (NOT __SINGLE_THREADED__))
#     if(MSVC)
#         message(FATAL_ERROR "Thread sanitizer is not supported on MSVC")
#     else()
#         set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fno-omit-frame-pointer")
#     endif()
# endif()
# if(${ENABLE_ADDRESS_SANITIZER})
#     if(MSVC)
#         set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /fsanitize=address")
#     else()
#         set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
#     endif()
# endif()
# if(${ENABLE_UBSAN})
#     if(MSVC)
#         message(FATAL_ERROR "Undefined behavior sanitizer is not supported on MSVC")
#     else()
#         set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-omit-frame-pointer")
#     endif()
# endif()

# if(${ENABLE_RUNTIME_CHECKS})
#     add_compile_definitions(KUZU_RUNTIME_CHECKS)
# endif()

# if (${ENABLE_DESER_DEBUG})
#     add_compile_definitions(KUZU_DESER_DEBUG)
# endif()

# if(${ENABLE_LTO})
#     set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
# endif()

# option(AUTO_UPDATE_GRAMMAR "Automatically regenerate C++ grammar files on change." TRUE)
# option(BUILD_EXTENSIONS "Semicolon-separated list of extensions to build." "")
# option(BUILD_SINGLE_FILE_HEADER "Build single file header. Requires Python >= 3.9." FALSE)
# option(BUILD_TESTS "Build C++ tests." TRUE)
# option(BUILD_EXTENSION_TESTS "Build C++ extension tests." FALSE)
# option(BUILD_KUZU "Build Kuzu." TRUE)
# option(ENABLE_BACKTRACES "Enable backtrace printing for exceptions and segfaults" FALSE)


# if (BUILD_TESTS)
#     enable_testing()
#     find_package(GTest)
#     if (GTest_FOUND)
#         message(STATUS "GTest found")

#         add_library(neug::GTest INTERFACE IMPORTED)
#         target_link_libraries(neug::GTest INTERFACE GTest::gtest_main)
#         target_link_libraries(neug::GTest INTERFACE GTest::gmock_main)
#     endif()
# endif()

# if (ENABLE_BACKTRACES)
#     find_package(cpptrace)
#     if (NOT cpptrace_FOUND)
#         include(FetchContent)
#         FetchContent_Declare(
#           cpptrace
#           GIT_REPOSITORY https://github.com/jeremy-rifkin/cpptrace.git
#           GIT_TAG        v0.5.4
#           GIT_SHALLOW    TRUE
#         )
#         FetchContent_MakeAvailable(cpptrace)
#     endif()
#     add_compile_definitions(KUZU_BACKTRACE)
# endif()

# function(add_kuzu_test TEST_NAME)
#     set(SRCS ${ARGN})
#     add_executable(${TEST_NAME} ${SRCS})
#     #target_link_libraries(${TEST_NAME} PRIVATE test_helper test_runner graph_test)
#     #if (ENABLE_BACKTRACES)
#     #    target_link_libraries(${TEST_NAME} PRIVATE register_backtrace_signal_handler)
#     #endif()
#     #target_include_directories(${TEST_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/test/include)

#     target_link_libraries(${TEST_NAME} PUBLIC neug::GTest kuzu)
#     target_include_directories(${TEST_NAME} PRIVATE src/include)

#     include(GoogleTest)

#     if (TEST_NAME STREQUAL "e2e_test")
#         gtest_discover_tests(${TEST_NAME}
#             DISCOVERY_TIMEOUT 600
#             # DISCOVERY_MODE PRE_TEST
#             DISCOVERY_MODE POST_BUILD
#             TEST_PREFIX e2e_test_
#         )
#     else()
# 	    #    gtest_discover_tests(${TEST_NAME}
#             #    gtesDISCOVERY_TIMEOUT 600
#             #    gtes# DISCOVERY_MODE PRE_TEST
#             #    gtesDISCOVERY_MODE POST_BUILD
# 	    #)
# 	add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
# 	gtest_add_tests(TARGET ${TEST_NAME} TEST_LIST ${TEST_NAME}_tests)
#     endif()
# endfunction()

# Windows doesn't support dynamic lookup, so we have to link extensions against kuzu.
# if (MSVC AND (NOT BUILD_EXTENSIONS EQUAL ""))
#     set(BUILD_KUZU TRUE)
# endif ()

# include_directories(third_party/antlr4_cypher/include)
# include_directories(third_party/antlr4_runtime/src)
# # include_directories(third_party/brotli/c/include)
# include_directories(third_party/fast_float/include)
# include_directories(third_party/mbedtls/include)
# include_directories(third_party/parquet)
# include_directories(third_party/snappy)
# include_directories(third_party/thrift)
# include_directories(third_party/miniz)
# include_directories(third_party/nlohmann_json)
# include_directories(third_party/pybind11/include)
# include_directories(third_party/pyparse)
# include_directories(third_party/re2/include)
# include_directories(third_party/alp/include)
# if (${BUILD_TESTS} OR ${BUILD_EXTENSION_TESTS})
#     include_directories(third_party/spdlog)
# elseif (${BUILD_BENCHMARK})
#     include_directories(third_party/spdlog)
# endif ()
# include_directories(third_party/utf8proc/include)
# include_directories(third_party/zstd/include)
# include_directories(third_party/httplib)
# include_directories(third_party/pcg)
# include_directories(third_party/lz4)
# include_directories(third_party/roaring_bitmap)
# Use SYSTEM to suppress warnings from simsimd
# include_directories(SYSTEM third_party/simsimd/include)

# add_subdirectory(third_party)

# add_definitions(-DKUZU_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}")
# add_definitions(-DKUZU_CMAKE_VERSION="${CMAKE_PROJECT_VERSION}")
# add_definitions(-DKUZU_EXTENSION_VERSION="0.9.0")



# include_directories(
#     src/include
#     ${CMAKE_BINARY_DIR}/src/include
# )
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(src)

include(CTest)
enable_testing()

if (BUILD_TEST)
    add_subdirectory(test)
endif()

#if (${BUILD_TESTS} OR ${BUILD_EXTENSION_TESTS})
#    add_subdirectory(test)
#elseif (${BUILD_BENCHMARK})
#    add_subdirectory(test/test_helper)
#endif ()
#add_subdirectory(tools)
# endif ()

# Link extensions which require static linking.
# foreach(ext IN LISTS STATICALLY_LINKED_EXTENSIONS)
#     target_link_libraries(kuzu PRIVATE "kuzu_${ext}_static_extension")
#     target_link_libraries(kuzu_shared PRIVATE "kuzu_${ext}_static_extension")
# endforeach()
