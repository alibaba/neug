set(FTS_EXTENSION_OBJECT_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_function.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_index.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_index_scan.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_tokenizer.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_sqlite.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_extension.cc")

include(${CMAKE_SOURCE_DIR}/cmake/BuildSQLiteAsThirdParty.cmake)
find_package(ZLIB REQUIRED)

if(NOT EXISTS "${CMAKE_SOURCE_DIR}/third_party/cppjieba/include/cppjieba/Jieba.hpp" OR
   NOT EXISTS "${CMAKE_SOURCE_DIR}/third_party/cppjieba/deps/limonp/include/limonp/StringUtil.hpp")
    message(FATAL_ERROR
        "cppjieba submodule is missing. Run: git submodule update --init "
        "--recursive third_party/cppjieba")
endif()

set(NEUG_CPPJIEBA_PATCH "${CMAKE_SOURCE_DIR}/third_party/cppjieba.patch")
execute_process(
    COMMAND git apply --check "${NEUG_CPPJIEBA_PATCH}"
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/third_party/cppjieba"
    RESULT_VARIABLE NEUG_CPPJIEBA_PATCH_CHECK
    OUTPUT_QUIET ERROR_QUIET)
if(NEUG_CPPJIEBA_PATCH_CHECK EQUAL 0)
    execute_process(
        COMMAND git apply "${NEUG_CPPJIEBA_PATCH}"
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/third_party/cppjieba"
        RESULT_VARIABLE NEUG_CPPJIEBA_PATCH_RESULT)
    if(NOT NEUG_CPPJIEBA_PATCH_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to apply cppjieba.patch")
    endif()
else()
    execute_process(
        COMMAND git apply --reverse --check "${NEUG_CPPJIEBA_PATCH}"
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/third_party/cppjieba"
        RESULT_VARIABLE NEUG_CPPJIEBA_PATCH_APPLIED
        OUTPUT_QUIET ERROR_QUIET)
    if(NOT NEUG_CPPJIEBA_PATCH_APPLIED EQUAL 0)
        message(FATAL_ERROR "cppjieba.patch does not apply cleanly")
    endif()
endif()

if(NOT TARGET neug_cppjieba)
    add_library(neug_cppjieba INTERFACE)
    target_include_directories(neug_cppjieba SYSTEM INTERFACE
        "${CMAKE_SOURCE_DIR}/third_party/cppjieba/include"
        "${CMAKE_SOURCE_DIR}/third_party/cppjieba/deps/limonp/include")
endif()

build_extension_lib("fts")

target_include_directories(neug_fts_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(neug_fts_extension PRIVATE
    neug
    neug_sqlite3
    neug_cppjieba
    ZLIB::ZLIB
    ${Protobuf_LIBRARIES})
add_dependencies(neug_fts_extension neug_sqlite3_amalgamation)

if(BUILD_TEST)
    add_extension_test(
        NAME fts
        TEST_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/tests/fts_extension_test.cc"
        EXTRA_LINK_LIBS neug_sqlite3 neug_cppjieba ZLIB::ZLIB)
    add_dependencies(fts_extension_test neug_fts_extension)
endif()
