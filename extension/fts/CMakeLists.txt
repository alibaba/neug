set(FTS_EXTENSION_OBJECT_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_function.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_index.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_index_scan.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_tokenizer.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_sqlite.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/fts_extension.cc")

include(${CMAKE_SOURCE_DIR}/cmake/BuildSQLiteAsThirdParty.cmake)

if(NOT EXISTS "${CMAKE_SOURCE_DIR}/third_party/cppjieba/include/cppjieba/Jieba.hpp" OR
   NOT EXISTS "${CMAKE_SOURCE_DIR}/third_party/cppjieba/deps/limonp/include/limonp/StringUtil.hpp")
    message(FATAL_ERROR
        "cppjieba submodule is missing. Run: git submodule update --init "
        "--recursive third_party/cppjieba")
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
    ${Protobuf_LIBRARIES})
add_dependencies(neug_fts_extension neug_sqlite3_amalgamation)

add_custom_command(
    TARGET neug_fts_extension POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_SOURCE_DIR}/third_party/cppjieba/dict/jieba.dict.utf8"
        "$<TARGET_FILE_DIR:neug_fts_extension>/jieba.dict.utf8"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_SOURCE_DIR}/third_party/cppjieba/dict/hmm_model.utf8"
        "$<TARGET_FILE_DIR:neug_fts_extension>/hmm_model.utf8"
    VERBATIM)

if(BUILD_TEST)
    add_extension_test(
        NAME fts
        TEST_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/tests/fts_extension_test.cc"
        EXTRA_LINK_LIBS neug_sqlite3 neug_cppjieba)
    add_dependencies(fts_extension_test neug_fts_extension)
    add_custom_command(
        TARGET fts_extension_test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${CMAKE_SOURCE_DIR}/third_party/cppjieba/dict/jieba.dict.utf8"
            "$<TARGET_FILE_DIR:fts_extension_test>/jieba.dict.utf8"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${CMAKE_SOURCE_DIR}/third_party/cppjieba/dict/hmm_model.utf8"
            "$<TARGET_FILE_DIR:fts_extension_test>/hmm_model.utf8"
        VERBATIM)
endif()
