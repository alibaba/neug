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
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils)


include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(src)

include(CTest)
enable_testing()

if (BUILD_TEST)
    add_subdirectory(test)
endif()
