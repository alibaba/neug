file(GLOB EXAMPLE_INDEX_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
set(EXAMPLE_INDEX_EXTENSION_OBJECT_FILES ${EXAMPLE_INDEX_EXTENSION_SOURCES})

build_extension_lib("example_index")

target_include_directories(neug_example_index_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(neug_example_index_extension PRIVATE neug)
