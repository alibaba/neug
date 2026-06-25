file(GLOB INDEX_EXAMPLE_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
set(INDEX_EXAMPLE_EXTENSION_OBJECT_FILES ${INDEX_EXAMPLE_EXTENSION_SOURCES})

build_extension_lib("index_example")

target_include_directories(neug_index_example_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(neug_index_example_extension PRIVATE neug)

