# Collect source files
file(GLOB LOUVAIN_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
set(LOUVAIN_EXTENSION_OBJECT_FILES ${LOUVAIN_EXTENSION_SOURCES})

# Use the extension build system
build_extension_lib("louvain")

# Set include directories
target_include_directories(neug_louvain_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(neug_louvain_extension PRIVATE
    neug
)

# Build test
if(BUILD_TEST)
    add_extension_test(NAME louvain
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/louvain_test.cpp
    )
endif()
