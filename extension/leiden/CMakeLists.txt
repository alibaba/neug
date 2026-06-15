# Collect source files
file(GLOB LEIDEN_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
set(LEIDEN_EXTENSION_OBJECT_FILES ${LEIDEN_EXTENSION_SOURCES})

# Use the extension build system
build_extension_lib("leiden")

# Set include directories
target_include_directories(neug_leiden_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(neug_leiden_extension PRIVATE
    neug
)

# Build test
if(BUILD_TEST)
    add_extension_test(NAME leiden
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/leiden_test.cpp
    )
endif()
