# HTTPFS Extension for NeuG
# Provides remote-filesystem support: S3-compatible object storage
# (AWS S3, Alibaba OSS, MinIO, etc.) plus HTTP/HTTPS file access.

# Find required dependencies
find_package(CURL REQUIRED)

# Collect source files for extension build
file(GLOB HTTPFS_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
set(HTTPFS_EXTENSION_OBJECT_FILES ${HTTPFS_EXTENSION_SOURCES})

# Use the extension build system
build_extension_lib("httpfs")

# Set include directories
target_include_directories(neug_httpfs_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CURL_INCLUDE_DIRS})

if(ARROW_INCLUDE_DIRS)
    target_include_directories(neug_httpfs_extension PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
endif()

# The S3 filesystem support (arrow/filesystem/s3fs.h) is compiled into
# ARROW_BASE_LIB when ARROW_S3=ON, so no additional Arrow component is needed.
# cURL is required for HTTP/HTTPS filesystem support.
# IMPORTANT: neug (shared library) MUST come AFTER all Arrow static archives.
# See extension/parquet/CMakeLists.txt for detailed explanation.
target_link_libraries(neug_httpfs_extension PRIVATE
    ${ARROW_BASE_LIB}
    ${ARROW_BUNDLED_DEPS_LIB}
    ${CURL_LIBRARIES}
    neug
)

if(BUILD_TEST)
    add_extension_test(NAME httpfs
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/s3_test.cpp
                  ${CMAKE_CURRENT_SOURCE_DIR}/tests/http_test.cpp
        EXTRA_LINK_LIBS ${ARROW_BASE_LIB} ${CURL_LIBRARIES}
        EXTRA_INCLUDE_DIRS ${CURL_INCLUDE_DIRS}
    )
endif()
