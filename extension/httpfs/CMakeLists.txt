# HTTPFS Extension for NeuG
# Provides remote-filesystem support: S3-compatible object storage
# (AWS S3, Alibaba OSS, MinIO, etc.) plus HTTP/HTTPS file access.

# Find required dependencies.
#
# Prefer a static libcurl from the install prefix (typically /opt/neug, built
# by scripts/install_deps.sh) so the extension does NOT depend on the system
# libcurl at runtime.  This avoids a TLS symbol conflict between Arrow's
# bundled aws-lc/BoringSSL (statically linked into the extension) and the
# system OpenSSL 3.0 that the system libcurl loads, which segfaults inside
# OPENSSL_sk_value() during cURL SSL initialization on the first HTTPS
# request.
set(_curl_search_paths
    ${CMAKE_PREFIX_PATH}
    $ENV{NEUG_HOME}
    /opt/neug)
find_library(CURL_STATIC_LIB
    NAMES libcurl.a
    PATHS ${_curl_search_paths}
    PATH_SUFFIXES lib lib64
    NO_DEFAULT_PATH)
if(CURL_STATIC_LIB)
    message(STATUS "httpfs: using static cURL ${CURL_STATIC_LIB}")
    find_path(CURL_INCLUDE_DIRS curl/curl.h
        PATHS ${_curl_search_paths}
        PATH_SUFFIXES include
        NO_DEFAULT_PATH)
    # A static libcurl pulls in OpenSSL/zlib/pthread/dl symbols itself.
    # Link them after libcurl.a so the linker can resolve those references.
    find_package(OpenSSL REQUIRED)
    find_package(ZLIB REQUIRED)
    set(CURL_LIBRARIES
        ${CURL_STATIC_LIB}
        OpenSSL::SSL
        OpenSSL::Crypto
        ZLIB::ZLIB
        pthread
        dl)
else()
    message(STATUS "httpfs: static libcurl.a not found, falling back to shared cURL")
    find_package(CURL REQUIRED)
endif()
unset(_curl_search_paths)

# Collect source files for extension build
file(GLOB HTTPFS_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
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
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/s3_test.cc
                  ${CMAKE_CURRENT_SOURCE_DIR}/tests/http_test.cc
        EXTRA_LINK_LIBS ${ARROW_BASE_LIB} ${CURL_LIBRARIES}
        EXTRA_INCLUDE_DIRS ${CURL_INCLUDE_DIRS}
    )
endif()
