# HTTPFS Extension for NeuG
# Provides remote-filesystem support: S3-compatible object storage
# (AWS S3, Alibaba OSS, MinIO, etc.) plus HTTP/HTTPS file access.
#
# This extension is Arrow-free: the S3 protocol (SigV4 signing, ranged GET,
# ListObjectsV2, PutObject/multipart) is implemented directly on top of
# libcurl + OpenSSL. Parquet decoding lives in the parquet
# extension's adapter layer instead.

# Reuse the core's OpenSSL targets. Never resolve a second OpenSSL for cURL.
find_package(OpenSSL REQUIRED)

# Prefer a static libcurl from the install prefix (typically /opt/neug, built
# by scripts/install_deps.sh) so the extension does NOT depend on the system
# libcurl at runtime.
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

    # A static libcurl pulls in zlib/pthread/dl symbols itself.
    # Link them after libcurl.a so the linker can resolve those references.
    # pthread/dl are POSIX-only; MSVC resolves these inside the CRT.
    find_package(ZLIB REQUIRED)
    set(CURL_LIBRARIES
        ${CURL_STATIC_LIB}
        ZLIB::ZLIB)
    if(NOT WIN32)
        list(APPEND CURL_LIBRARIES pthread dl)
    endif()
    # Static cURL uses proxy APIs from SystemConfiguration and CFRelease from
    # CoreFoundation.
    if(APPLE)
        find_library(SYSTEMCONFIGURATION_FRAMEWORK SystemConfiguration REQUIRED)
        find_library(COREFOUNDATION_FRAMEWORK CoreFoundation REQUIRED)
        list(APPEND CURL_LIBRARIES ${SYSTEMCONFIGURATION_FRAMEWORK} ${COREFOUNDATION_FRAMEWORK})
    endif()
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
    ${CMAKE_CURRENT_BINARY_DIR})
# An old cURL prefix may still contain OpenSSL 1.1 headers. Keep both external
# include paths SYSTEM and put the selected OpenSSL first, including when the
# core already added its OpenSSL include directory as SYSTEM.
target_include_directories(neug_httpfs_extension SYSTEM BEFORE PRIVATE
    ${OPENSSL_INCLUDE_DIR} ${CURL_INCLUDE_DIRS})

# SigV4 signing and TLS use OpenSSL directly; cURL provides the transport.
target_link_libraries(neug_httpfs_extension PRIVATE
    ${CURL_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    neug
)

# The C++ test suite is not built on Windows (consistent with the Windows CI
# test policy; the tests use POSIX socket headers).
if(BUILD_TEST AND NOT WIN32)
    add_extension_test(NAME httpfs
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/s3_test.cc
                  ${CMAKE_CURRENT_SOURCE_DIR}/tests/http_test.cc
        EXTRA_LINK_LIBS ${CURL_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto
        EXTRA_INCLUDE_DIRS ${OPENSSL_INCLUDE_DIR} ${CURL_INCLUDE_DIRS}
    )
    if(TARGET neug_parquet_carquet_impl)
        target_link_libraries(httpfs_extension_test PRIVATE neug_parquet_carquet_impl)
        target_compile_definitions(httpfs_extension_test PRIVATE NEUG_HTTPFS_CARQUET_TEST=1)
    endif()
endif()
