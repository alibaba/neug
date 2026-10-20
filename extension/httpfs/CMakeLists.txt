# HTTPFS Extension for NeuG
# Provides remote-filesystem support: S3-compatible object storage
# (AWS S3, Alibaba OSS, MinIO, etc.) plus HTTP/HTTPS file access.
#
# This extension is Arrow-free: the S3 protocol (SigV4 signing, ranged GET,
# ListObjectsV2, PutObject/multipart) is implemented directly on top of
# libcurl + OpenSSL. Parquet decoding lives in the parquet
# extension's adapter layer instead.

# Find required dependencies.
#
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

    # The static libcurl is configured with --with-openssl=<install prefix>
    # (scripts/install_deps.sh), so its object files reference that prefix's
    # OpenSSL ABI (1.1.1k). The generic find_package(OpenSSL) result must NOT
    # be used here: the top-level CMakeLists.txt resolves OpenSSL (honoring
    # OPENSSL_ROOT_DIR, which points at Homebrew openssl@3 on the macOS
    # runners) before the extensions run, and OpenSSL 3.x no longer exports
    # SSL_get_peer_certificate / EVP_PKEY_id etc., failing the link with
    # "Undefined symbols for architecture arm64". Resolve the OpenSSL that
    # pairs with this libcurl explicitly, bypassing the cached result.
    get_filename_component(_curl_prefix "${CURL_STATIC_LIB}" DIRECTORY)
    get_filename_component(_curl_prefix "${_curl_prefix}" DIRECTORY)
    find_library(CURL_OPENSSL_SSL_LIB
        NAMES libssl.a
        PATHS "${_curl_prefix}"
        PATH_SUFFIXES lib lib64
        NO_DEFAULT_PATH)
    find_library(CURL_OPENSSL_CRYPTO_LIB
        NAMES libcrypto.a
        PATHS "${_curl_prefix}"
        PATH_SUFFIXES lib lib64
        NO_DEFAULT_PATH)
    find_path(CURL_OPENSSL_INCLUDE_DIR
        NAMES openssl/ssl.h
        PATHS "${_curl_prefix}"
        PATH_SUFFIXES include
        NO_DEFAULT_PATH)
    if(CURL_OPENSSL_SSL_LIB AND CURL_OPENSSL_CRYPTO_LIB
            AND CURL_OPENSSL_INCLUDE_DIR)
        set(CURL_OPENSSL_LIBS
            ${CURL_OPENSSL_SSL_LIB} ${CURL_OPENSSL_CRYPTO_LIB})
        message(STATUS "httpfs: pairing static cURL with OpenSSL at ${_curl_prefix}")
    else()
        # Pairing failed: a static libcurl was found but its prefix has no
        # matching OpenSSL. find_library/find_path cache the misses as
        # "<VAR>-NOTFOUND"; clear them so a -NOTFOUND path is never handed to
        # target_include_directories below. The find_package(OpenSSL) fallback
        # then resolves headers and libraries from one consistent OpenSSL.
        unset(CURL_OPENSSL_SSL_LIB CACHE)
        unset(CURL_OPENSSL_CRYPTO_LIB CACHE)
        unset(CURL_OPENSSL_INCLUDE_DIR CACHE)
    endif()
    unset(_curl_prefix)

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
    # On macOS, a static libcurl references network proxy APIs such as
    # SCDynamicStoreCopyProxies from SystemConfiguration.framework.
    if(APPLE)
        find_library(SYSTEMCONFIGURATION_FRAMEWORK SystemConfiguration REQUIRED)
        message(STATUS "httpfs: linking SystemConfiguration.framework for static cURL")
        list(APPEND CURL_LIBRARIES ${SYSTEMCONFIGURATION_FRAMEWORK})
    endif()
else()
    message(STATUS "httpfs: static libcurl.a not found, falling back to shared cURL")
    find_package(CURL REQUIRED)
endif()
unset(_curl_search_paths)

# OpenSSL is required unconditionally: SigV4 signing uses EVP/HMAC APIs and
# TLS verification goes through the OpenSSL-backed cURL. When the static
# cURL's own OpenSSL was resolved above, use it for both headers and
# libraries so the extension never mixes two OpenSSL ABIs in one image.
if(NOT CURL_OPENSSL_LIBS)
    find_package(OpenSSL REQUIRED)
    set(CURL_OPENSSL_LIBS OpenSSL::SSL OpenSSL::Crypto)
    # Set the include dir alongside the libraries so the extension and its
    # tests compile against the SAME OpenSSL they link, rather than an unset
    # (or stale -NOTFOUND) CURL_OPENSSL_INCLUDE_DIR or headers retained from a
    # different OpenSSL installation.
    set(CURL_OPENSSL_INCLUDE_DIR "${OPENSSL_INCLUDE_DIR}")
endif()

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
    ${CURL_INCLUDE_DIRS}
    ${CURL_OPENSSL_INCLUDE_DIR})

# SigV4 signing and TLS use OpenSSL directly; cURL provides the transport.
target_link_libraries(neug_httpfs_extension PRIVATE
    ${CURL_LIBRARIES}
    ${CURL_OPENSSL_LIBS}
    neug
)

# The C++ test suite is not built on Windows (consistent with the Windows CI
# test policy; the tests use POSIX socket headers).
if(BUILD_TEST AND NOT WIN32)
    add_extension_test(NAME httpfs
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/s3_test.cc
                  ${CMAKE_CURRENT_SOURCE_DIR}/tests/http_test.cc
        EXTRA_LINK_LIBS ${CURL_LIBRARIES} ${CURL_OPENSSL_LIBS}
        EXTRA_INCLUDE_DIRS ${CURL_INCLUDE_DIRS} ${CURL_OPENSSL_INCLUDE_DIR}
    )
    if(TARGET neug_parquet_carquet_impl)
        target_link_libraries(httpfs_extension_test PRIVATE neug_parquet_carquet_impl)
        target_compile_definitions(httpfs_extension_test PRIVATE NEUG_HTTPFS_CARQUET_TEST=1)
    endif()
endif()
