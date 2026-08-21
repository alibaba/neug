# Collect source files
file(GLOB PARQUET_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")

set(PARQUET_EXTENSION_OBJECT_FILES ${PARQUET_EXTENSION_SOURCES})

# OpenSSL is linked for robustness: a system-installed Arrow (CI path) may
# have been built with cloud filesystems that reference OpenSSL. Since the
# project's own Arrow build has ARROW_S3=OFF (the S3 protocol lives in the
# httpfs extension's curl-based client), no AWS SDK code is pulled in here.
find_package(OpenSSL REQUIRED)

# Use the extension build system (should be called before add_subdirectory(test))
build_extension_lib("parquet")

# Set include directories
target_include_directories(neug_parquet_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)

if(ARROW_INCLUDE_DIRS)
    target_include_directories(neug_parquet_extension PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
endif()

# Link only the Arrow components this extension actually needs, instead of
# the full ${ARROW_LIB} (which includes acero, etc.).
#
# When Arrow is built from source as static archives, prefer object
# libraries (e.g., arrow_dataset_objlib) to avoid undefined-symbol errors
# from the linker's demand-driven archive scan (it may omit .o files whose
# symbols are only referenced indirectly, e.g. ParquetFragmentScanOptions).
#
# The extension is loaded with RTLD_LOCAL (see extension.cc) so its Arrow
# global objects stay isolated from libneug.so's copy, preventing the
# double-free / heap corruption that RTLD_GLOBAL would cause.
if(TARGET arrow_dataset_objlib)
    set(_EXT_ARROW_DATASET_LIB arrow_dataset_objlib)
else()
    set(_EXT_ARROW_DATASET_LIB ${ARROW_DATASET_LIB})
endif()
if(TARGET parquet_objlib)
    set(_EXT_ARROW_PARQUET_LIB parquet_objlib)
elseif(TARGET arrow_parquet_objlib)
    set(_EXT_ARROW_PARQUET_LIB arrow_parquet_objlib)
else()
    set(_EXT_ARROW_PARQUET_LIB ${ARROW_PARQUET_LIB})
endif()
# IMPORTANT: neug (shared library) MUST come AFTER all Arrow static archives.
# GNU ld resolves undefined symbols from shared libraries eagerly; if libneug.so
# appears first and exports the same symbol, the linker binds against it
# instead of pulling the definition from the static archive. Placing neug
# last ensures static archives provide their own definitions and the
# extension is self-contained.
target_link_libraries(neug_parquet_extension PRIVATE
    ${ARROW_BASE_LIB}
    ${_EXT_ARROW_DATASET_LIB}
    ${_EXT_ARROW_PARQUET_LIB}
    ${ARROW_BUNDLED_DEPS_LIB}
    OpenSSL::SSL
    OpenSSL::Crypto
    neug
    ${Protobuf_LIBRARIES}
)

# Build test by compiling extension sources directly (no link to neug_parquet_extension)
if(BUILD_TEST)
    # arrow_dataset_objlib only exists when Arrow is built from source.
    # Under system Arrow (CI=ON), fall back to ARROW_DATASET_LIB instead.
    if(TARGET arrow_dataset_objlib)
        set(_parquet_test_dataset_lib arrow_dataset_objlib)
    elseif(ARROW_DATASET_LIB)
        set(_parquet_test_dataset_lib ${ARROW_DATASET_LIB})
    else()
        set(_parquet_test_dataset_lib "")
    endif()
    add_extension_test(NAME parquet
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/parquet_test.cc
                  ${CMAKE_CURRENT_SOURCE_DIR}/tests/arrow_fs_adapter_test.cc
        EXTRA_LINK_LIBS ${_parquet_test_dataset_lib} ${ARROW_PARQUET_LIB}
    )
endif()

