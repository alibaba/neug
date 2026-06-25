# Collect source files
file(GLOB PARQUET_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

set(PARQUET_EXTENSION_OBJECT_FILES ${PARQUET_EXTENSION_SOURCES})

# Resolve OpenSSL for the link step.
#
# Even though the parquet extension does NOT make any S3/HTTP calls itself,
# enabling ARROW_S3=ON in the project-wide Arrow build pulls AWS SDK code
# (which references OpenSSL OCSP / BIO / EC / RSA / X509 APIs) into
# arrow_bundled_dependencies.a. The linker's demand-driven scan drags those
# .o files into the parquet extension, leaving symbols like OCSP_basic_verify
# undefined and breaking dlopen at LOAD time. Linking OpenSSL here satisfies
# those references; the AWS code paths are never actually executed in parquet.
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
# appears first and exports the same symbol (e.g. AWS SDK typeinfo), the linker
# binds against it instead of pulling the definition from the static archive.
# At runtime a DIFFERENT libneug.so (without ARROW_S3) may be loaded, causing
# "undefined symbol" errors.  Placing neug last ensures static archives provide
# their own definitions and the extension is self-contained.
#
# IMPORTANT (group): Arrow's bundled AWS C++ SDK static archives appear in
# arrow_static's INTERFACE_LINK_LIBRARIES *after* OpenSSL, but their .o files
# reference OpenSSL OCSP/EC/BIO symbols.  GNU ld does a single-pass scan and
# would skip libcrypto.a before those refs become undefined.  Even though
# Arrow's interface re-lists libcrypto.a at the very end, ld's --as-needed
# default elides .o files (e.g. ocsp_vfy.o) whose symbols only appear in
# already-processed archives.  --whole-archive forces every .o from libssl /
# libcrypto into the final extension, guaranteeing OCSP_basic_verify et al.
# are resolved.  Cost: ~3 MB binary size growth.
#
# On macOS, Apple ld64 does not recognise --whole-archive; it uses
# -force_load per archive instead.  However macOS ld performs two-pass
# symbol resolution, so the OCSP symbols get resolved without forcing.
# We therefore only apply the --whole-archive wrapper on non-Apple platforms.
target_link_libraries(neug_parquet_extension PRIVATE
    ${ARROW_BASE_LIB}
    ${_EXT_ARROW_DATASET_LIB}
    ${_EXT_ARROW_PARQUET_LIB}
    ${ARROW_BUNDLED_DEPS_LIB}
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--whole-archive>
    OpenSSL::SSL
    OpenSSL::Crypto
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--no-whole-archive>
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
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/parquet_test.cpp
        EXTRA_LINK_LIBS ${_parquet_test_dataset_lib} ${ARROW_PARQUET_LIB}
    )
endif()

