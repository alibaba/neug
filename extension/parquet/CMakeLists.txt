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
    # Follow the same submodule + adjacent patch convention as cppjieba.
    # Keep upstream options local to this function and use its native target.
    function(build_carquet_as_third_party)
        set(_carquet_source "${CMAKE_SOURCE_DIR}/third_party/carquet")
        set(_carquet_patch "${CMAKE_SOURCE_DIR}/third_party/carquet.patch")
        if(NOT EXISTS "${_carquet_source}/CMakeLists.txt")
            message(FATAL_ERROR
                "Carquet submodule is missing. Run: git submodule update --init "
                "--recursive third_party/carquet")
        endif()
        # Recheck after a patch edit or a checkout that restores the source.
        set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
            "${_carquet_patch}" "${_carquet_source}/src/writer/page_writer.c")
        execute_process(
            COMMAND git apply --check "${_carquet_patch}"
            WORKING_DIRECTORY "${_carquet_source}"
            RESULT_VARIABLE _patch_check OUTPUT_QUIET ERROR_QUIET)
        if(_patch_check EQUAL 0)
            execute_process(
                COMMAND git apply "${_carquet_patch}"
                WORKING_DIRECTORY "${_carquet_source}"
                RESULT_VARIABLE _patch_result)
            if(NOT _patch_result EQUAL 0)
                message(FATAL_ERROR "Failed to apply carquet.patch")
            endif()
        else()
            execute_process(
                COMMAND git apply --reverse --check "${_carquet_patch}"
                WORKING_DIRECTORY "${_carquet_source}"
                RESULT_VARIABLE _patch_applied OUTPUT_QUIET ERROR_QUIET)
            if(NOT _patch_applied EQUAL 0)
                message(FATAL_ERROR "carquet.patch does not apply cleanly")
            endif()
        endif()

        # Require the existing system codecs instead of fetching another copy.
        find_package(PkgConfig REQUIRED)
        pkg_check_modules(NEUG_CARQUET_ZSTD REQUIRED libzstd)
        pkg_check_modules(NEUG_CARQUET_LZ4 REQUIRED IMPORTED_TARGET liblz4)
        find_package(ZLIB REQUIRED)
        # Upstream skips LZ4 resolution on reconfigure when LZ4_FOUND is cached,
        # leaving a bare library name. Supply a target that retains its path.
        set(LZ4_FOUND TRUE)
        set(LZ4_LIBRARIES PkgConfig::NEUG_CARQUET_LZ4)
        foreach(_option IN ITEMS BUILD_DEV BUILD_TESTS BUILD_EXAMPLES
                BUILD_BENCHMARKS BUILD_ARROW_CPP_BENCHMARK BUILD_INTEROP
                BUILD_CLI BUILD_SHARED BUILD_FUZZ BUILD_DOCS BUNDLE_DEPS
                NATIVE_ARCH)
            set(CARQUET_${_option} OFF)
        endforeach()
        set(CMAKE_DISABLE_FIND_PACKAGE_OpenMP TRUE)
        add_subdirectory("${_carquet_source}"
            "${CMAKE_BINARY_DIR}/third_party/carquet" EXCLUDE_FROM_ALL)
        set_target_properties(carquet PROPERTIES
            POSITION_INDEPENDENT_CODE ON COMPILE_WARNING_AS_ERROR OFF)
        # NeuG's warning-as-error policy does not apply to upstream C sources.
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(carquet PRIVATE -Wno-error)
        elseif(MSVC)
            target_compile_options(carquet PRIVATE /WX-)
        endif()
        add_library(neug::carquet ALIAS carquet)
    endfunction()
    build_carquet_as_third_party()

    add_executable(parquet_carquet_smoke_test
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/carquet_smoke_test.c)
    target_link_libraries(parquet_carquet_smoke_test PRIVATE neug::carquet)
    add_test(NAME parquet_carquet_smoke_test COMMAND parquet_carquet_smoke_test)

    add_executable(parquet_carquet_multibatch_test
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/carquet_multibatch_test.c)
    target_link_libraries(parquet_carquet_multibatch_test PRIVATE neug::carquet)
    add_test(NAME parquet_carquet_multibatch_test COMMAND parquet_carquet_multibatch_test)

    add_executable(parquet_carquet_capabilities_test
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/carquet_capabilities_test.c)
    target_link_libraries(parquet_carquet_capabilities_test PRIVATE neug::carquet)
    add_test(NAME parquet_carquet_capabilities_test COMMAND parquet_carquet_capabilities_test)

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
