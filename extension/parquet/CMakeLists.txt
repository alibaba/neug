# Collect source files
file(GLOB PARQUET_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
set(PARQUET_EXTENSION_OBJECT_FILES ${PARQUET_EXTENSION_SOURCES})

# Use the extension build system (should be called before add_subdirectory(test))
build_extension_lib("parquet")

# Set include directories
target_include_directories(neug_parquet_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR})

if(ARROW_INCLUDE_DIRS)
    target_include_directories(neug_parquet_extension PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
endif()

# Link libraries
# Core Arrow symbols are already in neug (which links arrow_static,
# arrow_dataset_static, etc.). However, the linker strips unreferenced symbols
# from static archives — both arrow::parquet::* and arrow::dataset::Parquet* symbols
# are stripped because neug itself doesn't use them.
#
# Link the Arrow OBJECT libraries directly to include these symbols:
# - arrow_dataset_objlib: provides arrow::dataset::ParquetFileFormat,
#   ParquetFragmentScanOptions, and other dataset symbols used by the extension.
#
# Additionally, link ARROW_PARQUET_LIB (the Parquet static library) with
# arrow::parquet::* symbols (e.g., ParquetFileReader, ArrowReaderProperties).
#
# Using OBJECT library targets (resolved at build time) instead of file(GLOB)
# for .o files (which fails on clean builds since GLOB runs at configure time).
target_link_libraries(neug_parquet_extension PRIVATE
    neug
)

# Build test by compiling extension sources directly (no link to neug_parquet_extension)
if(BUILD_TEST)
    add_extension_test(NAME parquet
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/parquet_test.cpp
        EXTRA_LINK_LIBS arrow_dataset_objlib ${ARROW_PARQUET_LIB}
    )
endif()

