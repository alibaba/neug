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

# Link only the Arrow components this extension actually needs, instead of
# the full ${ARROW_LIB} (which includes acero, etc.).
# This significantly reduces the extension binary size.
# Extension symbols are hidden by neug_apply_symbol_visibility (in
# build_extension_lib) to avoid conflicts with pyarrow.
target_link_libraries(neug_parquet_extension PRIVATE
    neug
    ${ARROW_BASE_LIB}
    ${ARROW_DATASET_LIB}
    ${ARROW_PARQUET_LIB}
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

