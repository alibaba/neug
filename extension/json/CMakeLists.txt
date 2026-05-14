# Collect source files
file(GLOB JSON_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
set(JSON_EXTENSION_OBJECT_FILES ${JSON_EXTENSION_SOURCES})

# Use the extension build system (should be called before add_subdirectory(test))
build_extension_lib("json")

# Set include directories
target_include_directories(neug_json_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR})

if(ARROW_INCLUDE_DIRS)
    target_include_directories(neug_json_extension PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
endif()
target_include_directories(neug_json_extension PRIVATE SYSTEM ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)

# Link Arrow components directly. When Arrow is built from source, prefer
# object libraries to avoid undefined-symbol errors from demand-driven
# archive scanning. The extension is loaded with RTLD_LOCAL (see
# extension.cc) so its Arrow globals stay isolated from libneug.so's copy.
if(TARGET arrow_dataset_objlib)
    set(_EXT_JSON_DATASET_LIB arrow_dataset_objlib)
else()
    set(_EXT_JSON_DATASET_LIB ${ARROW_DATASET_LIB})
endif()
target_link_libraries(neug_json_extension PRIVATE
    neug
    ${ARROW_BASE_LIB}
    ${ARROW_ACERO_LIB}
    ${_EXT_JSON_DATASET_LIB}
)

# Build test by compiling extension sources directly (no link to neug_json_extension)
if(BUILD_TEST)
    add_extension_test(NAME json
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/json_test.cpp
        EXTRA_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include
    )
endif()
