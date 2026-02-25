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

# Link libraries
# Core Arrow symbols are already in neug_libraries (which links arrow_static,
# arrow_dataset_static, etc.). However, the linker strips unreferenced symbols
# from static archives — both arrow::json::* and arrow::dataset::Json* symbols
# are stripped because neug_libraries itself doesn't use them.
#
# Link the Arrow OBJECT libraries directly to include these symbols:
# - arrow_json: provides arrow::json::TableReader, ReadOptions, ParseOptions, etc.
# - arrow_dataset_objlib: provides arrow::dataset::JsonFileFormat,
#   JsonFragmentScanOptions, and other dataset symbols used by the extension.
#
# Using OBJECT library targets (resolved at build time) instead of file(GLOB)
# for .o files (which fails on clean builds since GLOB runs at configure time).
target_link_libraries(neug_json_extension PRIVATE
    arrow_json
    arrow_dataset_objlib
    ${GLOG_LIBRARIES}
    neug_libraries
)
