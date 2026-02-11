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
# Core Arrow symbols are already in neug_libraries. Only link the Arrow JSON-specific
# object files (arrow::json::* and arrow::dataset::JsonFileFormat) that are NOT
# part of the core Arrow build in neug_libraries, to avoid duplicating the entire
# Arrow static library (~44MB) and bloating the extension binary.
set(ARROW_JSON_OBJ_DIR "${CMAKE_BINARY_DIR}/_deps/arrow-build/src/arrow/CMakeFiles/arrow_json.dir/json")
set(ARROW_DATASET_JSON_OBJ_DIR "${CMAKE_BINARY_DIR}/_deps/arrow-build/src/arrow/dataset/CMakeFiles/arrow_dataset_objlib.dir")
file(GLOB ARROW_JSON_OBJS "${ARROW_JSON_OBJ_DIR}/*.o")
file(GLOB ARROW_DATASET_JSON_OBJS "${ARROW_DATASET_JSON_OBJ_DIR}/file_json.cc.o")

target_link_libraries(neug_json_extension PRIVATE
    ${ARROW_JSON_OBJS}
    ${ARROW_DATASET_JSON_OBJS}
    ${Protobuf_LIBRARIES}
    ${GLOG_LIBRARIES}
    neug_libraries
)
