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
# Core Arrow symbols are already in neug_libraries (which links arrow_static).
# However, the linker strips arrow::json::* symbols from neug_libraries because
# they are unreferenced there. Link the arrow_json OBJECT library directly to
# include these symbols in the extension. This avoids the previous approach of
# globbing for .o files (which fails on clean builds since file(GLOB) runs at
# configure time before Arrow is compiled).
target_link_libraries(neug_json_extension PRIVATE
    arrow_json
    ${Protobuf_LIBRARIES}
    ${GLOG_LIBRARIES}
    neug_libraries
)
