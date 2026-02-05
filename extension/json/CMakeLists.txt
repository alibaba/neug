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
if(ARROW_LIB)
    target_link_libraries(neug_json_extension PRIVATE ${ARROW_LIB})
endif()
target_link_libraries(neug_json_extension PRIVATE
    ${Protobuf_LIBRARIES}
    ${GLOG_LIBRARIES}
    neug_libraries
)
