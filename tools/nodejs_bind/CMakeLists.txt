# NeuG Node.js Bindings - CMake Build Configuration
#
# This file is built as a subdirectory of the main NeuG CMake project.
# It requires neug_main and neug_proto targets to exist.
#
# Build from project root:
#   mkdir -p build && cd build
#   cmake .. -DBUILD_NODEJS=ON -DCMAKE_BUILD_TYPE=Release
#   make -j$(nproc) neug_node_bind

# Find Node.js include headers
find_path(NODE_INCLUDE_DIR node_api.h
  HINTS /usr/include/node /usr/local/include/node
  PATH_SUFFIXES node
)
if (NOT NODE_INCLUDE_DIR)
  message(FATAL_ERROR "Cannot find Node.js headers (node_api.h)")
endif()

# Find node-addon-api include path (if installed via npm)
execute_process(
  COMMAND node -p "require('node-addon-api').include"
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  OUTPUT_VARIABLE NODE_ADDON_API_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)
string(REPLACE "\"" "" NODE_ADDON_API_DIR ${NODE_ADDON_API_DIR})

file(GLOB_RECURSE SOURCE_CPP "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")

# Match Python binding: disable LTO to avoid GCC ICE with large static libs
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION FALSE)

add_library(neug_node_bind MODULE ${SOURCE_CPP})
set_target_properties(neug_node_bind PROPERTIES
  PREFIX ""
  SUFFIX ".node"
)

target_include_directories(neug_node_bind PRIVATE
  ${NODE_INCLUDE_DIR}
  ${NODE_ADDON_API_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler
)

add_dependencies(neug_node_bind neug_proto)

set(NEUG_LIBRARIES neug)
if (BUILD_HTTP_SERVER)
    set(NEUG_LIBRARIES ${NEUG_LIBRARIES})
endif()

target_link_libraries(neug_node_bind PRIVATE
  ${NEUG_LIBRARIES}
  ${GLOG_LIBRARIES}
  ${Protobuf_LIBRARIES}
)

target_compile_definitions(neug_node_bind PRIVATE
  NAPI_VERSION=8
  NODE_ADDON_API_DISABLE_DEPRECATED
  BUILDING_NODE_EXTENSION
)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(neug_node_bind PRIVATE -Wno-vla-extension)
endif()

# Hide protobuf/abseil/arrow symbols to avoid conflicts
if (COMMAND neug_apply_symbol_visibility)
  neug_apply_symbol_visibility(neug_node_bind)
endif()

# macOS: allow undefined symbols (Node.js resolves them at runtime)
if (APPLE)
  set_target_properties(neug_node_bind PROPERTIES
    LINK_FLAGS "-undefined dynamic_lookup"
  )
endif()
