# NeuG Node.js Bindings - CMake Build Configuration
#
# This file is built as a subdirectory of the main NeuG CMake project.
# It requires neug_main and neug_proto targets to exist.
#
# Build from project root:
#   mkdir -p build && cd build
#   cmake .. -DBUILD_NODEJS=ON -DCMAKE_BUILD_TYPE=Release
#   make -j$(nproc) neug_node_bind

# Find Node.js headers by querying the node executable for its install prefix
find_program(NODE_EXECUTABLE NAMES node nodejs)
if (NOT NODE_EXECUTABLE)
  message(FATAL_ERROR "Cannot find Node.js executable. Please install Node.js or set PATH.")
endif()

execute_process(
  COMMAND ${NODE_EXECUTABLE} -p "require('path').resolve(process.execPath, '..', '..', 'include', 'node')"
  OUTPUT_VARIABLE NODE_INCLUDE_HINT
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

find_path(NODE_INCLUDE_DIR node_api.h
  HINTS ${NODE_INCLUDE_HINT} /usr/include/node /usr/local/include/node
  PATH_SUFFIXES node
)
if (NOT NODE_INCLUDE_DIR)
  message(FATAL_ERROR "Cannot find Node.js headers (node_api.h). Hint path: ${NODE_INCLUDE_HINT}")
endif()

# Find node-addon-api include path (if installed via npm)
execute_process(
  COMMAND ${NODE_EXECUTABLE} -p "require('node-addon-api').include"
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

find_package(OpenSSL REQUIRED)
if (WIN32)
  set(NODE_IMPORT_LIB "" CACHE FILEPATH
    "Path to the Node.js import library (node.lib)")
  if (NOT NODE_IMPORT_LIB OR NOT EXISTS "${NODE_IMPORT_LIB}")
    message(FATAL_ERROR
      "Cannot find the Node.js import library. Set NODE_IMPORT_LIB to node.lib.")
  endif()

  # Protobuf namespace-scope default instances are not exported from neug.dll.
  # Link the generated objects into the addon, matching the Windows Python binding.
  target_link_libraries(neug_node_bind PRIVATE
    ${NEUG_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${Protobuf_LIBRARIES}
    $<TARGET_OBJECTS:neug_proto>
    OpenSSL::SSL
    OpenSSL::Crypto
    "${NODE_IMPORT_LIB}"
  )
else()
  target_link_libraries(neug_node_bind PRIVATE
    ${NEUG_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${Protobuf_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
  )
endif()

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

# Set RPATH so neug_node_bind.node can find libneug.so / libneug.dylib
# in the same directory (build/Release/<platform>/). Package builds retain
# CMake's build-tree RPATHs until dependencies are bundled. Those paths identify
# the exact libraries selected by the linker; the platform bundle step copies them
# into the package and replaces non-portable RPATHs before npm pack runs.
if(APPLE)
  set(_neug_node_rpath "@loader_path")
elseif(NOT WIN32)
  set(_neug_node_rpath "$ORIGIN")
endif()

if(NOT WIN32)
  set_target_properties(neug_node_bind PROPERTIES
    BUILD_RPATH "${_neug_node_rpath}"
    INSTALL_RPATH "${_neug_node_rpath}"
    BUILD_WITH_INSTALL_RPATH FALSE
  )
  set_property(TARGET neug APPEND PROPERTY BUILD_RPATH "${_neug_node_rpath}")
else()
  # Windows has no RPATH. Keep neug.dll beside the addon so the loader can find it.
  add_custom_command(TARGET neug_node_bind POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:neug> $<TARGET_FILE_DIR:neug_node_bind>)
endif()
