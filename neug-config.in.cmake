# - Config file for the Flex package
#
# It defines the following variables
#
#  NEUG_INCLUDE_DIR         - include directory for neug
#  NEUG_INCLUDE_DIRS        - include directories for neug
#  NEUG_LIBRARIES           - libraries to link against

set(NEUG_HOME "${CMAKE_CURRENT_LIST_DIR}/../../..")

include(CMakeFindDependencyMacro)

# Propagate dependencies required by the exported targets so consumers
# automatically pick them up when calling find_package(neug).
find_dependency(Threads)
find_dependency(ZLIB)
find_dependency(OpenSSL)
find_package(absl REQUIRED CONFIG)
find_dependency(Protobuf CONFIG)
set(_neug_glog_module_dir "${NEUG_HOME}/share/glog/cmake")
list(APPEND CMAKE_MODULE_PATH "${_neug_glog_module_dir}")
find_dependency(Unwind)

include("${CMAKE_CURRENT_LIST_DIR}/neug-targets.cmake")
if(TARGET protobuf::libprotobuf)
    set_property(TARGET neug::neug APPEND PROPERTY
        INTERFACE_LINK_LIBRARIES "protobuf::libprotobuf")
endif()


set(NEUG_LIBRARIES @NEUG_LIBRARIES@)
set(NEUG_INCLUDE_DIR "${NEUG_HOME}/include")
set(NEUG_INCLUDE_DIRS "${NEUG_INCLUDE_DIR}")
include_directories(${Protobuf_INCLUDE_DIRS})

add_definitions(-DRAPIDJSON_HAS_CXX11=1)
add_definitions(-DRAPIDJSON_HAS_STDSTRING=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RVALUE_REFS=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RANGE_FOR=1)

include(FindPackageMessage)
find_package_message(neug
    "Found NeuG: ${CMAKE_CURRENT_LIST_FILE} (found version \"@NEUG_VERSION@\")"
    "Flex version: @NEUG_VERSION@\nFlex libraries: ${NEUG_LIBRARIES}, include directories: ${NEUG_INCLUDE_DIRS}"
)
