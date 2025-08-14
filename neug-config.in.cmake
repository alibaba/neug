# - Config file for the Flex package
#
# It defines the following variables
#
#  NEUG_INCLUDE_DIR         - include directory for neug
#  NEUG_INCLUDE_DIRS        - include directories for neug
#  NEUG_LIBRARIES           - libraries to link against

set(NEUG_HOME "${CMAKE_CURRENT_LIST_DIR}/../../..")
include("${CMAKE_CURRENT_LIST_DIR}/neug-targets.cmake")

set(NEUG_LIBRARIES @NEUG_LIBRARIES@)
set(NEUG_INCLUDE_DIR "${NEUG_HOME}/include")
set(NEUG_INCLUDE_DIRS "${NEUG_INCLUDE_DIR}")

find_package(Threads REQUIRED)
find_package(Arrow REQUIRED)
find_package(gflags REQUIRED)

if (@WITH_MIMALLOC@)
    find_package(mimalloc 2.0 REQUIRED)
endif()

add_definitions(-DRAPIDJSON_HAS_CXX11=1)
add_definitions(-DRAPIDJSON_HAS_STDSTRING=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RVALUE_REFS=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RANGE_FOR=1)

include(FindPackageMessage)
find_package_message(neug
    "Found NeuG: ${CMAKE_CURRENT_LIST_FILE} (found version \"@NEUG_VERSION@\")"
    "Flex version: @NEUG_VERSION@\nFlex libraries: ${NEUG_LIBRARIES}, include directories: ${NEUG_INCLUDE_DIRS}"
)
