# - Config file for the Flex package
#
# It defines the following variables
#
#  NEUG_INCLUDE_DIR         - include directory for neug
#  NEUG_INCLUDE_DIRS        - include directories for neug
#  NEUG_LIBRARIES           - libraries to link against

set(NEUG_HOME "${CMAKE_CURRENT_LIST_DIR}/../../..")
include("${CMAKE_CURRENT_LIST_DIR}/neug-targets.cmake")

if (BUILD_ALL_IN_ONE)
    set(NEUG_LIBRARIES neug::neug_libraries)
else()
    set(NEUG_LIBRARIES neug::neug_utils neug::neug_rt_mutable_graph neug::neug_graph_db neug::neug_plan_proto neug::neug_grape_lite)
endif()
set(NEUG_INCLUDE_DIR "${NEUG_HOME}/include")
set(NEUG_INCLUDE_DIRS "${NEUG_INCLUDE_DIR}")

include(FindPackageMessage)
find_package_message(neug
    "Found NeuG: ${CMAKE_CURRENT_LIST_FILE} (found version \"@NEUG_VERSION@\")"
    "Flex version: @NEUG_VERSION@\nFlex libraries: ${NEUG_LIBRARIES}, include directories: ${NEUG_INCLUDE_DIRS}"
)
