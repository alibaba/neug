# - Config file for the Flex package
#
# It defines the following variables
#
#  NEXG_INCLUDE_DIR         - include directory for nexg
#  NEXG_INCLUDE_DIRS        - include directories for nexg
#  NEXG_LIBRARIES           - libraries to link against

set(NEXG_HOME "${CMAKE_CURRENT_LIST_DIR}/../../..")
include("${CMAKE_CURRENT_LIST_DIR}/nexg-targets.cmake")

set(NEXG_LIBRARIES nexg::nexg_utils nexg::nexg_rt_mutable_graph nexg::nexg_graph_db nexg::nexg_bsp nexg::nexg_immutable_graph nexg::nexg_plan_proto)
set(NEXG_INCLUDE_DIR "${NEXG_HOME}/include")
set(NEXG_INCLUDE_DIRS "${NEXG_INCLUDE_DIR}")

include(FindPackageMessage)
find_package_message(nexg
    "Found NexG: ${CMAKE_CURRENT_LIST_FILE} (found version \"@NEXG_VERSION@\")"
    "Flex version: @NEXG_VERSION@\nFlex libraries: ${NEXG_LIBRARIES}, include directories: ${NEXG_INCLUDE_DIRS}"
)
