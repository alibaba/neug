
file(GLOB_RECURSE RT_MUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
if (NOT BUILD_ODPS_FRAGMENT_LOADER)
        # exclude odps_fragment_loader.cc
        message(STATUS "exclude odps_fragment_loader.cc")
        file(GLOB_RECURSE RT_MUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
        list(REMOVE_ITEM RT_MUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/loader/odps_fragment_loader.cc")
endif()
add_library(neug_property_graph OBJECT ${RT_MUTABLE_GRAPH_SRC_FILES})
add_dependencies(neug_property_graph neug_plan_proto)
