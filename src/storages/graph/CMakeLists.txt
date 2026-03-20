
file(GLOB_RECURSE RT_MUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(neug_property_graph OBJECT ${RT_MUTABLE_GRAPH_SRC_FILES})
add_dependencies(neug_property_graph neug_plan_proto)
