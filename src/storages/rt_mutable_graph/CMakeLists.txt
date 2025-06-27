
file(GLOB_RECURSE RT_MUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
if (NOT BUILD_ODPS_FRAGMENT_LOADER)
        # exclude odps_fragment_loader.cc
        message(STATUS "exclude odps_fragment_loader.cc")
        file(GLOB_RECURSE RT_MUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
        list(REMOVE_ITEM RT_MUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/loader/odps_fragment_loader.cc")
endif()
if (BUILD_ALL_IN_ONE)
        add_library(neug_rt_mutable_graph OBJECT ${RT_MUTABLE_GRAPH_SRC_FILES})
else()
        add_library(neug_rt_mutable_graph SHARED ${RT_MUTABLE_GRAPH_SRC_FILES})
        target_link_libraries(neug_rt_mutable_graph neug_utils neug_grape_lite ${CMAKE_THREAD_LIBS_INIT})
install_neug_target(neug_rt_mutable_graph)
endif()

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/src/storages
        FILES_MATCHING
        PATTERN "*.h"
)
