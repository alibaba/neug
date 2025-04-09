file(GLOB_RECURSE BSP_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
  
add_library(nexg_bsp SHARED ${BSP_SRC_FILES})
target_link_libraries(nexg_bsp ${LIBGRAPELITE_LIBRARIES})
install_nexg_target(nexg_bsp)