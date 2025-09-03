file(GLOB_RECURSE NEUG_MAIN_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
                                        "${CMAKE_CURRENT_SOURCE_DIR}/app/*.cc")

if (BUILD_ALL_IN_ONE)
    add_library(neug_main OBJECT ${NEUG_MAIN_SRC_FILES})
    add_dependencies(neug_main neug_plan_proto)
    set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_main> PARENT_SCOPE)
        
else()
    add_library(neug_main SHARED ${NEUG_MAIN_SRC_FILES})
    target_link_libraries(neug_main PUBLIC neug_utils neug_execution neug_compiler_shared)
    install_without_export_neug_target(neug_main)
endif()
