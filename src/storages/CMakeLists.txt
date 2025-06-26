
add_subdirectory(rt_mutable_graph)
set(NEUG_STORAGES_OBJFILES $<TARGET_OBJECTS:neug_rt_mutable_graph>)
if (BUILD_METADATA)
    add_subdirectory(metadata)
    set(NEUG_STORAGES_OBJFILES ${NEUG_STORAGES_OBJFILES}
        $<TARGET_OBJECTS:neug_metadata_store>)
endif()

if (BUILD_ALL_IN_ONE)
    set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} ${NEUG_STORAGES_OBJFILES} PARENT_SCOPE)
else()
    add_library(neug_storages SHARED ${NEUG_STORAGES_OBJFILES})
    target_link_libraries(neug_storages neug_utils neug_grape_lite ${CMAKE_THREAD_LIBS_INIT})
    install_neug_target(neug_storages)
endif()
