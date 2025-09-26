add_subdirectory(graph)
add_subdirectory(csr)
add_subdirectory(loader)
set(NEUG_STORAGES_OBJFILES $<TARGET_OBJECTS:neug_property_graph> $<TARGET_OBJECTS:neug_storages_csr> $<TARGET_OBJECTS:neug_storages_loader>)

set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} ${NEUG_STORAGES_OBJFILES} PARENT_SCOPE)
