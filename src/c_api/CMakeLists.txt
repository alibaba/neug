file(GLOB C_API_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(neug_c_api OBJECT ${C_API_SOURCES})
target_compile_definitions(neug_c_api PRIVATE NEUG_EXPORTS)
add_dependencies(neug_c_api neug_proto)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_c_api> PARENT_SCOPE)

# Existing install rules provide headers and the shared library; also ship the standalone C example and guide.
install(FILES "${PROJECT_SOURCE_DIR}/doc/source/reference/c_api/index.md"
    DESTINATION "${CMAKE_INSTALL_DOCDIR}" RENAME c_api.md)
install(FILES "${PROJECT_SOURCE_DIR}/examples/c/basic.c"
              "${PROJECT_SOURCE_DIR}/examples/c/CMakeLists.txt"
    DESTINATION "${CMAKE_INSTALL_DATADIR}/neug/examples/c")
