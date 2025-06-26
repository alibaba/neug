find_package (Hiactor)
if (Hiactor_FOUND)
  include (${Hiactor_CODEGEN_CMAKE_FILE})

  hiactor_codegen (server_actor_autogen server_actor_autogen_files
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/
        INCLUDE_PATHS ${Hiactor_INCLUDE_DIR},${CMAKE_CURRENT_SOURCE_DIR}/../../../,${CMAKE_CURRENT_BINARY_DIR}/../../utils/)
        

  # get all .cc files in current directory, except for generated/
  file(GLOB_RECURSE SERVER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
  list(FILTER SERVER_FILES EXCLUDE REGEX ".*generated.*")

  add_library(neug_server STATIC ${SERVER_FILES} ${server_actor_autogen_files})
  add_dependencies(neug_server neug_utils) # Make sure neug_utils is built before neug_server
  add_dependencies(neug_server server_actor_autogen)
  target_compile_options (neug_server
        PUBLIC
        -Wno-attributes)
  if (BUILD_ALL_IN_ONE)
    target_link_libraries(neug_server Hiactor::hiactor neug_libraries)
  else()
    target_link_libraries(neug_server Hiactor::hiactor neug_graph_db) 
  endif()

  # Hiactor/seastar could be compiled with a compiler support option: -Wno-maybe-uninitialized, 
  # so when we are compiling src with a compiler that does not support this option(e.g. AOCC compiler), 
  # we need to remove it from INTERFACE_COMPILE_OPTIONS.
  # Otherwise, we will got error: unrecognized warning option '-Wno-maybe-uninitialized'
  get_target_property(seastar_options Hiactor::seastar INTERFACE_COMPILE_OPTIONS)
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag("-Wno-maybe-uninitialized" COMPILER_SUPPORTS_WNO_MAYBE_UNINITIALIZED)
  if (NOT COMPILER_SUPPORTS_WNO_MAYBE_UNINITIALIZED)
    list(FILTER seastar_options EXCLUDE REGEX "-Wno-maybe-uninitialized")
    message(STATUS "remove -Wno-maybe-uninitialized from seastar_options, since not supported by ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
  endif ()
  set_target_properties(Hiactor::seastar PROPERTIES INTERFACE_COMPILE_OPTIONS "${seastar_options}")
  
  target_include_directories(neug_server PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../../utils/)
  install_without_export_neug_target(neug_server)
endif ()
