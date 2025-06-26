add_compile_options(-w)

# set(CMAKE_CXX_STANDARD 20)


include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/re2/include)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr4_cypher/include)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr4_runtime/src)

include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/utf8proc/include)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr_cypher)


configure_file(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/templates/system_config.h.in ${CMAKE_CURRENT_BINARY_DIR}/common/system_config.h @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils)


include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_compile_definitions(KUZU_EXPORTS)
add_compile_definitions(ANTLR4CPP_STATIC)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Have to pass this down to every subdirectory, which actually adds the files.
# This doesn't affect parent directories.
add_subdirectory(binder)
#add_subdirectory(c_api)
add_subdirectory(catalog)
add_subdirectory(common)
# to be removed after processor and storage
#add_subdirectory(expression_evaluator)
add_subdirectory(function)
add_subdirectory(graph)
add_subdirectory(main)
add_subdirectory(optimizer)
add_subdirectory(parser)
add_subdirectory(planner)
#add_subdirectory(processor)
#add_subdirectory(storage)
add_subdirectory(transaction)
add_subdirectory(extension)
add_subdirectory(gopt)

# add_library(neug_compiler STATIC ${ALL_OBJECT_FILES}) 
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} PARENT_SCOPE)

# Note: The static neug_compiler target is currently unused and may be removed in the future if deemed unnecessary.
if (NOT BUILD_ALL_IN_ONE)
        message(STATUS "Building compiler as shared library: ${ALL_OBJECT_FILES}")
        message(STATUS "compiler: Compiler libraries: ${COMPILER_LIBRARIES}")
        add_library(neug_compiler_shared SHARED ${ALL_OBJECT_FILES})
        target_link_libraries(neug_compiler_shared PUBLIC ${COMPILER_LIBRARIES})
        # target_include_directories(neug_compiler_shared PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
        set_target_properties(neug_compiler_shared PROPERTIES OUTPUT_NAME neug_compiler_shared)
        install_neug_target(neug_compiler_shared)
endif()

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/c_api/kuzu.h TYPE INCLUDE)

if(${BUILD_SINGLE_FILE_HEADER})
        install(FILES ${CMAKE_CURRENT_BINARY_DIR}/kuzu.hpp  TYPE INCLUDE)
endif()


include(CTest)
enable_testing()
if (BUILD_TEST)
    add_subdirectory(test)
endif()
