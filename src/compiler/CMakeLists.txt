add_compile_options(-w)

# set(CMAKE_CXX_STANDARD 20)

include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/re2/include)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr4_cypher/include)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr4_runtime/src)

include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/utf8proc/include)
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/antlr_cypher)


configure_file(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/templates/system_config.h.in ${CMAKE_CURRENT_BINARY_DIR}/neug/compiler/common/system_config.h @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

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


install(FILES ${CMAKE_CURRENT_BINARY_DIR}/neug/compiler/common/system_config.h
        DESTINATION include/neug/compiler/common
)
