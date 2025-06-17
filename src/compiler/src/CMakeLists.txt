include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Have to pass this down to every subdirectory, which actually adds the files.
# This doesn't affect parent directories.
add_compile_definitions(KUZU_EXPORTS)
add_compile_definitions(ANTLR4CPP_STATIC)
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

# add_library(nexg_compiler STATIC ${ALL_OBJECT_FILES}) 
# Note: The static nexg_compiler target is currently unused and may be removed in the future if deemed unnecessary.
add_library(nexg_compiler_shared SHARED ${ALL_OBJECT_FILES})

set(KUZU_LIBRARIES antlr4_cypher antlr4_runtime fast_float utf8proc re2 roaring_bitmap   thrift  zstd miniz mbedtls lz4  yaml-cpp)
# brotlidec brotlicommon fastpfor snappy  yyjson parquet
if (NOT __SINGLE_THREADED__)
        set(KUZU_LIBRARIES ${KUZU_LIBRARIES} Threads::Threads)
endif()
if(NOT WIN32)
        set(KUZU_LIBRARIES dl ${KUZU_LIBRARIES})
endif()
# Seems to be needed for clang on linux only
# for compiling std::atomic<T>::compare_exchange_weak
if ((NOT APPLE AND CMAKE_CXX_COMPILER_ID STREQUAL "Clang") AND NOT __WASM__ AND NOT __SINGLE_THREADED__)
        set(KUZU_LIBRARIES atomic ${KUZU_LIBRARIES})
endif()
if (ENABLE_BACKTRACES)
        set(KUZU_LIBRARIES ${KUZU_LIBRARIES} cpptrace::cpptrace)
endif()
# target_link_libraries(nexg_compiler PUBLIC ${KUZU_LIBRARIES})
# target_link_libraries(nexg_compiler PUBLIC nexg_plan_proto)
target_link_libraries(nexg_compiler_shared PUBLIC ${KUZU_LIBRARIES})
target_link_libraries(nexg_compiler_shared PUBLIC nexg_plan_proto)
unset(KUZU_LIBRARIES)

set(KUZU_INCLUDES $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}> ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/include/c_api)
# target_include_directories(nexg_compiler PUBLIC ${KUZU_INCLUDES})
target_include_directories(nexg_compiler_shared PUBLIC $<BUILD_INTERFACE:${KUZU_INCLUDES}>)
unset(KUZU_INCLUDES)

if(WIN32)
        # Anything linking against the static library must not use dllimport.
        target_compile_definitions(nexg_compiler INTERFACE KUZU_STATIC_DEFINE)
endif()

if(NOT WIN32)
        set_target_properties(nexg_compiler_shared PROPERTIES OUTPUT_NAME nexg_compiler_shared)
endif()

# install(TARGETS nexg_compiler nexg_compiler_shared)
install_nexg_target(nexg_compiler_shared)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/c_api/kuzu.h TYPE INCLUDE)

if(${BUILD_SINGLE_FILE_HEADER})
        install(FILES ${CMAKE_CURRENT_BINARY_DIR}/kuzu.hpp  TYPE INCLUDE)
endif()
