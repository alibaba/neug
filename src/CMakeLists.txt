if(ENABLE_GCOV)
    add_compile_options(
        $<$<COMPILE_LANGUAGE:CXX>:--coverage>
        $<$<COMPILE_LANGUAGE:C>:--coverage>
    )
    add_link_options(--coverage)
    
    set(LCOV_EXCLUDE_PATTERNS 
        "*/third_party/*" 
        "*/external/*" 
        "/usr/include/*"
        CACHE STRING "Lcov exclude patterns")
endif()

# Every target under src/ feeds into the neug SHARED library:
# ~60 object/static libraries across the tree, including the whole compiler
# subtree (src/compiler/**). All of them must see NEUG_EXPORTS so NEUG_API in
# their sources expands to the export side; a producer TU compiled on the
# import side fails with C2491 ("definition of dllimport static data member
# not allowed", e.g. date_t.cpp) or silently drops exports. Enumerating the
# targets one by one is unmaintainable, so define it once at directory scope
# here. This is safe: the definition only propagates into subdirectories added
# from this scope (all of src/). Extensions, python/node bindings and tests
# are added from the root scope and never inherit it, so their NEUG_API stays
# on the dllimport consumer side.
add_compile_definitions(NEUG_EXPORTS)

add_subdirectory(utils)
add_subdirectory(storages)
add_subdirectory(transaction)
add_subdirectory(common)

if (BUILD_COMPILER)
    add_subdirectory(compiler)
    add_subdirectory(main)
endif()
add_subdirectory(server)
add_subdirectory(execution)

message(STATUS "Compiler libraries: ${COMPILER_LIBRARIES}")
add_library(neug SHARED ${ALL_OBJECT_FILES})
# Define NEUG_EXPORTS so that NEUG_API resolves to __attribute__((visibility("default")))
# for symbols we want to export, while all other symbols remain hidden
# due to CMAKE_CXX_VISIBILITY_PRESET=hidden.
target_compile_definitions(neug PRIVATE NEUG_EXPORTS)
if (ENABLE_GCOV)
    target_link_options(neug PRIVATE --coverage)
endif()
if(NOT NEUG_LITE)
    target_link_libraries(neug PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()
target_link_libraries(neug PRIVATE ${GFLAGS_LIBRARIES} ${YAML_CPP_LIBRARIES} ${COMPILER_LIBRARIES})
target_link_libraries(neug PUBLIC ${GLOG_LIBRARIES})
# Protobuf is PRIVATE to avoid double-initialization of global statics when
# downstream shared libraries (e.g. neug_py_bind.so) also load libneug.dylib.
# Executables that use protobuf-generated headers (e.g. examples) must link
# ${Protobuf_LIBRARIES} themselves.
target_link_libraries(neug PRIVATE ${Protobuf_LIBRARIES})
if (BUILD_HTTP_SERVER)
    if (NOT WIN32)
        target_link_libraries(neug PRIVATE ${BRPC_LIB})
        target_link_libraries(neug PUBLIC ${CRYPTO_LIB}) #${PROTOC_LIB}
    endif()
    # leveldb backs the WAL writer set on every platform, including the
    # cpp-httplib-based Windows HTTP server.
    target_link_libraries(neug PRIVATE ${LEVELDB_LIB})
endif()
if (APPLE)
    target_link_libraries(neug PUBLIC ${MACOS_DYLIBS})
endif()
if (WITH_MIMALLOC)
    # It must be PUBLIC, otherwise there will be performance differences.
    if(WIN32)
        target_link_libraries(neug PUBLIC mimalloc-static)
    else()
        target_link_libraries(neug PUBLIC mimalloc)
    endif()
endif()
target_include_directories(neug PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/compiler>
    $<INSTALL_INTERFACE:include/neug/compiler>)

neug_apply_symbol_visibility(neug)

install_neug_target(neug)
if (ENABLE_BACKTRACES)
    target_link_libraries(neug PUBLIC cpptrace::cpptrace)
endif()
