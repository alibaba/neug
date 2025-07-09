file(GLOB SOURCE_PY "neug/*")

file(GLOB_RECURSE SOURCE_CPP "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
pybind11_add_module(neug_py_bind SHARED ${SOURCE_CPP})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)
add_dependencies(neug_py_bind neug_plan_proto)
if (BUILD_ALL_IN_ONE)
    set(NEUG_LIBRARIES neug_libraries)
else()
    set(NEUG_LIBRARIES neug_graph_db neug_main neug_utils)
endif()

target_link_libraries(neug_py_bind PRIVATE ${NEUG_LIBRARIES}) 