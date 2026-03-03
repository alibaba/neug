file(GLOB SOURCE_PY "neug/*")

file(GLOB_RECURSE SOURCE_CPP "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
pybind11_add_module(neug_py_bind MODULE ${SOURCE_CPP})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)
add_dependencies(neug_py_bind neug_plan_proto)
set(NEUG_LIBRARIES neug)
if (BUILD_HTTP_SERVER)
    set(NEUG_LIBRARIES ${NEUG_LIBRARIES})
endif()

target_link_libraries(neug_py_bind PRIVATE ${NEUG_LIBRARIES} ${GLOG_LIBRARIES})