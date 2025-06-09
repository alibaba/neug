file(GLOB SOURCE_PY "nexg/*")

file(GLOB_RECURSE SOURCE_CPP "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
pybind11_add_module(nexg_py_bind SHARED ${SOURCE_CPP})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../src/compiler/src/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)

target_link_libraries(nexg_py_bind PRIVATE nexg_utils nexg_main nexg_graph_db)