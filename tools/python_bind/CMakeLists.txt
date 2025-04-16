file(GLOB SOURCE_PY "nexg/*")

file(GLOB_RECURSE SOURCE_CPP "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
pybind11_add_module(nexg_py_bind SHARED ${SOURCE_CPP})

target_link_libraries(nexg_py_bind PRIVATE nexg_utils nexg_main nexg_graph_db)