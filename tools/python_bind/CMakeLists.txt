file(GLOB SOURCE_PY "neug/*")

file(GLOB_RECURSE SOURCE_CPP "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../../third_party/pybind11/CMakeLists.txt")
    message(STAUTS "pybind11 not found, running git submodule update --init")
    execute_process(
        COMMAND git submodule update --init
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../../third_party
        RESULT_VARIABLE result
    )
    if(result)
        message(FATAL_ERROR "Failed to update git submodule: ${result}")
    endif()
endif()
pybind11_add_module(neug_py_bind SHARED ${SOURCE_CPP})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)

target_link_libraries(neug_py_bind PRIVATE neug_utils neug_main neug_graph_db)