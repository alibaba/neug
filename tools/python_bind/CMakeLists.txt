file(GLOB SOURCE_PY "nexg/*")

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
pybind11_add_module(nexg_py_bind SHARED ${SOURCE_CPP})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../src/compiler/src/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)

target_link_libraries(nexg_py_bind PRIVATE nexg_utils nexg_main nexg_graph_db)