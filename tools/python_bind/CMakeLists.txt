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
add_dependencies(neug_py_bind neug_plan_proto)
if (BUILD_ALL_IN_ONE)
    set(NEUG_LIBRARIES neug_libraries)
else()
    set(NEUG_LIBRARIES neug_graph_db neug_main neug_utils)
endif()

target_link_libraries(neug_py_bind PRIVATE ${NEUG_LIBRARIES}) 