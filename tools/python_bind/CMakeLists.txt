find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
execute_process(
    COMMAND ${Python3_EXECUTABLE} -c "import sys; print('.'.join(map(str, sys.version_info[:3])))"
    OUTPUT_VARIABLE PYTHON_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
# create string major.minor
string(CONCAT PYTHON_VERSION "cp${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR}")
message(STATUS "Python version: ${PYTHON_VERSION}")

execute_process(COMMAND uname -m OUTPUT_VARIABLE SYSTEM_PROCESSOR)
string(REGEX REPLACE "\n$" "" SYSTEM_PROCESSOR "${SYSTEM_PROCESSOR}")
string(TOLOWER ${CMAKE_SYSTEM_NAME} OS_NAME)
message(STATUS "System processor: ${SYSTEM_PROCESSOR}")
message(STATUS "System os: ${OS_NAME}")
set(PYTHON_BIND_WHEEL_FILE "${CMAKE_CURRENT_SOURCE_DIR}/dist/nexg-${NEXG_VERSION}-${PYTHON_VERSION}-${PYTHON_VERSION}-${OS_NAME}_${SYSTEM_PROCESSOR}.whl")
message(STATUS "Python wheel file: ${PYTHON_BIND_WHEEL_FILE}")

file(GLOB_RECURSE PYTHON_BIND_CPP_FILES "${CMAKE_CURRENT_SOURCE_DIR}/cpp/*.cc")

add_custom_command(
    OUTPUT ${PYTHON_BIND_WHEEL_FILE}
    COMMAND ${CMAKE_COMMAND} -E echo "Building Python Bindings" 
    COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/setup.py bdist_wheel
    COMMAND ${CMAKE_COMMAND} -E echo "Python Bindings built successfully"
    COMMENT "Building Python Bindings... (this may take a while)"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${PYTHON_BIND_CPP_FILES}
    VERBATIM
)
file(GLOB PYTHON_BIND_WHEEL_FILE "${CMAKE_CURRENT_SOURCE_DIR}/dist/*.whl")
add_custom_target(
    nexg_python_bind ALL
    DEPENDS ${PYTHON_BIND_WHEEL_FILE}
    COMMENT "Python Bindings built successfully"
)