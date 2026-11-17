cmake_minimum_required(VERSION 3.16)

# Standalone builds require only a C compiler and installed NeuG headers and shared library.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(neug_c_example LANGUAGES C)
    find_path(NEUG_INCLUDE_DIR NAMES neug/c/neug.h)
    find_library(NEUG_LIBRARY NAMES neug)
    if(NOT NEUG_INCLUDE_DIR OR NOT NEUG_LIBRARY)
        message(FATAL_ERROR "NeuG C headers or library not found. Set CMAKE_PREFIX_PATH to the installation directory.")
    endif()
endif()

add_executable(neug_c_example basic.c)
set_target_properties(neug_c_example PROPERTIES
    C_STANDARD 11 C_STANDARD_REQUIRED YES C_EXTENSIONS NO)
if(TARGET neug)
    target_link_libraries(neug_c_example PRIVATE neug)
else()
    target_include_directories(neug_c_example PRIVATE "${NEUG_INCLUDE_DIR}")
    target_link_libraries(neug_c_example PRIVATE "${NEUG_LIBRARY}")
endif()

if(BUILD_TEST)
    # Use a temporary database without a path argument; no user data or fixtures are needed.
    add_test(NAME c_api_example COMMAND neug_c_example)
endif()
