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
set(example_targets neug_c_example)

option(NEUG_VERIFY_PACKAGE "Build C and C++ package verification examples" OFF)
if(NEUG_VERIFY_PACKAGE)
    enable_language(CXX)
    configure_file(verify.c "${CMAKE_CURRENT_BINARY_DIR}/verify.cpp" COPYONLY)
    add_executable(verify_c verify.c)
    add_executable(verify_cpp "${CMAKE_CURRENT_BINARY_DIR}/verify.cpp")
    list(APPEND example_targets verify_c verify_cpp)
endif()

if(WIN32 AND NOT TARGET neug)
    get_filename_component(neug_lib_dir "${NEUG_LIBRARY}" DIRECTORY)
    find_file(NEUG_RUNTIME_LIBRARY NAMES neug.dll
        PATHS "${neug_lib_dir}/../bin" NO_DEFAULT_PATH)
    if(NOT NEUG_RUNTIME_LIBRARY)
        message(FATAL_ERROR "NeuG runtime DLL not found beside the package's lib directory.")
    endif()
endif()

foreach(example IN LISTS example_targets)
    set_target_properties(${example} PROPERTIES
        C_STANDARD 11 C_STANDARD_REQUIRED YES C_EXTENSIONS NO
        CXX_STANDARD 17 CXX_STANDARD_REQUIRED YES CXX_EXTENSIONS NO)
    if(TARGET neug)
        target_link_libraries(${example} PRIVATE neug)
    else()
        target_include_directories(${example} PRIVATE "${NEUG_INCLUDE_DIR}")
        target_link_libraries(${example} PRIVATE "${NEUG_LIBRARY}")
        if(WIN32)
            add_custom_command(TARGET ${example} POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                    "${NEUG_RUNTIME_LIBRARY}" "$<TARGET_FILE_DIR:${example}>")
        endif()
    endif()
endforeach()

if(BUILD_TEST)
    # Use a temporary database without a path argument; no user data or fixtures are needed.
    add_test(NAME c_api_example COMMAND neug_c_example)
endif()
