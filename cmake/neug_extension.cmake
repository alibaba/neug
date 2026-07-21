# SPDX-License-Identifier: Apache-2.0
#
# CMake helpers for out-of-tree NeuG extensions.

set(NEUG_BUILTIN_EXTENSIONS parquet pattern_matching gds httpfs CACHE INTERNAL "")

function(neug_extension_load EXT_NAME)
    cmake_parse_arguments(PARSE_ARGV 1 EXT "" "SOURCE_DIR" "")
    if(NOT EXT_SOURCE_DIR)
        message(FATAL_ERROR "neug_extension_load(${EXT_NAME}): SOURCE_DIR is required")
    endif()
    # Relative SOURCE_DIR is resolved from the including config file's directory
    # (CMAKE_CURRENT_LIST_DIR at the call site), not the process CWD.
    get_filename_component(_abs_source "${EXT_SOURCE_DIR}" ABSOLUTE
        BASE_DIR "${CMAKE_CURRENT_LIST_DIR}")
    if(NOT IS_DIRECTORY "${_abs_source}")
        message(FATAL_ERROR
            "neug_extension_load(${EXT_NAME}): SOURCE_DIR '${_abs_source}' does not exist")
    endif()
    set(NEUG_EXTENSION_${EXT_NAME}_SOURCE_DIR "${_abs_source}" CACHE INTERNAL
        "Source directory for NeuG extension '${EXT_NAME}'")
    message(STATUS "Registered external NeuG extension '${EXT_NAME}' from ${_abs_source}")
endfunction()
