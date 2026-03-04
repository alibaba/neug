message(STATUS "=== Building extensions: ${BUILD_EXTENSIONS} ===")

function(set_extension_properties target_name output_name extension_name)
    # Determine output directory based on build context
    # If CMAKE_LIBRARY_OUTPUT_DIRECTORY is set (Python build), use it directly like neug
    # Otherwise, output to build/extension/ (standalone build)
    if(DEFINED CMAKE_LIBRARY_OUTPUT_DIRECTORY AND CMAKE_LIBRARY_OUTPUT_DIRECTORY)
        get_filename_component(lib_output_dir "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}" ABSOLUTE)
        set(output_dir "${lib_output_dir}")
        message(STATUS "Extension ${extension_name} output directory (Python build): ${output_dir}")
    else()
        get_filename_component(binary_dir "${CMAKE_BINARY_DIR}" ABSOLUTE)
        set(output_dir "${binary_dir}")
        message(STATUS "Extension ${extension_name} output directory (standalone): ${output_dir}")
    endif()
    
    set(extension_dir "${output_dir}/extension/${extension_name}")
    file(MAKE_DIRECTORY "${extension_dir}")
    message(STATUS "Setting LIBRARY_OUTPUT_DIRECTORY for ${target_name} to: ${extension_dir}")
    
    set_target_properties(${target_name}
            PROPERTIES
            LIBRARY_OUTPUT_DIRECTORY "${extension_dir}"
            OUTPUT_NAME ${output_name}
            PREFIX "lib"
            SUFFIX ".neug_extension"
            BUILD_WITH_INSTALL_RPATH true
            INSTALL_RPATH "$ORIGIN/../.."
    )
endfunction()

function(build_extension_lib ext_name)
    string(TOUPPER "${ext_name}" ext_name_upper_case)
    set(var_name "${ext_name_upper_case}_EXTENSION_OBJECT_FILES")
    
    if(NOT ${var_name})
        message(FATAL_ERROR "No sources found for extension ${ext_name}. Variable ${var_name} is empty or not set.")
    endif()
    
    add_library(neug_${ext_name}_extension
            SHARED
            ${${var_name}})
    SET(EXTENSION_LIB_NAME "${ext_name}" PARENT_SCOPE)
    SET(EXTENSION_LIB_NAME "${ext_name}")
    set_extension_properties(neug_${EXTENSION_LIB_NAME}_extension ${EXTENSION_LIB_NAME} ${ext_name})
endfunction()

# Build test executable by compiling extension sources directly (no link to
# neug_<ext>_extension .so). Call from extension's CMakeLists after building
# the extension lib and setting <EXT>_EXTENSION_OBJECT_FILES.
#
# add_extension_test(
#   NAME <ext_name>
#   TEST_SRCS <test.cpp> ...
#   [EXTRA_INCLUDE_DIRS <dir> ...]
#   [EXTRA_LINK_LIBS <lib> ...]
# )
function(add_extension_test)
    set(options "")
    set(oneValueArgs NAME)
    set(multiValueArgs TEST_SRCS EXTRA_INCLUDE_DIRS EXTRA_LINK_LIBS)
    cmake_parse_arguments(ARG "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    if(NOT ARG_NAME)
        message(FATAL_ERROR "add_extension_test: NAME is required")
    endif()
    if(NOT BUILD_TEST)
        return()
    endif()
    string(TOUPPER "${ARG_NAME}" _ext_upper)
    set(_sources_var "${_ext_upper}_EXTENSION_OBJECT_FILES")
    if(NOT ${_sources_var})
        message(FATAL_ERROR "add_extension_test: ${_sources_var} is not set. Set it in the extension CMakeLists before calling add_extension_test.")
    endif()
    set(_test_name ${ARG_NAME}_extension_test)
    find_package(OpenSSL REQUIRED)
    add_executable(${_test_name}
        ${ARG_TEST_SRCS}
        ${${_sources_var}})
    target_include_directories(${_test_name} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR})
    if(ARROW_INCLUDE_DIRS)
        target_include_directories(${_test_name} PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
    endif()
    if(ARG_EXTRA_INCLUDE_DIRS)
        target_include_directories(${_test_name} PRIVATE SYSTEM ${ARG_EXTRA_INCLUDE_DIRS})
    endif()
    target_link_libraries(${_test_name} PUBLIC
        neug::GTest
        neug
        ${ARG_EXTRA_LINK_LIBS}
        ${GLOG_LIBRARIES}
        ${ARROW_LIB}
        ${OPENSSL_LIBRARIES}
        ${CRYPTO_LIB})
    if(ENABLE_GCOV)
        target_compile_options(${_test_name} PRIVATE --coverage)
        target_link_options(${_test_name} PRIVATE --coverage)
    endif()
    include(GoogleTest)
    add_test(NAME ${_test_name} COMMAND ${_test_name})
    gtest_add_tests(TARGET ${_test_name} TEST_LIST ${_test_name}_tests)
endfunction()

function(add_extension_if_enabled extension)
    if ("${extension}" IN_LIST BUILD_EXTENSIONS)
        message(STATUS "Adding extension: ${extension}")
        add_subdirectory(${extension})
    endif()
endfunction()

# Add json extension
add_extension_if_enabled("json")

message(STATUS "=== Extension configuration complete ===")