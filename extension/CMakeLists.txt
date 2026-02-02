message(STATUS "=== Building extensions: ${BUILD_EXTENSIONS} ===")

function(set_extension_properties target_name output_name extension_name)
    # Determine output directory based on build context
    # If CMAKE_LIBRARY_OUTPUT_DIRECTORY is set (Python build), use it directly like neug_libraries
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

function(add_extension_test EXTENSION_NAME)
    find_package(OpenSSL REQUIRED)
    set(TEST_NAME ${EXTENSION_NAME}_extension_test)
    set(SRCS ${ARGN})
    add_executable(${TEST_NAME} ${SRCS})
    target_link_libraries(${TEST_NAME} PUBLIC neug::GTest neug_${EXTENSION_NAME}_extension neug_libraries
        ${GLOG_LIBRARIES} ${Protobuf_LIBRARIES} ${ARROW_LIB} ${OPENSSL_LIBRARIES} 
        ${CRYPTO_LIB} ${GFLAGS_LIBRARIES})
    if (ENABLE_GCOV)
        target_compile_options(${TEST_NAME} PRIVATE --coverage)
        target_link_options(${TEST_NAME} PRIVATE --coverage)
    endif()
    include(GoogleTest)
	add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
	gtest_add_tests(TARGET ${TEST_NAME} TEST_LIST ${TEST_NAME}_tests)
endfunction()

function(add_extension_if_enabled extension)
    if ("${extension}" IN_LIST BUILD_EXTENSIONS)
        message(STATUS "Adding extension: ${extension}")
        add_subdirectory(${extension})
    endif()
endfunction()

# Add json extension
add_extension_if_enabled("json")

# Add sample extension (subgraph matching)
add_extension_if_enabled("sample")

message(STATUS "=== Extension configuration complete ===")