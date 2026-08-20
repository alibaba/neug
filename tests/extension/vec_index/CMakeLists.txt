# VecIndex is built as a standalone .neug_extension shared library for the
# e2e_index_test.  It derives from StorageIndex and uses many internal NeuG
# symbols (VecColumn, ModuleFactory, CheckpointManifest, etc.) that are not
# exported from neug.dll on Windows (NEUG_API only covers the public API, and
# WINDOWS_EXPORT_ALL_SYMBOLS is intentionally disabled to avoid the MSVC
# 65535-export limit).  Skip this test extension on Windows; the e2e_index_test
# that loads it is also not run in the Windows CI test matrix.
if(NOT WIN32)
    add_library(neug_test_vec_index_extension SHARED vec_index_extension.cc)

    target_include_directories(neug_test_vec_index_extension PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/tests
        ${CMAKE_BINARY_DIR})
    target_link_libraries(neug_test_vec_index_extension PRIVATE neug)

    if(DEFINED CMAKE_LIBRARY_OUTPUT_DIRECTORY AND CMAKE_LIBRARY_OUTPUT_DIRECTORY)
        get_filename_component(_extension_output_root
            "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}" ABSOLUTE)
    else()
        get_filename_component(_extension_output_root "${CMAKE_BINARY_DIR}" ABSOLUTE)
    endif()
    set(NEUG_TEST_VEC_INDEX_EXTENSION_HOME "${_extension_output_root}"
        CACHE INTERNAL "Extension home containing the test VecIndex library")

    if(APPLE)
        set(_test_vec_index_rpath
            "@loader_path/../../neug/.dylibs"
            "@loader_path/../.."
            "@loader_path")
    else()
        set(_test_vec_index_rpath "$ORIGIN/../..")
    endif()

    set_target_properties(neug_test_vec_index_extension PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${_extension_output_root}/extension/vec_index"
        OUTPUT_NAME vec_index
        PREFIX "lib"
        SUFFIX ".neug_extension"
        BUILD_WITH_INSTALL_RPATH true
        INSTALL_RPATH "${_test_vec_index_rpath}")
endif()
