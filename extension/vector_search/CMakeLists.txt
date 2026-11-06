# Copyright 2020 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${CMAKE_SOURCE_DIR}/cmake/BuildZVecAsThirdParty.cmake)
build_zvec_as_third_party()

set(VECTOR_SEARCH_EXTENSION_OBJECT_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/hnsw_index.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/hnsw_index_scan.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/vector_distance_function.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/vector_search_extension.cc")
build_extension_lib("vector_search")

function(link_vector_search_dependencies target_name)
    target_include_directories(${target_name} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${ZVEC_SOURCE_DIR}/src/include)
    target_link_libraries(${target_name} PRIVATE
        neug
        protobuf::libprotobuf
        zvec_roaring)

    # ZVec registers metrics and index implementations from static initializers.
    # Force all registration translation units into both extension and tests.
    if(APPLE)
        target_link_options(${target_name} PRIVATE
            "LINKER:-force_load,$<TARGET_FILE:zvec_core_static>"
            "LINKER:-force_load,$<TARGET_FILE:zvec_ailego_static>"
            "LINKER:-force_load,$<TARGET_FILE:zvec_turbo_static>")
    elseif(UNIX)
        target_link_libraries(${target_name} PRIVATE
            -Wl,--whole-archive
            zvec_core_static
            zvec_ailego_static
            zvec_turbo_static
            -Wl,--no-whole-archive)
    else()
        # MSVC: /WHOLEARCHIVE is the whole-archive equivalent; it keeps ZVec's
        # static-initializer registration TUs in the extension DLL.
        target_link_options(${target_name} PRIVATE
            "LINKER:/WHOLEARCHIVE:$<TARGET_FILE:zvec_core_static>"
            "LINKER:/WHOLEARCHIVE:$<TARGET_FILE:zvec_ailego_static>"
            "LINKER:/WHOLEARCHIVE:$<TARGET_FILE:zvec_turbo_static>")
        target_link_libraries(${target_name} PRIVATE
            zvec_core_static
            zvec_ailego_static
            zvec_turbo_static)
    endif()
    if(TARGET zvec_rabitq_fastscan)
        target_link_libraries(${target_name} PRIVATE zvec_rabitq_fastscan)
    endif()
    add_dependencies(${target_name} zvec_external)
endfunction()

link_vector_search_dependencies(neug_vector_search_extension)

# to trigger extension CI workflow
# Windows C++ tests are currently skipped because DLL exports are incomplete.
# Enable this test along with the other C++ tests once that limitation is fixed.
if(BUILD_TEST AND NOT WIN32)
    add_neug_test(hnsw_vec_source_test
        tests/hnsw_vec_source_test.cc
        ${VECTOR_SEARCH_EXTENSION_OBJECT_FILES})
    target_include_directories(hnsw_vec_source_test PRIVATE
        ${ZVEC_SOURCE_DIR}/src
        ${ZVEC_SOURCE_DIR}/src/core
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_BINARY_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${ZVEC_SOURCE_DIR}/src/include)
    link_vector_search_dependencies(hnsw_vec_source_test)
endif()
