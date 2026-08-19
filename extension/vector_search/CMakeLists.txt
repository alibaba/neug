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

target_include_directories(neug_vector_search_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${ZVEC_SOURCE_DIR}/src/include)
target_link_libraries(neug_vector_search_extension PRIVATE
    neug
    protobuf::libprotobuf
    zvec_roaring)

# ZVec registers metrics and index implementations from static initializers.
# Force all registration translation units into the extension.
if(APPLE)
    target_link_options(neug_vector_search_extension PRIVATE
        "LINKER:-force_load,$<TARGET_FILE:zvec_core_static>"
        "LINKER:-force_load,$<TARGET_FILE:zvec_ailego_static>"
        "LINKER:-force_load,$<TARGET_FILE:zvec_turbo_static>")
elseif(UNIX)
    target_link_libraries(neug_vector_search_extension PRIVATE
        -Wl,--whole-archive
        zvec_core_static
        zvec_ailego_static
        zvec_turbo_static
        -Wl,--no-whole-archive)
else()
    target_link_libraries(neug_vector_search_extension PRIVATE
        zvec_core_static
        zvec_ailego_static
        zvec_turbo_static)
endif()
add_dependencies(neug_vector_search_extension zvec_external)
