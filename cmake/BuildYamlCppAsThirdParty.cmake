#!/bin/bash
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

function (build_yaml_cpp_as_third_party)
    set(CMAKE_DEBUG_POSTFIX "")
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)
    set(YAML_CPP_INSTALL OFF CACHE BOOL "Install yaml-cpp")
    set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "Build yaml-cpp tools")
    set(YAML_CPP_FORMAT_SOURCE OFF CACHE BOOL "Format yaml-cpp should be off")
    add_subdirectory(third_party/yaml-cpp)
    set(YAML_CPP_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/yaml-cpp/include PARENT_SCOPE)
    set(YAML_CPP_LIBRARIES yaml-cpp PARENT_SCOPE)
    set_target_properties(yaml-cpp PROPERTIES DEBUG_POSTFIX "")
endfunction(build_yaml_cpp_as_third_party)