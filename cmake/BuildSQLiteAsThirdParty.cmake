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

if(TARGET neug_sqlite3)
    return()
endif()

set(NEUG_SQLITE_VERSION "3.53.3")
set(NEUG_SQLITE_SOURCE_DIR "${CMAKE_SOURCE_DIR}/third_party/sqlite")
set(NEUG_SQLITE_BUILD_DIR "${CMAKE_BINARY_DIR}/third_party/sqlite-amalgamation")

if(NOT EXISTS "${NEUG_SQLITE_SOURCE_DIR}/VERSION")
    message(FATAL_ERROR
        "SQLite submodule is missing. Run: git submodule update --init third_party/sqlite")
endif()

file(READ "${NEUG_SQLITE_SOURCE_DIR}/VERSION" _neug_sqlite_actual_version)
string(STRIP "${_neug_sqlite_actual_version}" _neug_sqlite_actual_version)
if(NOT _neug_sqlite_actual_version STREQUAL NEUG_SQLITE_VERSION)
    message(FATAL_ERROR
        "Expected SQLite ${NEUG_SQLITE_VERSION}, found ${_neug_sqlite_actual_version}")
endif()

find_program(NEUG_SQLITE_MAKE_EXECUTABLE NAMES gmake make REQUIRED)
find_package(Threads REQUIRED)
file(MAKE_DIRECTORY "${NEUG_SQLITE_BUILD_DIR}")

set(NEUG_SQLITE_AMALGAMATION "${NEUG_SQLITE_BUILD_DIR}/sqlite3.c")
set(NEUG_SQLITE_HEADER "${NEUG_SQLITE_BUILD_DIR}/sqlite3.h")

add_custom_command(
    OUTPUT "${NEUG_SQLITE_AMALGAMATION}" "${NEUG_SQLITE_HEADER}"
    COMMAND "${NEUG_SQLITE_MAKE_EXECUTABLE}"
        -f "${NEUG_SQLITE_SOURCE_DIR}/Makefile.linux-generic"
        "TOP=${NEUG_SQLITE_SOURCE_DIR}"
        sqlite3.c
    WORKING_DIRECTORY "${NEUG_SQLITE_BUILD_DIR}"
    DEPENDS
        "${NEUG_SQLITE_SOURCE_DIR}/VERSION"
        "${NEUG_SQLITE_SOURCE_DIR}/manifest"
        "${NEUG_SQLITE_SOURCE_DIR}/Makefile.linux-generic"
        "${NEUG_SQLITE_SOURCE_DIR}/main.mk"
    COMMENT "Generating SQLite ${NEUG_SQLITE_VERSION} amalgamation"
    VERBATIM)

add_custom_target(neug_sqlite3_amalgamation
    DEPENDS "${NEUG_SQLITE_AMALGAMATION}" "${NEUG_SQLITE_HEADER}")

add_library(neug_sqlite3 STATIC "${NEUG_SQLITE_AMALGAMATION}")
add_dependencies(neug_sqlite3 neug_sqlite3_amalgamation)
set_target_properties(neug_sqlite3 PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    C_VISIBILITY_PRESET hidden)
target_include_directories(neug_sqlite3 PUBLIC "${NEUG_SQLITE_BUILD_DIR}")
target_compile_definitions(neug_sqlite3 PRIVATE
    SQLITE_ENABLE_FTS5=1
    SQLITE_THREADSAFE=1)
target_link_libraries(neug_sqlite3 PUBLIC Threads::Threads ${CMAKE_DL_LIBS})
if(UNIX AND NOT APPLE)
    target_link_libraries(neug_sqlite3 PUBLIC m)
endif()

message(STATUS
    "Using bundled SQLite ${NEUG_SQLITE_VERSION} from ${NEUG_SQLITE_SOURCE_DIR}")
