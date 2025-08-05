############################################ Generate Http service Proto ############################################
set(HTTP_SERVICE_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../proto)
set(HTTP_SERVICE_PROTO_FILES
        ${HTTP_SERVICE_PROTO_DIR}/http_svc.proto)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/http_service)

# proto gen for http service proto
protobuf_generate(APPEND_PATH
        TARGET ${LOCAL_EXE_NAME}
        LANGUAGE cpp
        OUT_VAR HTTP_SERVICE_PROTO_SRCS
        PROTOS ${HTTP_SERVICE_PROTO_FILES}
        IMPORT_DIRS ${HTTP_SERVICE_PROTO_DIR}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/neug/http_service
)

add_custom_target(generate_http_proto ALL
        DEPENDS ${HTTP_SERVICE_PROTO_SRCS}
        COMMENT "Generating HTTP service proto files"
)



############################################ Generate Physical Plan Proto ############################################
set(GIE_COMPILER_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../proto/)
set(GIE_COMPILER_WRITE_PROTO_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/../../../proto/)
# proto files of gie compiler
set(CODE_GEN_PROTOBUF_FILES
        ${GIE_COMPILER_PROTO_DIR}/algebra.proto
        ${GIE_COMPILER_PROTO_DIR}/common.proto
        ${GIE_COMPILER_PROTO_DIR}/expr.proto
        ${GIE_COMPILER_PROTO_DIR}/results.proto
        ${GIE_COMPILER_PROTO_DIR}/schema.proto
        ${GIE_COMPILER_PROTO_DIR}/type.proto
        ${GIE_COMPILER_PROTO_DIR}/stored_procedure.proto
        ${GIE_COMPILER_PROTO_DIR}/basic_type.proto
        ${GIE_COMPILER_WRITE_PROTO_DIR}/physical.proto
        ${GIE_COMPILER_WRITE_PROTO_DIR}/cypher_write.proto  
        ${GIE_COMPILER_WRITE_PROTO_DIR}/cypher_dml.proto
        ${GIE_COMPILER_WRITE_PROTO_DIR}/cypher_ddl.proto
        ${GIE_COMPILER_WRITE_PROTO_DIR}/error.proto
)

#create directory first
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/proto_generated_gie)
set(PROTO_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/neug/proto_generated_gie/proto)

foreach(file ${CODE_GEN_PROTOBUF_FILES})
        file(COPY ${file} DESTINATION ${PROTO_GENERATED_DIR})
endforeach()
file(GLOB CODE_GEN_PROTOBUF_FILES "${PROTO_GENERATED_DIR}/*.proto")

# proto gen for gie proto
protobuf_generate(APPEND_PATH
        TARGET ${LOCAL_EXE_NAME}
        LANGUAGE cpp
        OUT_VAR PROTO_SRCS_GIE
        PROTOS ${CODE_GEN_PROTOBUF_FILES}
        IMPORT_DIRS ${PROTO_GENERATED_DIR}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/neug/proto_generated_gie
)
if (BUILD_ALL_IN_ONE)
        add_library(neug_plan_proto OBJECT ${PROTO_SRCS_GIE})
else()
        add_library(neug_plan_proto SHARED ${PROTO_SRCS_GIE})
        if (APPLE)
                find_package(absl REQUIRED)
                message(STATUS "Linking with ${Protobuf_LIBRARIES}")
                target_link_libraries(neug_plan_proto  PUBLIC ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} ${GLOG_LIBRARIES})
        else()
                target_link_libraries(neug_plan_proto ${Protobuf_LIBRARIES} ${GLOG_LIBRARIES})
        endif()
        install_neug_target(neug_plan_proto)
endif()

#install proto_generated_files
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug
        DESTINATION include
        FILES_MATCHING
        PATTERN "*.h"
)

############################################ Build Utils Library ############################################
file(GLOB_RECURSE UTILS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

if (NOT BUILD_WITH_OSS)
        list(REMOVE_ITEM UTILS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/remote/oss_storage.cc")
endif()

set(ALL_UTILS_SRC_FILES ${UTILS_SRC_FILES})
if (BUILD_ALL_IN_ONE)
        if (BUILD_HTTP_SERVER)
                if (HTTP_SERVER_TYPE STREQUAL "brpc")
                        list(APPEND ALL_UTILS_SRC_FILES ${HTTP_SERVICE_PROTO_SRCS})
                endif()
        endif()
endif()

if (BUILD_ALL_IN_ONE)
        add_library(neug_utils OBJECT ${ALL_UTILS_SRC_FILES})
        set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_utils> $<TARGET_OBJECTS:neug_plan_proto> PARENT_SCOPE)
else()
        add_library(neug_utils SHARED ${UTILS_SRC_FILES})
        add_library(neug_brpc_proto STATIC ${HTTP_SERVICE_PROTO_SRCS})
        target_link_libraries(neug_utils PUBLIC neug_plan_proto ${Protobuf_LIBRARIES} ${ARROW_LIB}  ${Boost_LIBRARIES} ${GLOG_LIBRARIES} ${YAML_CPP_LIBRARIES})
        target_link_libraries(neug_brpc_proto PUBLIC ${Protobuf_LIBRARIES} ${GLOG_LIBRARIES})
        install_neug_target(neug_brpc_proto)
        if (BUILD_WITH_OSS)
                target_link_libraries(neug_utils PRIVATE cpp-sdk)
        endif()
        if (ENABLE_BACKTRACES)
            target_link_libraries(neug_utils PUBLIC cpptrace::cpptrace)
        endif()
        target_link_libraries(neug_utils PUBLIC mimalloc)
        # for gcc < 9, link stdc++fs
        if (CMAKE_CXX_COMPILER_ID MATCHES "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9)
                message(STATUS "Linking with stdc++fs for gcc < 9")
                target_link_libraries(neug_utils PUBLIC stdc++fs)
        endif()
        install_neug_target(neug_utils)
endif()
add_dependencies(neug_utils neug_plan_proto generate_http_proto)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/neug/
        FILES_MATCHING
        PATTERN "*.h"
)
