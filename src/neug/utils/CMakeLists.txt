############################################ Generate Http service Proto ############################################
set(HTTP_SERVICE_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../proto)
set(HTTP_SERVICE_PROTO_FILES
        ${HTTP_SERVICE_PROTO_DIR}/http_svc.proto)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/http_service)

# proto gen for http service proto
# NOTICE: Currently, the auto generation of http service proto files is disabled to avoid introducing dependencies for protobuf-compiler.
# We generate the proto files in advance and use them as source files.
if (USE_SYSTEM_PROTOBUF)
        message(STATUS "Using system protobuf for http service proto generation")
        file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/http_service)
        protobuf_generate(APPEND_PATH
                TARGET ${LOCAL_EXE_NAME}
                LANGUAGE cpp
                OUT_VAR HTTP_SERVICE_PROTO_SRCS
                PROTOS ${HTTP_SERVICE_PROTO_FILES}
                IMPORT_DIRS ${HTTP_SERVICE_PROTO_DIR}
                PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/http_service
        )
else()
        file(GLOB HTTP_SERVICE_PROTO_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/proto/http_service/*.pb.cc")
        include_directories(SYSTEM ${CMAKE_CURRENT_SOURCE_DIR}/../../../include/neug/utils/proto/http_service/)
endif()


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

# Remove the comments when you want to generate the proto files automatically.
if (USE_SYSTEM_PROTOBUF)
        file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/plan)
        file(GLOB CODE_GEN_PROTOBUF_FILES "${CMAKE_CURRENT_SOURCE_DIR}/../../../proto/*.proto")
        list(FILTER CODE_GEN_PROTOBUF_FILES EXCLUDE REGEX ".*http_svc\\.proto$")

        protobuf_generate(APPEND_PATH
                TARGET ${LOCAL_EXE_NAME}
                LANGUAGE cpp
                OUT_VAR PROTO_SRCS_GIE
                PROTOS ${CODE_GEN_PROTOBUF_FILES}
                IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/../../../proto
                PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/plan
        )
else()
        file(GLOB PROTO_SRCS_GIE "${CMAKE_CURRENT_SOURCE_DIR}/proto/plan/*.pb.cc")
        include_directories(SYSTEM ${CMAKE_CURRENT_SOURCE_DIR}/../../../include/neug/utils/proto/plan/)
endif()
if (BUILD_ALL_IN_ONE)
        add_library(neug_plan_proto OBJECT ${PROTO_SRCS_GIE})
else()
        add_library(neug_plan_proto SHARED ${PROTO_SRCS_GIE})
        if (APPLE)
                message(STATUS "Linking with ${Protobuf_LIBRARIES}")
                target_link_libraries(neug_plan_proto  PUBLIC ${Protobuf_LIBRARIES} ${GLOG_LIBRARIES})
        else()
                target_link_libraries(neug_plan_proto ${Protobuf_LIBRARIES} ${GLOG_LIBRARIES})
        endif()
        install_neug_target(neug_plan_proto)
endif()

if (USE_SYSTEM_PROTOBUF)        
        install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug
                DESTINATION include
                FILES_MATCHING
                PATTERN "*.h"
        )
else()
        install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/proto
                DESTINATION include/neug/proto
                FILES_MATCHING
                PATTERN "*.h"
        )
endif()

############################################ Build Utils Library ############################################
file(GLOB_RECURSE UTILS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
# exclude files end with .pb.cc
list(FILTER UTILS_SRC_FILES EXCLUDE REGEX ".*\\.pb\\.cc$")
# Remove the files in proto directory
message(STATUS "Utils source files: ${UTILS_SRC_FILES}")

if (NOT BUILD_WITH_OSS)
        list(REMOVE_ITEM UTILS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/remote/oss_storage.cc")
endif()

set(ALL_UTILS_SRC_FILES ${UTILS_SRC_FILES})
if (BUILD_ALL_IN_ONE)
        if (BUILD_HTTP_SERVER)
                list(APPEND ALL_UTILS_SRC_FILES ${HTTP_SERVICE_PROTO_SRCS})
        endif()
endif()

if (BUILD_ALL_IN_ONE)
        add_library(neug_utils OBJECT ${ALL_UTILS_SRC_FILES})
        set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_utils> $<TARGET_OBJECTS:neug_plan_proto> PARENT_SCOPE)
else()
        add_library(neug_utils SHARED ${UTILS_SRC_FILES})
        add_library(neug_brpc_proto STATIC ${HTTP_SERVICE_PROTO_SRCS})
        target_link_libraries(neug_utils PUBLIC neug_plan_proto ${Protobuf_LIBRARIES} ${ARROW_LIB} ${GLOG_LIBRARIES} ${YAML_CPP_LIBRARIES})
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
