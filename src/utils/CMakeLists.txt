############################################ Generate Error Code Proto ############################################
# The error code of FLEX Interactive is defined in $GRAPHSCOPE_HOME/proto/error.proto
set(GRAPHSCOPE_ERROR_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../proto/)
# proto files of gie compiler
set(INTERACTIVE_ERROR_PROTO_FILES
        ${GRAPHSCOPE_ERROR_PROTO_DIR}/error.proto
)
#create directory first
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/utils/error_pb)

# proto gen for gie proto
protobuf_generate(APPEND_PATH
        TARGET ${LOCAL_EXE_NAME}
        LANGUAGE cpp
        OUT_VAR ERROR_PROTO_SRCS
        PROTOS ${INTERACTIVE_ERROR_PROTO_FILES}
        IMPORT_DIRS ${GRAPHSCOPE_ERROR_PROTO_DIR}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/src/utils/error_pb
)


############################################ Generate Physical Plan Proto ############################################
set(GIE_COMPILER_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../proto/)
set(GIE_COMPILER_WRITE_PROTO_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/../../proto/)
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
)

#create directory first
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/proto_generated_gie)
set(PROTO_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/src/proto_generated_gie/proto)

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
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/src/proto_generated_gie
)

add_library(nexg_plan_proto SHARED ${PROTO_SRCS_GIE})
target_include_directories(nexg_plan_proto PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

if (APPLE)
        find_package(absl REQUIRED)
        message(STATUS "Linking with ${Protobuf_LIBRARIES}")
        target_link_libraries(nexg_plan_proto  PUBLIC ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} ${GLOG_LIBRARIES})
else()
        target_link_libraries(nexg_plan_proto ${Protobuf_LIBRARIES} ${GLOG_LIBRARIES})
endif()
install_nexg_target(nexg_plan_proto)

#install proto_generated_files
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src
        DESTINATION include
        FILES_MATCHING
        PATTERN "*.h"
)

############################################ Build Utils Library ############################################
file(GLOB_RECURSE UTILS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

if (NOT BUILD_WITH_OSS)
        list(REMOVE_ITEM UTILS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/remote/oss_storage.cc")
endif()


add_library(nexg_utils SHARED ${UTILS_SRC_FILES} ${ERROR_PROTO_SRCS})
target_include_directories(nexg_utils PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
# Link the static library of arrow, to save the trouble of linking the shared library of arrow
target_link_libraries(nexg_utils PUBLIC nexg_plan_proto ${Protobuf_LIBRARIES} ${ARROW_LIB} ${YAML_CPP_LIBRARIES} ${Boost_LIBRARIES} ${GLOG_LIBRARIES})
if (BUILD_WITH_OSS)
        target_link_libraries(nexg_utils PRIVATE cpp-sdk)
endif()


find_package(mimalloc REQUIRED)
# must link with the dynamic library of mimalloc
# NOTE: linking towards mimalloc will cause runtime memory error on macos 15 arm64
# target_link_libraries(nexg_utils PUBLIC mimalloc)
if (APPLE)
        target_link_libraries(nexg_utils PUBLIC absl::log_internal_message)
endif()

install_nexg_target(nexg_utils)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/src/
        FILES_MATCHING
        PATTERN "*.h"
)
