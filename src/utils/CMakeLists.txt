############################################ Generate Http service Proto ############################################
set(HTTP_SERVICE_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../proto)
set(HTTP_SERVICE_PROTO_FILES
        ${HTTP_SERVICE_PROTO_DIR}/http_svc.proto)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/http_service)

function(compile_proto OUT_HDRS OUT_SRCS DESTDIR HDR_OUTPUT_DIR PROTO_DIR PROTO_FILES)
        # Prefer the built protoc target; fall back to found executable
        if (TARGET protobuf::protoc)
                set(PROTOC_EXE $<TARGET_FILE:protobuf::protoc>)
        elseif (DEFINED PROTOBUF_PROTOC_EXECUTABLE)
                set(PROTOC_EXE ${PROTOBUF_PROTOC_EXECUTABLE})
        else()
                set(PROTOC_EXE protoc)
        endif()

        foreach(P ${PROTO_FILES})
                # Compute a path relative to PROTO_DIR (works whether P is absolute or relative)
                if (IS_ABSOLUTE "${P}")
                        file(RELATIVE_PATH REL_PROTO ${PROTO_DIR} ${P})
                else()
                        set(REL_PROTO ${P})
                endif()

                # Derive output file paths under DESTDIR preserving relative structure
                string(REPLACE .proto .pb.h HDR_RELATIVE ${REL_PROTO})
                set(HDR ${DESTDIR}/${HDR_RELATIVE})
                string(REPLACE .proto .pb.cc SRC_RELATIVE ${REL_PROTO})
                set(SRC ${DESTDIR}/${SRC_RELATIVE})

                list(APPEND HDRS ${HDR})
                list(APPEND SRCS ${SRC})

                # Ensure output directories exist
                get_filename_component(HDR_DIR ${HDR} DIRECTORY)
                get_filename_component(SRC_DIR ${SRC} DIRECTORY)

                add_custom_command(
                        OUTPUT ${HDR} ${SRC}
                        COMMAND ${CMAKE_COMMAND} -E make_directory ${HDR_DIR} ${SRC_DIR}
                                        COMMAND ${PROTOC_EXE} ${PROTOC_FLAGS} -I${PROTO_DIR} -I${Protobuf_INCLUDE_DIRS} --cpp_out=${DESTDIR} ${PROTO_DIR}/${REL_PROTO}
                        # When HDR_OUTPUT_DIR differs from DESTDIR, optionally copy headers
                        COMMAND ${CMAKE_COMMAND} -E copy ${HDR} ${HDR_OUTPUT_DIR}/${HDR_RELATIVE}
                        DEPENDS ${PROTO_DIR}/${REL_PROTO}
                        COMMENT "Generating protobuf C++ from ${REL_PROTO}"
                        VERBATIM
                )
        endforeach()
        set(${OUT_HDRS} ${HDRS} PARENT_SCOPE)
        set(${OUT_SRCS} ${SRCS} PARENT_SCOPE)
endfunction()

# proto gen for http service proto
# NOTICE: Currently, the auto generation of http service proto files is disabled to avoid introducing dependencies for protobuf-compiler.
# We generate the proto files in advance and use them as source files.
message(STATUS "Using system protobuf for http service proto generation")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/http_service)
compile_proto(HTTP_SERVICE_PROTO_HDRS HTTP_SERVICE_PROTO_SRCS
        ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/http_service
        ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/http_service
        ${HTTP_SERVICE_PROTO_DIR}
        "${HTTP_SERVICE_PROTO_FILES}"
)

############################################ Generate Physical Plan Proto ############################################
set(GIE_COMPILER_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../proto/)
set(GIE_COMPILER_WRITE_PROTO_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/../../proto/)
# proto files of gie compiler
set(CODE_GEN_PROTOBUF_FILES
        algebra.proto
        common.proto
        expr.proto
        results.proto
        schema.proto
        type.proto
        stored_procedure.proto
        basic_type.proto
        physical.proto
        cypher_write.proto  
        cypher_dml.proto
        cypher_ddl.proto
        error.proto
)

# Remove the comments when you want to generate the proto files automatically.
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/plan)
file(GLOB CODE_GEN_PROTOBUF_FILES "${CMAKE_CURRENT_SOURCE_DIR}/../../proto/*.proto")
list(FILTER CODE_GEN_PROTOBUF_FILES EXCLUDE REGEX ".*http_svc\\.proto$")
compile_proto(PROTO_HDRS_GIE PROTO_SRCS_GIE
        ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/plan
        ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/plan
        ${GIE_COMPILER_PROTO_DIR}
        "${CODE_GEN_PROTOBUF_FILES}"
)

add_library(neug_plan_proto OBJECT ${PROTO_SRCS_GIE})

# Ensure protoc is built before generating the plan proto sources
if (TARGET protobuf::protoc)
        add_dependencies(neug_plan_proto protobuf::protoc)
endif()

     
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug
        DESTINATION include
        FILES_MATCHING
        PATTERN "*.h"
)

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
if (BUILD_HTTP_SERVER)
        list(APPEND ALL_UTILS_SRC_FILES ${HTTP_SERVICE_PROTO_SRCS})
endif()


add_library(neug_utils OBJECT ${ALL_UTILS_SRC_FILES})
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_utils> $<TARGET_OBJECTS:neug_plan_proto> PARENT_SCOPE)

add_dependencies(neug_utils neug_plan_proto)

# Make all future targets depend on neug_plan_proto
set_property(GLOBAL APPEND PROPERTY GLOBAL_DEPENDS_DEBUG_MODE neug_plan_proto)
