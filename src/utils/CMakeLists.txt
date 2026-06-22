############################################ Proto Code Generation ############################################
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../proto)

function(compile_proto OUT_HDRS OUT_SRCS DESTDIR HDR_OUTPUT_DIR PROTO_DIR PROTO_FILES)
        # Prefer the built protoc target; fall back to found executable
        if (TARGET protobuf::protoc)
                set(PROTOC_EXE $<TARGET_FILE:protobuf::protoc>)
        elseif (DEFINED PROTOBUF_PROTOC_EXECUTABLE)
                set(PROTOC_EXE ${PROTOBUF_PROTOC_EXECUTABLE})
        else()
                set(PROTOC_EXE protoc)
        endif()

        # Build a list of include arguments for protoc (-I path ...)
        set(_proto_include_dirs ${PROTO_DIR})
        if (Protobuf_INCLUDE_DIRS)
                list(APPEND _proto_include_dirs ${Protobuf_INCLUDE_DIRS})
        endif()
        list(REMOVE_DUPLICATES _proto_include_dirs)
        set(PROTOC_INCLUDE_ARGS)
        foreach(_inc IN LISTS _proto_include_dirs)
                list(APPEND PROTOC_INCLUDE_ARGS -I ${_inc})
        endforeach()

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

                string(REPLACE ";" " " PROTOC_INCLUDE_ARGS_STR "${PROTOC_INCLUDE_ARGS}")
                message(STATUS "Generating protobuf C++ files with command: ${PROTOC_EXE} ${PROTOC_FLAGS} ${PROTOC_INCLUDE_ARGS_STR} --cpp_out=${DESTDIR} ${PROTO_DIR}/${REL_PROTO}")

                add_custom_command(
                        OUTPUT ${HDR} ${SRC}
                        COMMAND ${CMAKE_COMMAND} -E make_directory ${HDR_DIR} ${SRC_DIR}
                        COMMAND ${PROTOC_EXE} ${PROTOC_FLAGS} ${PROTOC_INCLUDE_ARGS} --cpp_out=${DESTDIR} ${PROTO_DIR}/${REL_PROTO}
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

# --- Generate response proto ---
message(STATUS "Generating query response proto")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/response)
compile_proto(QUERY_RESPONSE_PROTO_HDRS QUERY_RESPONSE_PROTO_SRCS
        ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/response
        ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/response
        ${PROTO_DIR}
        "${PROTO_DIR}/response.proto"
)

# --- Generate http service proto (conditional) ---
if (BUILD_HTTP_SERVER)
        message(STATUS "Generating http service proto")
        file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/http_service)
        compile_proto(HTTP_SERVICE_PROTO_HDRS HTTP_SERVICE_PROTO_SRCS
                ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/http_service
                ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/http_service
                ${PROTO_DIR}
                "${PROTO_DIR}/http_svc.proto"
        )
endif()

# --- Generate physical plan protos (all remaining *.proto) ---
message(STATUS "Generating physical plan protos")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/plan)
file(GLOB PLAN_PROTO_FILES "${PROTO_DIR}/*.proto")
list(FILTER PLAN_PROTO_FILES EXCLUDE REGEX ".*http_svc\\.proto$")
list(FILTER PLAN_PROTO_FILES EXCLUDE REGEX ".*response\\.proto$")
compile_proto(PLAN_PROTO_HDRS PLAN_PROTO_SRCS
        ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/plan
        ${CMAKE_CURRENT_BINARY_DIR}/neug/generated/proto/plan
        ${PROTO_DIR}
        "${PLAN_PROTO_FILES}"
)

# --- Unified proto OBJECT library ---
set(ALL_PROTO_SRCS ${PLAN_PROTO_SRCS} ${QUERY_RESPONSE_PROTO_SRCS})
if (BUILD_HTTP_SERVER)
        list(APPEND ALL_PROTO_SRCS ${HTTP_SERVICE_PROTO_SRCS})
endif()

add_library(neug_proto OBJECT ${ALL_PROTO_SRCS})

if (TARGET protobuf::protoc)
        add_dependencies(neug_proto protobuf::protoc)
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
# Arrow-backed reader sources live in the parquet extension
list(FILTER UTILS_SRC_FILES EXCLUDE REGEX ".*/reader/reader\\.cc$")
list(FILTER UTILS_SRC_FILES EXCLUDE REGEX ".*/reader/dataset_builder\\.cc$")
list(FILTER UTILS_SRC_FILES EXCLUDE REGEX ".*/reader/expression_converter\\.cc$")
list(FILTER UTILS_SRC_FILES EXCLUDE REGEX ".*/reader/json_dataset_builder\\.cc$")
list(FILTER UTILS_SRC_FILES EXCLUDE REGEX ".*/reader/json_options\\.cc$")
list(FILTER UTILS_SRC_FILES EXCLUDE REGEX ".*/arrow_utils\\.cc$")
message(STATUS "Utils source files: ${UTILS_SRC_FILES}")

add_library(neug_utils OBJECT ${UTILS_SRC_FILES})
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_utils> $<TARGET_OBJECTS:neug_proto> PARENT_SCOPE)

add_dependencies(neug_utils neug_proto)