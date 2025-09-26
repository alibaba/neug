file(GLOB_RECURSE TRANSACTION_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc" "${CMAKE_CURRENT_SOURCE_DIR}/wal/*.cc")
add_library(neug_transaction OBJECT ${TRANSACTION_SOURCES})
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_transaction> PARENT_SCOPE)

add_dependencies(neug_transaction neug_plan_proto)