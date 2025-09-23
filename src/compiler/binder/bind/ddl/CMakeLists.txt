add_library(neug_binder_bind_ddl
        OBJECT
        bound_create_table_info.cpp)
add_dependencies(neug_binder_bind_ddl neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_binder_bind_ddl>
        PARENT_SCOPE)
