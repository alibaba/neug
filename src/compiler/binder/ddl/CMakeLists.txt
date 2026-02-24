add_library(
        neug_binder_ddl
        OBJECT
        bound_alter_info.cpp
        property_definition.cpp)
add_dependencies(neug_binder_ddl neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_binder_ddl>
        PARENT_SCOPE)
