add_library(neug_binder_bind_copy
        OBJECT
        bind_copy_to.cpp
        bind_copy_from.cpp)
add_dependencies(neug_binder_bind_copy neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_binder_bind_copy>
        PARENT_SCOPE)
