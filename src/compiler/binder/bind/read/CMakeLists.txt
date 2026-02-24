add_library(neug_binder_bind_read
        OBJECT
        bind_in_query_call.cpp
        bind_load_from.cpp
        bind_match.cpp
        bind_unwind.cpp)
add_dependencies(neug_binder_bind_read neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_binder_bind_read>
        PARENT_SCOPE)
