add_library(nexg_planner_factorization
        OBJECT
        flatten_resolver.cpp
        sink_util.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_planner_factorization>
        PARENT_SCOPE)
