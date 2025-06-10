add_library(nexg_planner_extend
        OBJECT
        base_logical_extend.cpp
        logical_extend.cpp
        logical_recursive_extend.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_planner_extend>
        PARENT_SCOPE)