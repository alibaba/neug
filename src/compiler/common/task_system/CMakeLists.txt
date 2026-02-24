add_library(neug_common_task_system
        OBJECT
        task.cpp
        task_scheduler.cpp 
        progress_bar.cpp
        terminal_progress_bar_display.cpp)
add_dependencies(neug_common_task_system neug_plan_proto)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_common_task_system>
        PARENT_SCOPE)
