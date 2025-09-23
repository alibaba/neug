add_library(neug_common_copier_config
        OBJECT
        csv_reader_config.cpp
        reader_config.cpp)
add_dependencies(neug_common_copier_config neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_common_copier_config>
        PARENT_SCOPE)
