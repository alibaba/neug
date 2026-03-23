add_subdirectory(copier_config)
add_subdirectory(data_chunk)
add_subdirectory(enums)
add_subdirectory(serializer)
# add_subdirectory(signal)
add_subdirectory(task_system)
add_subdirectory(types)
add_subdirectory(vector)

add_library(compiler_common
        OBJECT
        case_insensitive_map.cpp
        constants.cpp
        expression_type.cpp
        in_mem_overflow_buffer.cpp
        mask.cpp
        md5.cpp
        metric.cpp
        null_mask.cpp
        profiler.cpp
        random_engine.cpp
        string_utils.cpp
        system_message.cpp
        type_utils.cpp
        utils.cpp
        windows_utils.cpp)
add_dependencies(compiler_common neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:compiler_common>
        PARENT_SCOPE)
