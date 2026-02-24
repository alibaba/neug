add_library(neug_common_data_chunk
        OBJECT
        data_chunk.cpp
        data_chunk_collection.cpp
        data_chunk_state.cpp
        sel_vector.cpp)
add_dependencies(neug_common_data_chunk neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_common_data_chunk>
        PARENT_SCOPE)
