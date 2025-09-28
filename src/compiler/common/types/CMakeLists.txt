add_subdirectory(value)

add_library(neug_common_types
        OBJECT
        blob.cpp
        date_t.cpp
        dtime_t.cpp
        interval_t.cpp
        neug_list.cpp
        neug_string.cpp
        timestamp_t.cpp
        types.cpp
        int128_t.cpp
        uuid.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_common_types>
        PARENT_SCOPE)
