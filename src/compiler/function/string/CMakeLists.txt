add_library(neug_string_function
        OBJECT
        string_split_function.cpp
        init_cap_function.cpp
        levenshtein_function.cpp
        split_part.cpp
        regex_full_match_function.cpp
        regex_replace_function.cpp)
add_dependencies(neug_string_function neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_string_function>
        PARENT_SCOPE)
