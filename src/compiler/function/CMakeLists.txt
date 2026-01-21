add_subdirectory(aggregate)
add_subdirectory(arithmetic)
add_subdirectory(cast)
add_subdirectory(gds)
add_subdirectory(list)
add_subdirectory(path)
add_subdirectory(pattern)
add_subdirectory(struct)
add_subdirectory(table)

add_library(neug_function
        OBJECT
        aggregate_function.cpp
        base_lower_upper_operation.cpp
        built_in_function_utils.cpp
        cast_string_non_nested_functions.cpp
        cast_from_string_functions.cpp
        comparison_functions.cpp
        function.cpp
        function_collection.cpp
        scalar_macro_function.cpp
        show_loaded_extensions_function.cpp
        vector_arithmetic_functions.cpp
        vector_cast_functions.cpp
        vector_date_functions.cpp
        vector_string_functions.cpp
        export_csv_function.cpp)
add_dependencies(neug_function neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function>
        PARENT_SCOPE)
