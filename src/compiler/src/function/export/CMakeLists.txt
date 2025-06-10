add_library(nexg_function_export
        OBJECT
        export_csv_function.cpp
        export_parquet_function.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_function_export>
        PARENT_SCOPE)
