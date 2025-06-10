add_library(nexg_file_system
        OBJECT
        compressed_file_system.cpp
        file_info.cpp
        file_system.cpp
        local_file_system.cpp
        virtual_file_system.cpp
        gzip_file_system.cpp)

target_link_libraries(nexg_file_system Glob)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_file_system>
        PARENT_SCOPE)
