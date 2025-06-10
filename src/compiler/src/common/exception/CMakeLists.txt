add_library(nexg_common_exception
        OBJECT
        exception.cpp
        message.cpp)

if (ENABLE_BACKTRACES)
        target_link_libraries(nexg_common_exception PRIVATE cpptrace::cpptrace)
endif()

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_common_exception>
        PARENT_SCOPE)
