add_library(neug_common_exception
        OBJECT
        exception.cpp
        message.cpp)

if (ENABLE_BACKTRACES)
        target_link_libraries(neug_common_exception PRIVATE cpptrace::cpptrace)
endif()

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_common_exception>
        PARENT_SCOPE)
