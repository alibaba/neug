if (BUILD_PYTHON)
    if (TARGET neug_main AND TARGET neug_planner)
        add_subdirectory(python_bind)
    else()
        message(WARNING "neug_main or neug_planner not found, skipping python bindings")
    endif()
endif()

add_subdirectory(utils)