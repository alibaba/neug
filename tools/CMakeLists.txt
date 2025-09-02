if (BUILD_PYTHON)
    if (TARGET neug_main)
        add_subdirectory(python_bind)
    else()
        message(WARNING "neug_main not found, skipping python bindings")
    endif()
endif()

add_subdirectory(utils)