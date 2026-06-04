if (BUILD_PYTHON)
    add_subdirectory(python_bind)
endif()

if (BUILD_NODEJS)
    if (TARGET neug_main)
        add_subdirectory(nodejs_bind)
    else()
        message(WARNING "neug_main not found, skipping nodejs bindings")
    endif()
endif()

add_subdirectory(utils)