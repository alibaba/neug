if (BUILD_PYTHON)
    add_subdirectory(python_bind)
endif()

if (BUILD_NODEJS)
    add_subdirectory(nodejs_bind)
endif()

add_subdirectory(utils)