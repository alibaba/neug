function(neug_configure_native_arch)
    if(NEUG_PACKAGE_BUILD AND NEUG_NATIVE_ARCH)
        message(FATAL_ERROR
            "NEUG_NATIVE_ARCH must be OFF when NEUG_PACKAGE_BUILD is ON. "
            "Distributable NeuG packages must not depend on the build host CPU.")
    endif()

    set(NEUG_NATIVE_ARCH_CXX_FLAG "" CACHE INTERNAL "Native architecture C++ flag for NeuG targets" FORCE)
    if(NOT NEUG_NATIVE_ARCH)
        message(STATUS "NEUG_NATIVE_ARCH: OFF")
        return()
    endif()

    if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang)$")
        set(NEUG_NATIVE_ARCH_CXX_FLAG "-march=native" CACHE INTERNAL "Native architecture C++ flag for NeuG targets" FORCE)
        message(STATUS "NEUG_NATIVE_ARCH: ON (${NEUG_NATIVE_ARCH_CXX_FLAG})")
    elseif(APPLE)
        message(STATUS "NEUG_NATIVE_ARCH: ON, but no native CPU flag is applied on Apple platforms")
    else()
        message(WARNING "NEUG_NATIVE_ARCH is ON, but no native CPU flag is defined for ${CMAKE_SYSTEM_NAME}/${CMAKE_CXX_COMPILER_ID}")
    endif()
endfunction()

function(neug_apply_native_arch_to_target target_name)
    if(NOT NEUG_NATIVE_ARCH OR NOT NEUG_NATIVE_ARCH_CXX_FLAG)
        return()
    endif()
    if(NOT TARGET ${target_name})
        return()
    endif()

    get_target_property(_neug_target_type ${target_name} TYPE)
    if(_neug_target_type STREQUAL "INTERFACE_LIBRARY" OR
       _neug_target_type STREQUAL "UTILITY" OR
       _neug_target_type STREQUAL "UNKNOWN_LIBRARY")
        return()
    endif()

    target_compile_options(${target_name} PRIVATE
        $<$<COMPILE_LANGUAGE:CXX>:${NEUG_NATIVE_ARCH_CXX_FLAG}>)
endfunction()

function(neug_apply_native_arch_to_directory dir)
    if(NOT NEUG_NATIVE_ARCH OR NOT NEUG_NATIVE_ARCH_CXX_FLAG)
        return()
    endif()

    get_property(_neug_targets DIRECTORY "${dir}" PROPERTY BUILDSYSTEM_TARGETS)
    foreach(_neug_target IN LISTS _neug_targets)
        neug_apply_native_arch_to_target(${_neug_target})
    endforeach()

    get_property(_neug_subdirs DIRECTORY "${dir}" PROPERTY SUBDIRECTORIES)
    foreach(_neug_subdir IN LISTS _neug_subdirs)
        neug_apply_native_arch_to_directory("${_neug_subdir}")
    endforeach()
endfunction()
