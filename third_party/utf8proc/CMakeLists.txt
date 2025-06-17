add_library(utf8proc
        STATIC
        utf8proc.cpp
        utf8proc_wrapper.cpp)

install_neug_target(utf8proc)

target_include_directories(utf8proc PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
