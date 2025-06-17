add_library(fast_float INTERFACE)
# export the fast_float target as nexg-targets
install_nexg_target(fast_float)

target_include_directories(fast_float
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)
