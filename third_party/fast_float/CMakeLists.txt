add_library(fast_float INTERFACE)
# export the fast_float target as neug-targets
install_neug_target(fast_float)

target_include_directories(fast_float
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/fast_float
        DESTINATION include/fast_float
        FILES_MATCHING
        PATTERN "*.h"
)