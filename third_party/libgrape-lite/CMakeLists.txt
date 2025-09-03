file(GLOB_RECURSE GRAPE_LITE_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/grape/io/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/grape/serialization/*.cc"
)

add_library(neug_grape_lite STATIC ${GRAPE_LITE_SRC})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
install_neug_target(neug_grape_lite)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include
        FILES_MATCHING
        PATTERN "*.h"
)
