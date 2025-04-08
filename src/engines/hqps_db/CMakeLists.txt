install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/src/engines/
        FILES_MATCHING
        PATTERN "*.h"
)