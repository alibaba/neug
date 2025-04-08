add_subdirectory(utils)
add_subdirectory(storages)
add_subdirectory(engines)
add_subdirectory(bin)
if (OPENTELEMETRY_CPP_FOUND)
   add_subdirectory(otel)
endif()

file(GLOB_RECURSE FILES_NEED_LINT
        "engines/*.cc"
        "engines/*.h"
        "bin/*.cc"
        "storages/*.h"
        "storages/*.cc"
        "otel/*.h"
        "otel/*.cc"
        "test/*.h"
        "test/*.cc"
        "utils/*.h"
        "utils/*.cc"
        EXCEPT "*.act.h" "*.actg.h" "*.autogen.h" "*.autogen.cc")
list(FILTER FILES_NEED_LINT EXCLUDE REGEX ".*\.act.h$|.*\.actg.h$|.*\.autogen.h$|.*\.autogen.cc$")
# gsa_clformat
add_custom_target(nexg_clformat
    COMMAND clang-format --style=file -i ${FILES_NEED_LINT}
    COMMENT "Running clang-format, using clang-format-8 from https://github.com/muttleyxd/clang-tools-static-binaries/releases"
    VERBATIM)

install(FILES resources/hqps/CMakeLists.txt.template DESTINATION lib/src/)