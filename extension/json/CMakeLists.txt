file(GLOB_RECURSE JSON_SRC_FILES 
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/functions/*.cpp")

add_library(json SHARED ${JSON_SRC_FILES})

set_target_properties(json PROPERTIES
    OUTPUT_NAME "json"
    PREFIX "lib"
    SUFFIX ".neug_extension"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/lib"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/lib"
)

target_include_directories(json
    PRIVATE
      ${CMAKE_SOURCE_DIR}/include
      ${CMAKE_SOURCE_DIR}/third_party
      ${CMAKE_CURRENT_SOURCE_DIR}/src
)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(json PRIVATE -fvisibility=default)
endif()