file(GLOB_RECURSE GRAPE_LITE_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/grape/io/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/grape/serialization/*.cc"
)

add_library(neug_grape_lite SHARED ${GRAPE_LITE_SRC})
target_link_libraries(neug_grape_lite PUBLIC ${Protobuf_LIBRARIES} ${GLOG_LIBRARIES} ${Boost_LIBRARIES})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
install_neug_target(neug_grape_lite)
