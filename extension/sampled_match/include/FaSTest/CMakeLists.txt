cmake_minimum_required(VERSION 3.21)
project(Fastest)

message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "C++ Compiler: ${CMAKE_CXX_COMPILER}")
 set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -Ofast")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20")
FIND_PACKAGE(Boost REQUIRED)
FIND_PACKAGE(GSL REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${GSL_INCLUDE_DIRS})
include_directories(lib)


# add_executable(Fastest driver/subgraph-cardinality-estimation.cc)

# 如果 lib/ 目录中有可复用的源码
file(GLOB_RECURSE FASTEST_LIB_HEADERS "lib/**/*.hpp" "lib/**/*.h")
file(GLOB_RECURSE FASTEST_LIB_SOURCES "lib/**/*.cpp")


if(FASTEST_LIB_SOURCES OR FASTEST_LIB_HEADERS)
    add_library(FaSTestLib STATIC ${FASTEST_LIB_SOURCES})
    target_include_directories(FaSTestLib PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/lib>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(FaSTestLib PUBLIC Boost::boost ${GSL_LIBRARIES})

    # 保留原来的可执行文件
    # target_link_libraries(Fastest PRIVATE FaSTestLib)
    
    # 安装和导出FaSTestLib
    install(TARGETS FaSTestLib
        EXPORT GBITargets
        ARCHIVE DESTINATION lib
        INCLUDES DESTINATION include
    )
    
    # 安装头文件
    install(DIRECTORY lib/ DESTINATION include/fastest
        FILES_MATCHING PATTERN "*.hpp"
                     PATTERN "*.h"
    )
endif()
