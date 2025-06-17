add_library(thrift
        STATIC
        protocol/TProtocol.cpp
        transport/TTransportException.cpp
        transport/TBufferTransports.cpp)
install_nexg_target(thrift)

target_include_directories(thrift
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
