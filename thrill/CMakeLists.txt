################################################################################
# thrill/CMakeLists.txt
#
# CMake file for Thrill library
################################################################################

if(NOT MSVC)
  file(GLOB_RECURSE SRCS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/*/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/*/*.hpp
    )
else()
  set(SRCS
    api/context.cpp
    common/cmdline_parser.cpp
    common/logger.cpp
    common/porting.cpp
    common/string.cpp
    common/thread_pool.cpp
    core/file_io.cpp
    data/multiplexer.cpp
    mem/malloc_tracker.cpp
    mem/manager.cpp
    net/group.cpp
    net/dispatcher_thread.cpp
    net/flow_control_channel.cpp
    net/system_control_channel.cpp
    net/mock/group.cpp
    #net/lowlevel/select_dispatcher.cpp
    #net/lowlevel/socket.cpp
    #net/lowlevel/socket_address.cpp
    )
endif()

add_library(thrill ${SRCS})

################################################################################
