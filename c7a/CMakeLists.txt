################################################################################
# c7a/CMakeLists.txt
#
# CMake file for c7a library
################################################################################

if(OFF)
##### PROTOBUF STUFF #############################
include(FindProtobuf)
find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIRS})

# find all proto iles
file(GLOB ProtoFiles "${CMAKE_CURRENT_SOURCE_DIR}/../tests/proto/*.proto")

# set custom output directory for pb.cc files
set(CMAKE_CURRENT_BINARY_DIR_OLD ${CMAKE_CURRENT_BINARY_DIR})
set(CMAKE_CURRENT_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto/")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# compile all protofiles found
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${ProtoFiles})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# set CMAKE_CURRENT_BINARY_DIR back to old path
set(CMAKE_CURRENT_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR_OLD})

#set(ALL_LIBRARIES ${PROTOBUF_LIBRARIES} ${ALL_LIBRARIES})
#set(ALL_LIBRARIES protobuf ${ALL_LIBRARIES})
endif()

file(GLOB_RECURSE SRCS
  RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/api/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/api/*.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/common/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/common/*.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/net/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/net/*.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/data/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/data/*.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/core/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/core/*.hpp
  # ${CMAKE_CURRENT_SOURCE_DIR}/proto/*.pb.h
  # ${CMAKE_CURRENT_BINARY_DIR}/proto/*.pb.cc
)

add_library(c7a STATIC ${SRCS}
                       ${PROTO_SRCS}
                       ${PROTO_HDRS})

################################################################################
