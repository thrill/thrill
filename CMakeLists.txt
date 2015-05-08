################################################################################
# CMakeLists.txt
#
# Build script for c7a.
################################################################################

cmake_minimum_required(VERSION 2.8)

project(c7a)

# custom cmake scripts
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# prohibit in-source builds
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(SEND_ERROR "In-source builds are not allowed.")
endif()

# default to Debug building for single-config generators
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message("Defaulting CMAKE_BUILD_TYPE to Debug")
  set(CMAKE_BUILD_TYPE "Debug")
endif()

# enable warnings
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall -std=c++1y")

# add sanitizer flags for debug builds
if(NOT APPLE AND ${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  # enable AddressSanitizer
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  # enable ThreadSanitizer
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
  # enable UndefinedBehaviorSanitizer
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
endif()

################################################################################
### Find Required Libraries

# example: use Boost libraries:

#find_package(Boost 1.42.0 REQUIRED COMPONENTS regex locale serialization)
#include_directories(${Boost_INCLUDE_DIRS})
#set(ALL_LIBRARIES ${Boost_LIBRARIES} ${ALL_LIBRARIES})
#message("Final Boost include dir: ${Boost_INCLUDE_DIRS}")

# google test + mock - enable "make test" and add_test()
add_subdirectory(extlib/gmock) # note: gmock includes gtest
enable_testing()
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
include_directories(${gmock_SOURCE_DIR}/include ${gmock_SOURCE_DIR})

################################################################################
### Descend into Subdirectories

include_directories(${CMAKE_SOURCE_DIR})

# descend into library source
add_subdirectory(c7a)

# descend into testsuite
add_subdirectory(tests)

# descend into example programs source
add_subdirectory(examples)

################################################################################
