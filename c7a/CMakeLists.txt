################################################################################
# c7a/CMakeLists.txt
#
# CMake file for c7a library
################################################################################

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
)

add_library(c7a STATIC ${SRCS})

################################################################################
