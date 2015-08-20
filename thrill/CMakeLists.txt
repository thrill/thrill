################################################################################
# thrill/CMakeLists.txt
#
# CMake file for c7a library
################################################################################

file(GLOB_RECURSE SRCS
  RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/*.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/*/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/*/*.hpp
)

add_library(c7a STATIC ${SRCS})

################################################################################
