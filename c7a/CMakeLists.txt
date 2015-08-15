################################################################################
# c7a/CMakeLists.txt
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

set_source_files_properties(
  core/malloc_tracker.cpp PROPERTIES COMPILE_FLAGS "-fno-sanitize=address")

add_library(c7a STATIC ${SRCS})

################################################################################
