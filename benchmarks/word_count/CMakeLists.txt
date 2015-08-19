################################################################################
# benchmarks/word_count/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

c7a_build_prog(word_count)

file(COPY "${CMAKE_SOURCE_DIR}/tests/inputs/wordcount.in"
  DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

file(COPY "${CMAKE_SOURCE_DIR}/tests/inputs/headwords"
  DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

################################################################################
