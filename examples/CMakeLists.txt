################################################################################
# examples/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

# real apps
c7a_build_prog(word_count)
c7a_build_prog(page_rank)

file(COPY "../tests/inputs/wordcount.in" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY "../tests/inputs/headwords" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

################################################################################
