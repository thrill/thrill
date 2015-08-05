################################################################################
# examples/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

# real apps
add_executable(word_count word_count.cpp)
add_executable(page_rank page_rank.cpp)

file(COPY "../tests/inputs/wordcount.in" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY "../tests/inputs/headwords" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(word_count c7a ${ALL_LIBRARIES})
target_link_libraries(page_rank c7a ${ALL_LIBRARIES})

################################################################################
