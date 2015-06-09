################################################################################
# examples/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

# real apps
add_executable(word_count wordcount/word_count.cpp)
add_executable(local_word_count wordcount/word_count_simple.cpp)
add_executable(table hashtable/bench_table.cpp)


file(COPY "../tests/inputs/wordcount.in" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY "../tests/inputs/headwords" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(table c7a ${ALL_LIBRARIES})
target_link_libraries(word_count c7a ${ALL_LIBRARIES})
target_link_libraries(local_word_count c7a ${ALL_LIBRARIES})

################################################################################
