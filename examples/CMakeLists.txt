################################################################################
# examples/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

# real apps
add_executable(word_count word_count.cpp)
add_executable(local_word_count word_count_simple.cpp)

file(COPY "../tests/inputs/wordcount.in" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY "../tests/inputs/headwords" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(word_count c7a ${ALL_LIBRARIES})
target_link_libraries(local_word_count c7a ${ALL_LIBRARIES})

################################################################################
