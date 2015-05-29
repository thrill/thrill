################################################################################
# examples/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

add_executable(bench_word_count word_count.cpp)
add_executable(bench_table bench_table.cpp)
# add_executable(simple simple.cpp)

target_link_libraries(bench_word_count c7a ${ALL_LIBRARIES} pthread)

################################################################################
