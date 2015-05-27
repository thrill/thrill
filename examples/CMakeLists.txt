################################################################################
# examples/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

# real apps
add_executable(word_count word_count.cpp)

# benchmarks
add_executable(bench bench.cpp)
add_executable(bench_ref bench_ref.cpp)

target_link_libraries(word_count c7a ${ALL_LIBRARIES})

################################################################################
