################################################################################
# examples/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

add_executable(bench_bucket_hashtable hashtable/bench_bucket_hashtable.cpp)
add_executable(bench_probing_hashtable hashtable/bench_probing_hashtable.cpp)

target_link_libraries(bench_bucket_hashtable c7a ${ALL_LIBRARIES})
target_link_libraries(bench_probing_hashtable c7a ${ALL_LIBRARIES})

################################################################################
