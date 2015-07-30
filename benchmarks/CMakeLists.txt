################################################################################
# benchmarks/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

add_executable(bench_bucket_hashtable hashtable/bench_bucket_hashtable.cpp)
add_executable(bench_probing_hashtable hashtable/bench_probing_hashtable.cpp)

target_link_libraries(bench_bucket_hashtable c7a ${ALL_LIBRARIES})
target_link_libraries(bench_probing_hashtable c7a ${ALL_LIBRARIES})

add_executable(bench_data_disk data/disk.cpp)
target_compile_options(bench_data_disk PRIVATE "-DENABLE_STATS=true")
target_link_libraries(bench_data_disk c7a ${ALL_LIBRARIES})

add_executable(bench_serialization serialization/bench_serialization.cpp)
target_link_libraries(bench_serialization c7a ${ALL_LIBRARIES})

################################################################################
