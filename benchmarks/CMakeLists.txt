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
add_executable(bench_data_file_read_write data/file_read_write.cpp)
add_executable(bench_data_channel_read_write data/channel_read_write.cpp)
add_executable(bench_data_channel_a_to_b data/channel_a_to_b.cpp)
add_executable(bench_data_channel_scatter data/channel_scatter.cpp)
file(COPY "data/run_data_benchmark.sh" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(bench_data_disk c7a ${ALL_LIBRARIES})
target_link_libraries(bench_data_file_read_write c7a ${ALL_LIBRARIES})
target_link_libraries(bench_data_channel_read_write c7a ${ALL_LIBRARIES})
target_link_libraries(bench_data_channel_a_to_b c7a ${ALL_LIBRARIES})
target_link_libraries(bench_data_channel_scatter c7a ${ALL_LIBRARIES})

add_executable(bench_serialization serialization/bench_serialization.cpp)
target_link_libraries(bench_serialization c7a ${ALL_LIBRARIES})

################################################################################
