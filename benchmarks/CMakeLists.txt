################################################################################
# benchmarks/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

c7a_build_prog(hashtable/bench_bucket_hashtable)
c7a_build_prog(hashtable/bench_probing_hashtable)

c7a_build_prog(data/disk)
c7a_build_prog(data/file_read_write)
c7a_build_prog(data/channel)
c7a_build_prog(data/channel_read_write)
c7a_build_prog(data/channel_a_to_b)
c7a_build_prog(data/channel_scatter)
file(COPY "data/run_data_benchmark.sh" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

c7a_build_prog(serialization/bench_serialization)

################################################################################
