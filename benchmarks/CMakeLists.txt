################################################################################
# benchmarks/CMakeLists.txt
#
# CMake file for Thrill benchmark programs.
################################################################################

thrill_build_prog(hashtable/bench_bucket_hashtable)
thrill_build_prog(hashtable/bench_probing_hashtable)

thrill_build_prog(data/disk)
thrill_build_prog(data/file_read_write)
thrill_build_prog(data/stream)
thrill_build_prog(data/stream_read_write)
thrill_build_prog(data/stream_a_to_b)
thrill_build_prog(data/stream_scatter)
file(COPY "data/run_data_benchmark.sh" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

thrill_build_prog(serialization/bench_serialization)

add_subdirectory(word_count)
add_subdirectory(page_rank)
add_subdirectory(net)

################################################################################
