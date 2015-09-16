################################################################################
# benchmarks/CMakeLists.txt
#
# CMake file for Thrill benchmark programs.
################################################################################

thrill_build_prog(hashtable/bench_bucket_hashtable)
thrill_build_prog(hashtable/bench_probing_hashtable)

thrill_build_prog(data/disk)
thrill_build_prog(data/file_read_write)
thrill_build_prog(data/channel)

if(NOT MSVC)
  # these benchmarks use a non-portable method to initialize the network,
  # TODO(ts): change to use RunXYZ().
  thrill_build_prog(data/channel_read_write)
  thrill_build_prog(data/channel_a_to_b)
  thrill_build_prog(data/channel_scatter)
  file(COPY "data/run_data_benchmark.sh" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endif()

thrill_build_prog(serialization/bench_serialization)

add_subdirectory(word_count)
add_subdirectory(page_rank)
add_subdirectory(net)
add_subdirectory(merge)

################################################################################
