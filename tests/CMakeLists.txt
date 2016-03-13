################################################################################
# tests/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

# macro for building test target programs with correct libraries
macro(thrill_build_target TARGETNAME)

  add_executable(${TARGETNAME} ${ARGN})
  target_link_libraries(${TARGETNAME} thrill gtest gtest_main ${ALL_LIBRARIES})

endmacro(thrill_build_target)

# macro for building test programs, without main() in gtest_main
macro(thrill_build_plain PROGNAME)

  string(REPLACE "/" "_" TESTNAME "${PROGNAME}") # replace slashes

  thrill_build_target(${TESTNAME} ${PROGNAME}.cpp ${ARGN})

endmacro(thrill_build_plain)

# macro for building test programs, adding gtest runner in gtest_main
macro(thrill_build_only PROGNAME)

  # append gtest runner program.
  thrill_build_plain(${PROGNAME} ${ARGN})

endmacro(thrill_build_only)

# macro for registering test programs: maybe prepend valgrind
macro(thrill_test_only TESTNAME)

  set(TARGETNAME ${TESTNAME} ${ARGN})
  string(REPLACE "/" "_" TARGETNAME "${TARGETNAME}") # replace slashes
  string(REPLACE ";" "_" TARGETNAME "${TARGETNAME}") # stringify list

  if(USE_VALGRIND)
    # prepend valgrind call
    add_test(
      NAME ${TARGETNAME}
      COMMAND /usr/bin/valgrind ${THRILL_VALGRIND_OPTS}
      --xml=yes --xml-file=${TESTNAME}.xml
      ./${TESTNAME} ${ARGN})
  else()
    add_test(
      NAME ${TARGETNAME}
      COMMAND ${TESTNAME} ${ARGN})
  endif()

endmacro(thrill_test_only)

# macro for building and running test programs
macro(thrill_build_test PROGNAME)

  thrill_build_only(${PROGNAME})

  string(REPLACE "/" "_" TESTNAME "${PROGNAME}") # replace slashes
  thrill_test_only(${TESTNAME})

endmacro(thrill_build_test)

# macro for building and running a group of tests
macro(thrill_build_test_group TESTNAME)

  string(REPLACE "/" "_" TARGETNAME "${TESTNAME}") # replace slashes

  thrill_build_target(${TARGETNAME} ${ARGN})
  thrill_test_only(${TARGETNAME})

endmacro(thrill_build_test_group)

### list of tests in subdirectories

thrill_build_plain(common/cmdline_parser_example)
thrill_build_test_group(common/tests
  common/aggregate_test
  common/cmdline_parser_test.cpp
  common/concurrent_bounded_queue_test.cpp
  common/concurrent_queue_test.cpp
  common/counting_ptr_test.cpp
  common/delegate_test.cpp
  common/fast_string_test.cpp
  common/function_traits_test.cpp
  common/future_queue_test.cpp
  common/future_test.cpp
  common/json_logger_test.cpp
  common/lru_cache_test.cpp
  common/math_test.cpp
  common/matrix_test.cpp
  common/meta_test.cpp
  common/splay_tree_test.cpp
  common/stats_counter_test.cpp
  common/stats_timer_test.cpp
  common/string_view_test.cpp
  common/thread_barrier_test.cpp
  common/thread_pool_test.cpp
  common/timed_counter_test.cpp
  common/zipf_distribution_test.cpp
  )

thrill_build_test(mem/allocator_test)
thrill_build_test(mem/pool_test)
thrill_build_test(mem/stack_allocator_test)
if(NOT MSVC)
  thrill_build_test(mem/malloc_tracker_test)
endif()

thrill_build_test(net/binary_builder_test)
thrill_build_test(net/buffer_test)
thrill_build_test(net/mock_test)
if(NOT MSVC)
  thrill_build_test(net/tcp_test)
endif()
if(MPI_FOUND)
  thrill_build_only(net/mpi_test)
  # run test with mpirun
  add_test(net_mpi_test ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG} 8 net_mpi_test)
endif()

# compile io tests
thrill_build_only(io/syscall_file_test)
thrill_build_only(io/file_io_sizes_test)
thrill_build_only(io/cancel_io_test)
thrill_build_test(io/block_manager_test)
thrill_build_test(io/config_file_test)

# run io tests with different backend files
thrill_test_only(io_syscall_file_test ".")

thrill_test_only(io_cancel_io_test memory "./testdisk1")
thrill_test_only(io_cancel_io_test syscall "./testdisk1")
thrill_test_only(io_cancel_io_test mmap "./testdisk1")
if(NOT APPLE)
  thrill_test_only(io_cancel_io_test linuxaio "./testdisk1")
endif()

thrill_test_only(io_file_io_sizes_test memory "./testdisk1" 134217728)
thrill_test_only(io_file_io_sizes_test syscall "./testdisk1" 134217728)
thrill_test_only(io_file_io_sizes_test mmap "./testdisk1" 134217728)
if(NOT APPLE)
  thrill_test_only(io_file_io_sizes_test linuxaio "./testdisk1" 134217728)
endif()

thrill_build_test(data/block_queue_test)
thrill_build_test(data/block_pool_test)
thrill_build_test(data/file_test)
thrill_build_test(data/multiplexer_header_test)
thrill_build_test(data/multiplexer_test)
thrill_build_test(data/stream_set_test)
thrill_build_test(data/serialization_cereal_test)
thrill_build_test(data/serialization_test)

thrill_build_test(core/reduce_hash_table_test)
thrill_build_test(core/reduce_post_stage_test)
thrill_build_test(core/reduce_pre_stage_test)
thrill_build_test(core/multiway_merge_test)

thrill_build_test(api/function_stack_test)
thrill_build_test(api/groupby_node_test)
thrill_build_test(api/operations_test)
thrill_build_test(api/read_write_test)
thrill_build_test(api/reduce_node_test)
thrill_build_test(api/sort_node_test)
thrill_build_test(api/stage_builder_test)
thrill_build_test(api/zip_node_test)
# currently disabled because it fails randomly when result is disbalanced
thrill_build_only(api/merge_node_test)

thrill_build_test(examples/k_means_test)
thrill_build_test(examples/page_rank_test)
thrill_build_test(examples/word_count_test)

### Copy input files into tests binary directory: CMAKE_BINARY_DIR/tests/

file(COPY "inputs/" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/inputs/")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/outputs/")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/outputs/binary/")

################################################################################
