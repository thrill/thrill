################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

# macro for building test programs with correct libraries
macro(c7a_build_test TARGETNAME)

  add_executable(${TARGETNAME} ${ARGN})
  target_link_libraries(${TARGETNAME} c7a gtest gtest_main ${ALL_LIBRARIES})

endmacro(c7a_build_test)

# macro for building test programs, without main() in c7a_tests.cpp
macro(build_plain PROGNAME)

  string(REPLACE "/" "_" TESTNAME "${PROGNAME}") # replace slashes

  c7a_build_test(${TESTNAME} ${PROGNAME}.cpp ${ARGN})

endmacro(build_plain)

# macro for building test programs, adding gtest runner in c7a_tests.cpp
macro(build_only PROGNAME)

  # append gtest runner program.
  build_plain(${PROGNAME} ${ARGN})

endmacro(build_only)

# macro for registering test programs: maybe prepend valgrind
macro(test_only TESTNAME)

  if(USE_VALGRIND)
    # prepend valgrind call
    add_test(
      NAME ${TESTNAME}
      COMMAND /usr/bin/valgrind ${C7A_VALGRIND_OPTS}
      --xml=yes --xml-file=${TESTNAME}.xml
      ./${TESTNAME} ${ARGN})
  else()
    add_test(
      NAME ${TESTNAME}
      COMMAND ${TESTNAME} ${ARGN})
  endif()

endmacro(test_only)

# macro for building and running test programs
macro(build_test PROGNAME)

  build_only(${PROGNAME})

  string(REPLACE "/" "_" TESTNAME "${PROGNAME}") # replace slashes
  test_only(${TESTNAME})

endmacro(build_test)

# macro for building and running a group of tests
macro(build_test_group TESTNAME)

  string(REPLACE "/" "_" TARGETNAME "${TESTNAME}") # replace slashes

  c7a_build_test(${TARGETNAME} ${ARGN})
  test_only(${TARGETNAME})

endmacro(build_test_group)

### list of tests in subdirectories

build_plain(common/cmdline_parser_example)
build_test_group(common/tests
  common/cmdline_parser_test.cpp
  common/concurrent_bounded_queue_test.cpp
  common/concurrent_queue_test.cpp
  common/cyclic_barrier_test.cpp
  common/delegate_test.cpp
  common/function_traits_test.cpp
  common/future_queue_test.cpp
  common/future_test.cpp
  common/stats_counter_test.cpp
  common/stats_timer_test.cpp
  common/thread_pool_test.cpp
  common/timed_counter_test.cpp
  )

build_test(net/binary_builder_test)
build_test(net/buffer_test)
build_test(net/dispatcher_thread_test)
build_test(net/flow_control_test)
build_test(net/group_test)

build_test(data/block_queue_test)
build_test(data/file_test)
build_test(data/multiplexer_header_test)
build_test(data/multiplexer_test)
build_test(data/serialization_cereal_test)
build_test(data/serialization_test)

build_test(core/pre_hash_table_test)
build_test(core/pre_hash_table_probing_test)
build_test(core/stage_builder_test)

build_test(api/function_stack_test)
build_test(api/graph_test)
build_test(api/operations_test)
build_test(api/reduce_node_test)
build_test(api/sort_node_test)
build_test(api/sum_node_test)
build_test(api/zip_node_test)

build_test(examples/wordcount_test)

if (OFF)

  build_test(api/reduce_node_test)
  build_test(core/post_hash_table_test)

endif()

### Copy input files into tests binary directory: CMAKE_BINARY_DIR/tests/

file(COPY "inputs/" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

################################################################################
