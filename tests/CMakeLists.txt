################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

set(C7A_LIBRARIES c7a gtest gmock pthread)

# macro for building test programs
macro(build_only PROGNAME)

  string(REPLACE "/" "_" TESTNAME "${PROGNAME}") # replace slashes

  add_executable(${TESTNAME} c7a_tests.cpp ${PROGNAME}.cpp ${ARGN})
  target_link_libraries(${TESTNAME} ${C7A_LIBRARIES})

endmacro(build_only)

# macro for building and running test programs
macro(build_test PROGNAME)

  build_only(${PROGNAME})

  string(REPLACE "/" "_" TESTNAME "${PROGNAME}") # replace slashes
  add_test(${TESTNAME} ${TESTNAME} "${CMAKE_CURRENT_SOURCE_DIR}" ${ARGN})

endmacro(build_test)

### list of tests in subdirectories

#build_test(api/simple_api_test)
build_test(api/wordcount_test)
build_test(data/data_manager_test)
build_test(data/block_emitter_iterator_test)
build_only(data/data_manager_channels_test) #TODO(tb, ej, ts): where do the invalid fd come from?
build_test(data/block_iterator_test)
#build_test(data/dia_allocate_test)
build_test(data/serializer_test)
build_test(common/delegate_test)
build_test(common/stats_counter_test)
build_test(common/cmdline_parser_test)
build_test(common/stats_timer_test)
#build_test(core/stage_builder_test)
build_only(core/hash_table_bench) #takes multiple seconds to complete
build_test(core/post_hash_table_test)
build_test(core/pre_hash_table_test)
build_test(net/group_test)
build_test(net/buffer_test)
build_test(data/binary_builder_test)
#build_test(net/test_channel_multiplexer) //broken because buffer is non-copyable
build_test(net/stream_test)

################################################################################
