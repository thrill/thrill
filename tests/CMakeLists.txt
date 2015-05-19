################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

set(C7A_LIBRARIES c7a gtest gmock pthread)

# macro for building test programs
macro(build_only PROGNAME)

  string(REPLACE "/" "-" TESTNAME "${PROGNAME}") # replace slashes

  add_executable(${TESTNAME} c7a_tests.cpp ${PROGNAME}.cpp ${ARGN})
  target_link_libraries(${TESTNAME} ${C7A_LIBRARIES})

endmacro(build_only)

# macro for building and running test programs
macro(build_test PROGNAME)

  build_only(${PROGNAME})

  string(REPLACE "/" "-" TESTNAME "${PROGNAME}") # replace slashes
  add_test(${TESTNAME} ${TESTNAME} "${CMAKE_CURRENT_SOURCE_DIR}" ${ARGN})

endmacro(build_test)

### list of tests in subdirectories

#build_test(api/test_simple_api)
build_test(api/test_wordcount)
build_test(data/test_data_manager)
build_test(data/test_block_emitter_iterator)
build_test(data/test_data_manager_channels)
build_test(data/test_block_iterator)
#build_test(data/test_dia_allocate)
build_test(data/test_serializer)
build_test(common/test_delegate)
build_test(net/mock_network)
#build_test(core/test_stage_builder)
build_only(core/bench_hash_table) #takes multiple seconds to complete
build_test(core/test_post_hash_table)
build_test(core/test_pre_hash_table)
build_test(net/test_net_group)
build_test(net/test_buffer)
build_test(data/test_binary_builder)
#build_test(net/test_channel_multiplexer) //broken because buffer is non-copyable
build_test(net/test_stream)

################################################################################
