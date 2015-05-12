################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

set(C7A_LIBRARIES c7a gtest gmock pthread)

# macro for building test programs
macro(build_only PROGNAME)

  string(REPLACE "/" "-" TESTNAME "${PROGNAME}") # replace slashes

  add_executable(${TESTNAME} c7a-tests.cpp ${PROGNAME}.cpp ${ARGN})
  target_link_libraries(${TESTNAME} ${C7A_LIBRARIES})

endmacro(build_only)

# macro for building and running test programs
macro(build_test PROGNAME)

  build_only(${PROGNAME})

  string(REPLACE "/" "-" TESTNAME "${PROGNAME}") # replace slashes
  add_test(${TESTNAME} ${TESTNAME} "${CMAKE_CURRENT_SOURCE_DIR}" ${ARGN})

endmacro(build_test)

### list of tests in subdirectories

build_test(api/test_simple_api)
#build_only(api/test_wordcount)
build_test(data/test_data_manager)
build_test(data/test_block_iterator)
#build_test(data/test_dia_allocate)
build_test(data/test_serializer)
build_test(net/mock-network)
build_test(core/test_stage_builder)
build_test(net/test-net-group)
build_test(net/test-buffer)
#build_test(net/test-channel-multiplexer)
build_test(net/test-stream)

################################################################################
