################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

# macro GTEST_ADD_TESTS taken from FindGTests.cmake
# Thanks to Daniel Blezek <blezek@gmail.com> for the GTEST_ADD_TESTS code
function(GTEST_ADD_TESTS executable extra_args)
  if(NOT ARGN)
    message(FATAL_ERROR "Missing ARGN: Read the documentation for GTEST_ADD_TESTS")
  endif()
  foreach(source ${ARGN})
    file(READ "${source}" contents)
    string(REGEX MATCHALL "TEST_?F?\\(([A-Za-z_0-9 ,]+)\\)" found_tests ${contents})
    foreach(hit ${found_tests})
      string(REGEX
        REPLACE ".*\\( *([A-Za-z_0-9]+), *([A-Za-z_0-9]+) *\\).*" "\\1.\\2"
        test_name ${hit})
      add_test(${test_name} ${executable} --gtest_filter=${test_name} ${extra_args})
    endforeach()
  endforeach()
endfunction()

set(SRCS
    c7a-tests.cpp
    api/test_simple_api.cpp
    data/test_data_manager.cpp
    data/test_block_iterator.cpp
    data/test_serializer.cpp
    net/test-net-group.cpp
    net/test-channel-multiplexer.cpp
    engine/mock-network.cpp
    engine/test_stage_builder.cpp
    # api/test_wordcount.cpp
    data/test_dia_allocate.cpp
)

add_executable(c7a-tests ${SRCS})

target_link_libraries(c7a-tests
  c7a gtest gmock pthread
)

GTEST_ADD_TESTS(c7a-tests "${CMAKE_CURRENT_SOURCE_DIR}" ${SRCS})

################################################################################
