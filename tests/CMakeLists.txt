################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

# macro for building test programs
macro(build_only PROGNAME)

  string(REPLACE "/" "_" TESTNAME "${PROGNAME}") # replace slashes

  add_executable(${TESTNAME} ${PROGNAME}.cpp ${ARGN})
  # target_link_libraries(${TESTNAME} c7a ${ALL_LIBRARIES})

endmacro(build_only)

# macro for building and running test programs
macro(build_test PROGNAME)

  build_only(${PROGNAME})

  string(REPLACE "/" "_" TESTNAME "${PROGNAME}") # replace slashes
  add_test(${TESTNAME} ${TESTNAME} ${ARGN})

endmacro(build_test)

# Macro for running tests
macro(test_run NAME)

  if(BUILD_TESTING)
    set(FULLNAME ${NAME} ${ARGN})
    string(REPLACE ";" "_" FULLNAME "${FULLNAME}") # stringify list
    add_test(${FULLNAME} ${NAME} ${ARGN}) # add test with arguments
  endif(BUILD_TESTING)

endmacro(test_run)

include_directories(${gtest_SOURCE_DIR}/include)
set(test_sources
    c7a/test1.cpp
    data/test_data_manager.cpp
    data/test_block_iterator.cpp
    data/test_serializer.cpp
)
add_executable(unit_tests ${test_sources})
target_link_libraries(unit_tests gtest_main)

# descend into directories
add_subdirectory(c7a)

################################################################################
