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

# descend into directories
add_subdirectory(c7a)

################################################################################
