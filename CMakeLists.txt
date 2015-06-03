################################################################################
# CMakeLists.txt
#
# Build script for c7a.
################################################################################

cmake_minimum_required(VERSION 2.8)

project(c7a)

# custom cmake scripts
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# prohibit in-source builds
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(SEND_ERROR "In-source builds are not allowed.")
endif()

# default to Debug building for single-config generators
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message("Defaulting CMAKE_BUILD_TYPE to Debug")
  set(CMAKE_BUILD_TYPE "Debug")
endif()

option(JENKINS_BUILD "Build all with more warnings" OFF)

option(USE_GCOV "Compile and run tests with gcov for coverage analysis." OFF)

option(USE_VALGRIND "Run tests with valgrind, reports using XML files." OFF)

# enable warnings
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall -std=c++1y -g")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -pie -fPIC")

# add sanitizer flags for debug builds
if(JENKINS_BUILD)
  if(NOT USE_VALGRIND)
    # enable AddressSanitizer
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    # enable ThreadSanitizer
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    # enable UndefinedBehaviorSanitizer
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
  endif()

  # build using libc++ with clang in Jenkins
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
    message(STATUS "Building using libc++ on clang")
  endif()
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  # Clang does not support debug output for auto return types yet
  string(REPLACE "-g" "" CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
  string(REPLACE "-g" "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
endif()

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

###############################################################################
# enable gcov coverage analysis with gcc

if(USE_GCOV)
  # find programs
  find_program(GENHTML genhtml)
  find_program(LCOV lcov)

  if(NOT LCOV OR NOT GENHTML)
    message(SEND_ERROR "Coverage analysis requires lcov and genhtml programs.")
  endif()

  # add coverage anaylsis compile and link flags
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lgcov")

  # add cached variable containing parameters for lcov/genhtml
  set(LCOV_FLAGS "" CACHE STRING "parameters for lcov")
  set(GENHTML_FLAGS --legend --no-branch-coverage
    CACHE STRING "parameters for genhtml")

  # custom target to run before tests
  add_custom_target(lcov-reset
    COMMAND ${LCOV} -q --directory ${CMAKE_BINARY_DIR} --zerocounters
    COMMENT "Resetting code coverage counters")

  # custom lcov target to run tests
  add_custom_target(lcov-runtests
    COMMAND ${CMAKE_CTEST_COMMAND} \${ARGS} || true
    DEPENDS lcov-reset
    COMMENT "Running all unit tests")

  # get git version description
  execute_process(COMMAND git describe --tags
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE GITDESC
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  # command sequence to gather, clean and generate HTML coverage report
  add_custom_target(lcov-html
    COMMAND ${LCOV} -q --directory . --capture --output-file lcov.info
    COMMAND ${LCOV} -q --remove lcov.info '/usr/*' '*/extlib/*' '*/tests/c7a_tests.cpp' ${LCOV_FLAGS} --output-file lcov-clean.info
    COMMAND ${GENHTML} -q -o coverage --title "c7a ${GITDESC}" --prefix ${PROJECT_SOURCE_DIR} ${GENHTML_FLAGS} lcov-clean.info
    DEPENDS lcov-runtests
    COMMENT "Capturing code coverage counters and create HTML coverage report"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

  # top-level target to run tests and generate coverage report
  add_custom_target(test-coverage
    COMMENT "Generate HTML coverage report "
    DEPENDS lcov-html)

endif(USE_GCOV)

###############################################################################
# enable gtest framework, valgrind, and collection of results

enable_testing()

# google test + mock - enable "make test" and add_test()
add_subdirectory(extlib/gmock) # note: gmock includes gtest
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
include_directories(${gmock_SOURCE_DIR}/include ${gmock_SOURCE_DIR})

# general valgrind setting, if run with USE_VALGRIND=ON.
set(C7A_VALGRIND_OPTS
  --leak-check=full --track-origins=yes)
#  --error-exitcode=1)
#  --suppressions=${PROJECT_SOURCE_DIR}/misc/valgrind.supp)

################################################################################
### Find Required Libraries

# find pthreads

find_package(Threads)
set(ALL_LIBRARIES ${CMAKE_THREAD_LIBS_INIT} ${ALL_LIBRARIES})

# example: use Boost libraries:

#find_package(Boost 1.42.0 REQUIRED COMPONENTS regex locale serialization)
#include_directories(${Boost_INCLUDE_DIRS})
#set(ALL_LIBRARIES ${Boost_LIBRARIES} ${ALL_LIBRARIES})
#message("Final Boost include dir: ${Boost_INCLUDE_DIRS}")

################################################################################
### Descend into Subdirectories

### Descend into Subdirectories
include_directories(${CMAKE_SOURCE_DIR})

# descend into library source
add_subdirectory(c7a)

# descend into testsuite
add_subdirectory(tests)

# descend into example programs source
add_subdirectory(examples)

add_subdirectory(benchmarks)

################################################################################
