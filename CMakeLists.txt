################################################################################
# CMakeLists.txt
#
# Build script for c7a.
################################################################################

cmake_minimum_required(VERSION 2.8)

# custom cmake scripts
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/misc/cmake)

# project: we only use C++
project(c7a CXX)

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

option(USE_IWYU "Compile with include-what-you-use (iwyu) tool." OFF)

option(USE_TBB "Use (optional) Intel TBB library if available." ON)

option(TRY_COMPILE_HEADERS "Test header files for self-sufficiency: try to compile them." OFF)

option(ENABLE_STATS "enable stats" OFF)
if(ENABLE_STATS)
    message(STATUS "Add Stats support")
    add_definitions(-DENABLE_STATS=1)
else()
    add_definitions(-DENABLE_STATS=0)
endif()

# enable warnings
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -g -W -Wall -Wextra -fPIC")

# enable more warnings
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wpedantic")

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wcast-align -Wcast-qual")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Winit-self -Wnoexcept")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Woverloaded-virtual -Wredundant-decls")
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wstrict-null-sentinel -Wstrict-overflow=5")
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wtautological-compare")
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fipa-pure-const -Wsuggest-attribute=const")
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-conversion")
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-promo")
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wold-style-cast")
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsuggest-override")
endif()

# enable AddressSanitizer
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")

# enable ThreadSanitizer
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -pie -fPIC")

# enable UndefinedBehaviorSanitizer
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")

# add sanitizer flags for debug builds
if(JENKINS_BUILD)

  if(NOT USE_VALGRIND)
    # enable AddressSanitizer
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    # enable ThreadSanitizer
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    # enable UndefinedBehaviorSanitizer
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
  endif()

  # build using libc++ with clang in Jenkins
  if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
    message(STATUS "Building using libc++ on clang")
  endif()
endif()

if(USE_IWYU)
  # Generate clang compilation database
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

  find_package(PythonInterp)
  find_program(iwyu_tool_path NAMES iwyu_tool.py)
  if (iwyu_tool_path AND PYTHONINTERP_FOUND)
    add_custom_target(iwyu
      ALL      # Remove ALL if you don't iwyu to be run by default.
      COMMAND "${PYTHON_EXECUTABLE}" "${iwyu_tool_path}" -p "${CMAKE_BINARY_DIR}"
      COMMENT "Running include-what-you-use tool"
      VERBATIM)
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} --version
    OUTPUT_VARIABLE clang_full_version_string)

  string(REGEX REPLACE ".*clang version ([0-9]+\\.[0-9]+).*" "\\1"
    CLANG_VERSION_STRING ${clang_full_version_string})

  if (CLANG_VERSION_STRING VERSION_LESS 3.6 AND NOT APPLE)
    # Clang < 3.6 0 (?) does not support debug output for auto return types yet
    string(REPLACE "-g" "" CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
    string(REPLACE "-g" "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
  endif()
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
add_subdirectory(extlib/gtest)
include_directories(SYSTEM ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

# general valgrind setting, if run with USE_VALGRIND=ON.
set(C7A_VALGRIND_OPTS
  --leak-check=full --track-origins=yes)
#  --error-exitcode=1)
#  --suppressions=${PROJECT_SOURCE_DIR}/misc/valgrind.supp)

###############################################################################
# add cereal

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/extlib/cereal/include)

################################################################################
### Find Required Libraries

# find pthreads

find_package(Threads)
set(ALL_LIBRARIES ${CMAKE_THREAD_LIBS_INIT} ${ALL_LIBRARIES})

# try to find Intel TBB (optional)

if(USE_TBB)
  find_package(TBB)
endif()

if(NOT TBB_FOUND)
  message(STATUS "Intel TBB not found. No problem, using slower replacements.")
  add_definitions(-DHAVE_INTELTBB=0)
elseif(TBB_INTERFACE_VERSION LESS 8000)
  message(STATUS "Intel TBB ${TBB_INTERFACE_VERSION} is too old. "
    "No problem, using slower replacements.")
  add_definitions(-DHAVE_INTELTBB=0)
else(NOT TBB_FOUND)
  include_directories(${TBB_INCLUDE_DIRS})
  link_directories(${TBB_LIBRARY_DIRS})
  set(ALL_LIBRARIES ${TBB_LIBRARIES} ${ALL_LIBRARIES})
  add_definitions(-DHAVE_INTELTBB=1)
endif()

# example: use Boost libraries:

#find_package(Boost 1.42.0 REQUIRED COMPONENTS regex locale serialization)
#include_directories(${Boost_INCLUDE_DIRS})
#set(ALL_LIBRARIES ${Boost_LIBRARIES} ${ALL_LIBRARIES})
#message("Final Boost include dir: ${Boost_INCLUDE_DIRS}")

# SWIG (optional)

find_package(SWIG)
include(${SWIG_USE_FILE})

################################################################################
### Build Macros

# macro for building main c7a programs with correct libraries
macro(c7a_build TARGETNAME)

  add_executable(${TARGETNAME} ${ARGN})
  target_link_libraries(${TARGETNAME} c7a ${ALL_LIBRARIES})

endmacro(c7a_build)

# macro for building a single-source c7a program
macro(c7a_build_prog PROGNAME)

  string(REPLACE "/" "_" TARGETNAME "${PROGNAME}") # replace slashes

  c7a_build(${TARGETNAME} ${PROGNAME}.cpp ${ARGN})

endmacro(c7a_build_prog)

################################################################################
### Descend into Subdirectories

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# descend into library source
add_subdirectory(c7a)

# descend into testsuite
add_subdirectory(tests)

# descend into example programs source
add_subdirectory(examples)

add_subdirectory(benchmarks)

if(SWIG_FOUND)
  # build swig frontends
  add_subdirectory(swig)
endif()

###############################################################################
### cmake script TRY_COMPILE all stxxl header files

if(TRY_COMPILE_HEADERS)

  include(CheckCXXSourceCompiles)
  set(CMAKE_REQUIRED_FLAGS ${CMAKE_CXX_FLAGS})
  set(CMAKE_REQUIRED_INCLUDES ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/extlib/cereal/include)
  set(CMAKE_REQUIRED_LIBRARIES
    ${CMAKE_BINARY_DIR}/c7a/libc7a.a ${ALL_LIBRARIES})

  file(GLOB_RECURSE header_files FOLLOW_SYMLINKS "c7a/*.hpp")
  list(SORT header_files)

  foreach(file ${header_files})
    # replace / to _ to fix warnings
    string(REPLACE "/" "_" compilename "${file}")
    string(REPLACE "." "_" compilename "${compilename}")

    check_cxx_source_compiles(
      "#include \"${file}\"
      int main() { return 0; }" IsSelfContained${compilename})

    if(NOT IsSelfContained${compilename})
      message(FATAL
        "Compilation FAILED for ${file}\n\nCompiler output:\n${OUTPUT}")
    endif()
  endforeach()

endif(TRY_COMPILE_HEADERS)

################################################################################
