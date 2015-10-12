################################################################################
# CMakeLists.txt
#
# Root CMake build script for Thrill.
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

cmake_minimum_required(VERSION 2.8)

# custom cmake scripts
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/misc/cmake)

# project: we only use C++
project(thrill CXX)

# prohibit in-source builds
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(SEND_ERROR "In-source builds are not allowed.")
endif()

# default to Debug building for single-config generators
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message("Defaulting CMAKE_BUILD_TYPE to Debug")
  set(CMAKE_BUILD_TYPE "Debug")
endif()

option(USE_GCOV "Compile and run tests with gcov for coverage analysis." OFF)

option(USE_VALGRIND "Run tests with valgrind, reports using XML files." OFF)

option(USE_IWYU "Compile with include-what-you-use (iwyu) tool." OFF)

option(THRILL_USE_TBB "Use (optional) Intel TBB library if available." ON)

option(THRILL_USE_MPI "Use (optional) MPI net backend." ON)

option(THRILL_USE_SWIG "Use (optional) swig wrapper generator." ON)

option(THRILL_REQUIRE_SWIG "Make swig wrapper generator mandatory." OFF)

option(TRY_COMPILE_HEADERS "Test header files for self-sufficiency: try to compile them." OFF)

# BUILD_SHARED_LIBS is a standard CMake variable, but we declare it here to
# make it prominent in the GUI.
option(BUILD_SHARED_LIBS "Build shared libraries (DLLs)." OFF)

option(ENABLE_STATS "enable stats" OFF)
if(ENABLE_STATS)
    message(STATUS "Add Stats support")
    add_definitions(-DENABLE_STATS=1)
else()
    add_definitions(-DENABLE_STATS=0)
endif()

if(NOT MSVC)
  # enable warnings
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -g -W -Wall -Wextra -fPIC")

  # enable more warnings
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wpedantic")

  # warn on conversions
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wconversion -Werror")

  # enable AddressSanitizer
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")

  # enable ThreadSanitizer
  if(OFF)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -pie -fPIC")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DTHRILL_HAVE_THREAD_SANITIZER=1")
    set(THRILL_USE_MPI OFF)
    set(THRILL_USE_TBB OFF)
  endif()

  # enable UndefinedBehaviorSanitizer
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")

  # enable extra warnings on gcc
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wcast-qual")
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
  # enable extra warnings on clang
  if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wdeprecated")
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wabstract-vbase-init")
  endif()

  # Clang < 3.6 0 (?) does not support debug output for auto return types yet.
  # try compiling a platform test for auto return types
  if(ON)
    include(CheckCXXSourceCompiles)
    set(OLD_CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS}")
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_CXX_FLAGS} -g")

    check_cxx_source_compiles(
      "template <typename T> struct A { auto func(int i) { return 42 + i; } };
     int main() { A<int> a; return 0; }"
      THRILL_CLANG_AUTO_RETURN_DEBUG_INFO)

    if (NOT THRILL_CLANG_AUTO_RETURN_DEBUG_INFO)
      message(STATUS "compiler does not support -g debug info with auto returns")
      string(REPLACE "-g" "" CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
      string(REPLACE "-g" "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
    endif()

    set(CMAKE_REQUIRED_FLAGS "${OLD_CMAKE_REQUIRED_FLAGS}")
  endif()
elseif(MSVC)
  # Force to always compile with W4
  if(CMAKE_CXX_FLAGS MATCHES "/W[0-4]")
    string(REGEX REPLACE "/W[0-4]" "/W4" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W4")
  endif()
  # raise warnings as errors
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX")

  ### disable verbose warnings:
  # warning C4589: Constructor of abstract class '...' ignores initializer for
  # virtual base class '...' (false positive warnings)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4589")
  # warning C4127: conditional expression is constant
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4127")
  # warning C4458: declaration of '...' hides class member
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4458")
  # warning C4459: declaration of '...' hides global declaration
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4459")
  # warning C4702: unreachable code
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4702")
  # disable lots of warnings about "unsecure" C runtime function
  add_definitions(-D_CRT_SECURE_NO_WARNINGS)
  # disable "The POSIX name for this item is deprecated. Instead, use the ISO C
  # and C++ conformant name.", Nope. We will not.
  add_definitions(-D_CRT_NONSTDC_NO_DEPRECATE)
  # disable lots of warnings about "unsecure" STL functions
  add_definitions(-D_SCL_SECURE_NO_WARNINGS)
  # windef.h bizzarly defines min and max as macros, unless this is defined.
  add_definitions(-DNOMINMAX)
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

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

###############################################################################

# set_join(VAR "foo" "bar" "abc") sets VAR="foo bar abc"
macro(set_join var)
  set(${var})
  foreach(v ${ARGN})
    set(${var} "${${var}} ${v}")
  endforeach()
  string(STRIP ${var} "${${var}}")
endmacro(set_join)

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
    COMMAND ${LCOV} -q --remove lcov.info '/usr/*' '*/extlib/*' ${LCOV_FLAGS} --output-file lcov-clean.info
    COMMAND ${GENHTML} -q -o coverage --title "Thrill ${GITDESC}" --prefix ${PROJECT_SOURCE_DIR} ${GENHTML_FLAGS} lcov-clean.info
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

### google test + mock - enable "make test" and add_test()

# this fixes compilation with static libs on MSVC
set(gtest_force_shared_crt ON CACHE BOOL "on" FORCE)
add_subdirectory(extlib/googletest/googletest)
include_directories(SYSTEM ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

### general valgrind setting, if run with USE_VALGRIND=ON.

set(THRILL_VALGRIND_OPTS
  --leak-check=full --track-origins=yes)
#  --error-exitcode=1)
#  --suppressions=${PROJECT_SOURCE_DIR}/misc/valgrind.supp)

###############################################################################
# check some required system functions

include(CheckFunctionExists)
check_function_exists(pipe2 THRILL_HAVE_PIPE2)
if(THRILL_HAVE_PIPE2)
  add_definitions(-DTHRILL_HAVE_PIPE2=1)
endif()

###############################################################################
# add cereal

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/extlib/cereal/include)

################################################################################
### Find Required Libraries

# find pthreads

find_package(Threads REQUIRED)
set(ALL_LIBRARIES ${CMAKE_THREAD_LIBS_INIT} ${ALL_LIBRARIES})

# use dl (dynamic linker library)

set(ALL_LIBRARIES ${ALL_LIBRARIES} ${CMAKE_DL_LIBS})

# try to find Intel TBB (optional)

if(THRILL_USE_TBB)
  find_package(TBB)
endif()

if(NOT TBB_FOUND)
  message(STATUS "Intel TBB not found. No problem, using slower replacements.")
  add_definitions(-DTHRILL_HAVE_INTELTBB=0)
elseif(TBB_INTERFACE_VERSION LESS 8000)
  message(STATUS "Intel TBB ${TBB_INTERFACE_VERSION} is too old. "
    "No problem, using slower replacements.")
  add_definitions(-DTHRILL_HAVE_INTELTBB=0)
else(NOT TBB_FOUND)
  include_directories(${TBB_INCLUDE_DIRS})
  link_directories(${TBB_LIBRARY_DIRS})
  set(ALL_LIBRARIES ${TBB_LIBRARIES} ${ALL_LIBRARIES})
  add_definitions(-DTHRILL_HAVE_INTELTBB=1)
endif()

# use MPI library (optional)

if(THRILL_USE_MPI)
  find_package(MPI)

  if(NOT MPI_FOUND)
    message(STATUS "No MPI library found. No problem, it is optional.")
  else()
    include_directories(${MPI_INCLUDE_PATH})
    set(ALL_LIBRARIES ${MPI_LIBRARIES} ${ALL_LIBRARIES})
    add_definitions(-DTHRILL_HAVE_NET_MPI=1)
  endif()
endif()

# example: use Boost libraries:

#find_package(Boost 1.42.0 REQUIRED COMPONENTS regex locale serialization)
#include_directories(${Boost_INCLUDE_DIRS})
#set(ALL_LIBRARIES ${Boost_LIBRARIES} ${ALL_LIBRARIES})
#message("Final Boost include dir: ${Boost_INCLUDE_DIRS}")

# SWIG (optional)

if(THRILL_USE_SWIG)
  find_package(SWIG)
  if(SWIG_FOUND)
    include(${SWIG_USE_FILE})
  endif()
  if(NOT SWIG_FOUND AND THRILL_REQUIRE_SWIG)
    message(FATAL "Swig was not found but is mandatory.")
  endif()
endif()

################################################################################
### Build Macros

# macro for building main thrill programs with correct libraries
macro(thrill_build TARGETNAME)

  add_executable(${TARGETNAME} ${ARGN})
  target_link_libraries(${TARGETNAME} thrill ${ALL_LIBRARIES})

endmacro(thrill_build)

# macro for building a single-source thrill program
macro(thrill_build_prog PROGNAME)

  string(REPLACE "/" "_" TARGETNAME "${PROGNAME}") # replace slashes

  thrill_build(${TARGETNAME} ${PROGNAME}.cpp ${ARGN})

endmacro(thrill_build_prog)

################################################################################
### Descend into Subdirectories

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# descend into library source
add_subdirectory(thrill)

# descend into testsuite
add_subdirectory(tests)

# descend into benchmarks and examples programs
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
    ${CMAKE_BINARY_DIR}/thrill/libthrill.a ${ALL_LIBRARIES})

  file(GLOB_RECURSE header_files FOLLOW_SYMLINKS "thrill/*.hpp")
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
