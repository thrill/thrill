################################################################################
# CMakeLists.txt
#
# Root CMake build script for Thrill.
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

cmake_minimum_required(VERSION 2.8)

# custom cmake scripts
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/misc/cmake)

# project: we only use C++
project(thrill)

# prohibit in-source builds
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(SEND_ERROR "In-source builds are not allowed.")
endif()

# default to Debug building for single-config generators
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message("Defaulting CMAKE_BUILD_TYPE to Debug")
  set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Build type")
endif()

# THRILL_USE_ZLIB tristate switch
set(THRILL_USE_ZLIB AUTO CACHE
  STRING "Use (optional) zlib for transparent .gz compression/decompression.")
set_property(CACHE THRILL_USE_ZLIB PROPERTY STRINGS AUTO ON OFF)

# THRILL_USE_BZIP2 tristate switch
set(THRILL_USE_BZIP2 AUTO CACHE
  STRING "Use (optional) bzip2 for transparent .bz2 compression/decompression.")
set_property(CACHE THRILL_USE_BZIP2 PROPERTY STRINGS AUTO ON OFF)

# THRILL_USE_MPI tristate switch
set(THRILL_USE_MPI AUTO CACHE STRING "Use (optional) MPI net backend.")
set_property(CACHE THRILL_USE_MPI PROPERTY STRINGS AUTO ON OFF)

option(THRILL_USE_TBB
  "Use (optional) Intel TBB library if available." ON)

option(THRILL_USE_JEMALLOC
  "Use (optional) JeMalloc allocation library if available." ON)

option(THRILL_USE_GCOV
  "Compile and run tests with gcov for coverage analysis." OFF)

option(THRILL_USE_VALGRIND
  "Run tests with valgrind, reports using XML files." OFF)

option(THRILL_USE_IWYU
  "Compile with include-what-you-use (iwyu) tool." OFF)

option(THRILL_USE_LTO
  "Compile with -flto (link-time optimization)." OFF)

option(THRILL_FULL_BUILD
  "Disable large compilation units for faster development." OFF)

option(THRILL_TRY_COMPILE_HEADERS
  "Test header files for self-sufficiency: try to compile them." OFF)

# THRILL_USE_S3 tristate switch
set(THRILL_USE_S3 AUTO CACHE
  STRING "Build libs3 as library. Enabled by default if all dependencies are found.")
set_property(CACHE THRILL_USE_S3 PROPERTY STRINGS AUTO ON OFF)

option(THRILL_USE_HDFS3
  "Download and build with libhdfs3 for hdfs:// support." OFF)

# BUILD_SHARED_LIBS is a standard CMake variable, but we declare it here to
# make it prominent in the GUI.
option(BUILD_SHARED_LIBS "Build shared libraries (DLLs)." OFF)

# variables to collect compile-time definitions, include dirs, and libraries
set(THRILL_DEFINITIONS "")
set(THRILL_INCLUDE_DIRS "")
set(THRILL_LINK_LIBRARIES "")

if(NOT MSVC)
  # enable warnings
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -g -W -Wall -Wextra -fPIC")

  # enable more warnings
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wpedantic")

  # enable -march=native on Release builds
  if(CMAKE_BUILD_TYPE STREQUAL "Release")
    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag(-march=native THRILL_HAS_MARCH_NATIVE)
    if(THRILL_HAS_MARCH_NATIVE)
      set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")
    endif()
  endif()

  # remove -rdynamic from linker flags (smaller binaries which cannot be loaded
  # with dlopen() -- something no one needs)
  string(REGEX REPLACE "-rdynamic" ""
    CMAKE_SHARED_LIBRARY_LINK_C_FLAGS "${CMAKE_SHARED_LIBRARY_LINK_C_FLAGS}")
  string(REGEX REPLACE "-rdynamic" ""
    CMAKE_SHARED_LIBRARY_LINK_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_LINK_CXX_FLAGS}")

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
  # warning C4250: ABC inherits XYZ via dominance
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4250")
  # warning C4503: decorated name length exceeded, name was truncated
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4503")
  # disable lots of warnings about "unsecure" C runtime function
  list(APPEND THRILL_DEFINITIONS "_CRT_SECURE_NO_WARNINGS")
  # disable "The POSIX name for this item is deprecated. Instead, use the ISO C
  # and C++ conformant name.", Nope. We will not.
  list(APPEND THRILL_DEFINITIONS "_CRT_NONSTDC_NO_DEPRECATE")
  # disable lots of warnings about "unsecure" STL functions
  list(APPEND THRILL_DEFINITIONS "_SCL_SECURE_NO_WARNINGS")
  # windef.h bizzarly defines min and max as macros, unless this is defined.
  list(APPEND THRILL_DEFINITIONS "NOMINMAX")
endif()

if(THRILL_USE_IWYU)
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

if(THRILL_USE_LTO)
  # build with link-time optimization
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag(-flto CXX_HAS_LTO_FLAG)

  if(CMAKE_BUILD_TYPE MATCHES Release AND CXX_HAS_LTO_FLAG)
    find_program(CMAKE_GCC_AR
      NAMES ${_CMAKE_TOOLCHAIN_PREFIX}gcc-ar${_CMAKE_TOOLCHAIN_SUFFIX}
      HINTS ${_CMAKE_TOOLCHAIN_LOCATION})

    find_program(CMAKE_GCC_NM
      NAMES ${_CMAKE_TOOLCHAIN_PREFIX}gcc-nm
      HINTS ${_CMAKE_TOOLCHAIN_LOCATION})

    find_program(CMAKE_GCC_RANLIB
      NAMES ${_CMAKE_TOOLCHAIN_PREFIX}gcc-ranlib
      HINTS ${_CMAKE_TOOLCHAIN_LOCATION})

    if(CMAKE_GCC_AR AND CMAKE_GCC_NM AND CMAKE_GCC_RANLIB)
      set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -flto")
      set(CMAKE_AR "${CMAKE_GCC_AR}")
      set(CMAKE_NM "${CMAKE_GCC_NM}")
      set(CMAKE_RANLIB "${CMAKE_GCC_RANLIB}")
    else()
      message(WARNING "GCC indicates LTO support, but binutils wrappers could not be found. Disabling LTO.")
    endif()
  endif()
endif()

if(APPLE)
  # disable warnings about "ranlib: file: libthrill.a(...cpp.o) has no symbols"
  set(CMAKE_C_ARCHIVE_FINISH   "<CMAKE_RANLIB> -no_warning_for_no_symbols -c <TARGET>")
  set(CMAKE_CXX_ARCHIVE_FINISH "<CMAKE_RANLIB> -no_warning_for_no_symbols -c <TARGET>")
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

if(THRILL_USE_GCOV)
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

endif(THRILL_USE_GCOV)

###############################################################################
# enable gtest framework, valgrind, and collection of results

enable_testing()
include(CTest)

### google test + mock - enable "make test" and add_test()

# this fixes compilation with static libs on MSVC
set(gtest_force_shared_crt ON CACHE BOOL "on" FORCE)
if(NOT MSVC)
  # silence some warnings
  set(GTEST_SAVE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers -Wno-deprecated")
endif()

add_subdirectory(extlib/googletest/googletest)

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS "${GTEST_SAVE_CXX_FLAGS}")
endif()

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
  list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_PIPE2=1")
endif()

###############################################################################
# add cereal

list(APPEND THRILL_INCLUDE_DIRS
  "${CMAKE_CURRENT_SOURCE_DIR}/extlib/cereal/include")

################################################################################
### Find Required Libraries

# find pthreads

find_package(Threads REQUIRED)
list(APPEND THRILL_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})

# use dl (dynamic linker library)

list(APPEND THRILL_LINK_LIBRARIES ${CMAKE_DL_LIBS})

# try to find jemalloc (optional)

if(THRILL_USE_JEMALLOC)
  find_package(JeMalloc)

  if(NOT JEMALLOC_FOUND)
    message(STATUS "jemalloc library found. No problem, it is optional,")
    message(STATUS "but consider installing it for systems with limited memory.")
  else()
    list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_JEMALLOC=1")
    list(APPEND THRILL_INCLUDE_DIRS ${JEMALLOC_INCLUDE_DIRS})
    list(APPEND THRILL_LINK_LIBRARIES ${JEMALLOC_LIBRARIES})
  endif()
endif()

# use ZLIB for transparent .gz compression/decompression

if(THRILL_USE_ZLIB STREQUAL "AUTO")
  find_package(ZLIB)
  if(ZLIB_FOUND)
    message("Using zlib for transparent .gz compression/decompression.")
    set(THRILL_USE_ZLIB ON)
  else()
    message("zlib not available (optional).")
    set(THRILL_USE_ZLIB OFF)
  endif()
endif()

if(THRILL_USE_ZLIB)
  find_package(ZLIB REQUIRED)

  list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_ZLIB=1")
  list(APPEND THRILL_INCLUDE_DIRS ${ZLIB_INCLUDE_DIRS})
  list(APPEND THRILL_LINK_LIBRARIES ${ZLIB_LIBRARIES})
endif()

# use BZLIB for transparent .bz2 compression/decompression

if(THRILL_USE_BZIP2 STREQUAL "AUTO")
  find_package(BZip2)
  if(BZIP2_FOUND)
    message("Using bzip2 for transparent .bz2 compression/decompression.")
    set(THRILL_USE_BZIP2 ON)
  else()
    message("bzip2 not available (optional).")
    set(THRILL_USE_BZIP2 OFF)
  endif()
endif()

if(THRILL_USE_BZIP2)
  find_package(BZip2 REQUIRED)

  list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_BZIP2=1")
  list(APPEND THRILL_INCLUDE_DIRS ${BZIP2_INCLUDE_DIRS})
  list(APPEND THRILL_LINK_LIBRARIES ${BZIP2_LIBRARIES})
endif()

# try to find libS3 (optional)

if(THRILL_USE_S3 STREQUAL "AUTO")
  # test for all dependencies of libS3
  find_package(CURL)
  find_package(LibXml2)
  find_package(OpenSSL)

  if(CURL_FOUND AND LIBXML2_FOUND AND OPENSSL_FOUND)
    message("All libS3 dependencies available.")
    set(THRILL_USE_S3 ON)
  else()
    message("libS3 not built as not all dependencies available.")
    set(THRILL_USE_S3 OFF)
  endif()
endif()

if(THRILL_USE_S3)
  find_package(CURL REQUIRED)
  find_package(LibXml2 REQUIRED)
  find_package(OpenSSL REQUIRED)

  # build libS3 as cmake library
  add_subdirectory(extlib/libs3)

  # add libS3 and dependencies to thrill's dependencies
  list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_LIBS3=1")
  list(APPEND THRILL_INCLUDE_DIRS
    "${CMAKE_CURRENT_SOURCE_DIR}/extlib/libs3/libs3/inc")
  list(APPEND THRILL_LINK_LIBRARIES s3)

  # saved code to detect libs3 on the system. use this once multipart upload is
  # mainstream.

  # find_package(LibS3)
  # if(NOT LibS3_FOUND)
  #   message(STATUS "libS3 not found. No problem, s3:// will not work.")
  #   list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_LIBS3=0")
  # else()
  #   list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_LIBS3=1")
  #   list(APPEND THRILL_INCLUDE_DIRS ${LibS3_INCLUDE_DIRS})
  #   list(APPEND THRILL_LINK_LIBRARIES ${LibS3_LIBRARIES})
  # endif()
endif()

# try to find libHDFS3 (optional)

if(THRILL_USE_HDFS3)

  include(ExternalProject)
  ExternalProject_Add(libhdfs3
    URL https://github.com/Pivotal-Data-Attic/pivotalrd-libhdfs3/archive/v2.2.31.tar.gz
    URL_MD5 7629f18ba81b638f5f47599e07eee28c
    PREFIX ${CMAKE_BINARY_DIR}/extlib/libhdfs3
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>)

  ExternalProject_Get_Property(libhdfs3 INSTALL_DIR)

  list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_LIBHDFS3=1")
  list(APPEND THRILL_INCLUDE_DIRS "${INSTALL_DIR}/include")
  add_library(hdfs3 UNKNOWN IMPORTED)
  set_target_properties(hdfs3 PROPERTIES
    IMPORTED_LOCATION "${INSTALL_DIR}/lib/libhdfs3.so")
  add_dependencies(hdfs3 libhdfs3)
  list(APPEND THRILL_LINK_LIBRARIES hdfs3)

endif()

# try to find Intel TBB (optional)

if(THRILL_USE_TBB)
  find_package(TBB)
endif()

if(NOT TBB_FOUND)
  message(STATUS "Intel TBB not found. No problem, using slower replacements.")
  list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_INTELTBB=0")
elseif(TBB_INTERFACE_VERSION LESS 8000)
  message(STATUS "Intel TBB ${TBB_INTERFACE_VERSION} is too old. "
    "No problem, using slower replacements.")
  list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_INTELTBB=0")
else(NOT TBB_FOUND)
  list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_INTELTBB=1")
  list(APPEND THRILL_INCLUDE_DIRS ${TBB_INCLUDE_DIRS})
  link_directories(${TBB_LIBRARY_DIRS})
  list(APPEND THRILL_LINK_LIBRARIES ${TBB_LIBRARIES})
endif()

# use MPI library (optional)

if(THRILL_USE_MPI STREQUAL "AUTO")
  find_package(MPI)

  if(NOT MPI_FOUND)
    message(STATUS "No MPI library found. No problem, it is optional.")
    set(THRILL_USE_MPI OFF)
  else()
    set(THRILL_USE_MPI ON)
  endif()
endif()

if(THRILL_USE_MPI)
  find_package(MPI REQUIRED)

  list(APPEND THRILL_DEFINITIONS "THRILL_HAVE_HAVE_NET_MPI=1")
  list(APPEND THRILL_INCLUDE_DIRS ${MPI_INCLUDE_PATH})
  list(APPEND THRILL_LINK_LIBRARIES ${MPI_LIBRARIES})
endif()

# build Infiniband/MPI net backend (optional)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/thrill/net/ib/CMakeLists.txt")
  include("${CMAKE_CURRENT_SOURCE_DIR}/thrill/net/ib/CMakeLists.txt")
endif()

# detect Boost libraries (optional). Currently no components linked.
# uses: Spirit.Qi in tutorial

find_package(Boost 1.42.0 COMPONENTS)
if(Boost_FOUND)
  list(APPEND THRILL_INCLUDE_DIRS ${Boost_INCLUDE_DIRS})
  list(APPEND THRILL_LINK_LIBRARIES ${Boost_LIBRARIES})
endif()

################################################################################
### Build Macros

# macro for building main thrill programs with correct libraries
macro(thrill_build TARGETNAME)

  add_executable(${TARGETNAME} ${ARGN})
  target_link_libraries(${TARGETNAME} thrill)

endmacro(thrill_build)

# macro for building a single-source thrill program
macro(thrill_build_prog PROGNAME)

  string(REPLACE "/" "_" TARGETNAME "${PROGNAME}") # replace slashes

  thrill_build(${TARGETNAME} ${PROGNAME}.cpp ${ARGN})

endmacro(thrill_build_prog)

# macro for registering test programs: maybe prepend valgrind, add environment
# Usage: thrill_test_single(testname "ENV=1;ENV2=1" program args)
macro(thrill_test_single TESTNAME ENVIRON PROG)

  if(THRILL_USE_VALGRIND)
    # prepend valgrind call
    add_test(
      NAME ${TESTNAME}
      COMMAND /usr/bin/valgrind ${THRILL_VALGRIND_OPTS}
      --xml=yes --xml-file=${TESTNAME}.xml
      ./${PROG} ${ARGN})
  else()
    add_test(
      NAME ${TESTNAME}
      COMMAND ${PROG} ${ARGN})
  endif()

  # environment of test run: set default and let ENVIRON override
  set(TEST_ENVIRON
    "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;"
    "THRILL_LOG=;THRILL_DIE_WITH_PARENT=;THRILL_UNLINK_BINARY=;"
    "${ENVIRON}")

  set_tests_properties(${TESTNAME} PROPERTIES ENVIRONMENT "${TEST_ENVIRON}")

endmacro(thrill_test_single)

# macro for registering test programs: prepends environment variables
# Usage: thrill_test_multiple(testname program args)
macro(thrill_test_multiple TESTNAME)

  thrill_test_single("${TESTNAME}3" "THRILL_LOCAL=3" ${ARGN})
  thrill_test_single("${TESTNAME}4" "THRILL_LOCAL=4" ${ARGN})

endmacro(thrill_test_multiple)

################################################################################
### Descend into Subdirectories

# descend into library source
add_subdirectory(thrill)

# descend into testsuite
add_subdirectory(tests)

# descend into benchmarks and examples programs
add_subdirectory(benchmarks)

# descend into examples source
add_subdirectory(examples)

# build frontends
add_subdirectory(frontends)

# build some miscellaneous helpers and programs
add_subdirectory(misc)

###############################################################################
### cmake script THRILL_TRY_COMPILE_HEADERS to compile all Thrill header files

if(THRILL_TRY_COMPILE_HEADERS)

  include(CheckCXXSourceCompiles)
  set(CMAKE_REQUIRED_FLAGS ${CMAKE_CXX_FLAGS})
  set(CMAKE_REQUIRED_INCLUDES ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/extlib/cereal/include)
  set(CMAKE_REQUIRED_LIBRARIES
    ${CMAKE_BINARY_DIR}/thrill/libthrill.a ${THRILL_LINK_LIBRARIES})

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

endif(THRILL_TRY_COMPILE_HEADERS)

################################################################################
