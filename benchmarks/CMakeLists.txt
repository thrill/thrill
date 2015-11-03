################################################################################
# benchmarks/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

# macro for registering test programs: maybe prepend valgrind, add environment
# Usage: thrill_test_single(testname "ENV=1;ENV2=1" program args)
macro(thrill_test_single TESTNAME ENVIRON PROG)

  if(USE_VALGRIND)
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
  set_tests_properties(${TESTNAME}
    PROPERTIES ENVIRONMENT
    "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1;${ENVIRON}")

endmacro(thrill_test_single)

thrill_build_prog(hashtable/bench_bucket_hashtable)
thrill_build_prog(hashtable/bench_probing_hashtable)

thrill_build_prog(data/disk)
thrill_build_prog(data/stream)
thrill_build_prog(data/data_benchmark)


thrill_build_prog(io/string_test)

if(NOT MSVC)
  # these benchmarks use a non-portable method to initialize the network,
  # TODO(ts): change to use RunXYZ().
  thrill_build_prog(data/stream_read_write)
  thrill_build_prog(data/stream_a_to_b)
  thrill_build_prog(data/stream_scatter)
  file(COPY "data/run_data_benchmark.sh" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endif()

thrill_build_prog(sort/generate_data)
thrill_build_prog(sort/sort)

thrill_build_prog(serialization/bench_serialization)

add_subdirectory(word_count)
add_subdirectory(page_rank)
add_subdirectory(net)
add_subdirectory(merge)

################################################################################
