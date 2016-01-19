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

thrill_build_prog(io/benchmark_files)
thrill_build_prog(io/benchmark_disks)
thrill_build_prog(io/benchmark_disks_random)

if(FULL_BUILD)
thrill_build_prog(hashtable/bench_bucket_hashtable)
thrill_build_prog(hashtable/bench_probing_hashtable)
thrill_build_prog(hashtable/generate_data)
thrill_build_prog(hashtable/reduce)

thrill_build_prog(serialization/bench_serialization)
thrill_build_prog(serialization/cpp-serializers)

add_subdirectory(api)
add_subdirectory(data)
add_subdirectory(dc3)
add_subdirectory(net)
add_subdirectory(page_rank)
add_subdirectory(word_count)
endif()

################################################################################
