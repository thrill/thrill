################################################################################
# benchmarks/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

thrill_build_prog(duplicates/bench_golomb)

thrill_build_prog(io/benchmark_files)
thrill_build_prog(io/benchmark_disks)
thrill_build_prog(io/benchmark_disks_random)

thrill_build_prog(hashtable/bench_hashtable)
thrill_build_prog(hashtable/generate_data)
thrill_build_prog(hashtable/reduce)

thrill_build_prog(serialization/bench_serialization)
thrill_build_prog(serialization/cpp-serializers)

add_subdirectory(api)
add_subdirectory(data)
add_subdirectory(net)

if(THRILL_FULL_BUILD)
  add_subdirectory(chaining)
endif()

################################################################################
