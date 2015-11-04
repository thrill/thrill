################################################################################
# benchmarks/dc3/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

thrill_build_prog(dc3)

if(MSVC)
  # requires /bigobj flag to build
  set_target_properties(dc3 PROPERTIES COMPILE_FLAGS /bigobj)
endif()

thrill_test_single(dc3_aaa3 "THRILL_LOCAL=3" dc3 -c -s 1000 unary)
thrill_test_single(dc3_aaa4 "THRILL_LOCAL=4" dc3 -c -s 1000 unary)

thrill_test_single(dc3_random3 "THRILL_LOCAL=3" dc3 -c -s 1000 random)
thrill_test_single(dc3_random4 "THRILL_LOCAL=4" dc3 -c -s 1000 random)

################################################################################
