################################################################################
# examples/suffix_sorting/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
# Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
# Copyright (C) 2016 Simon Gog <gog@kit.edu>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

thrill_build_prog(suffix_sorting
  prefix_doubling.cpp dc3.cpp dc7.cpp sa_checker.cpp bwt_generator.cpp)

set_source_files_properties(
  prefix_doubling.cpp dc3.cpp dc7.cpp PROPERTIES COMPILE_FLAGS "-g0 -O1")

thrill_build_prog(wavelet_tree)
thrill_build_prog(wavelet_tree2)
thrill_build_prog(wavelet_tree3)
thrill_build_prog(rl_bwt)

thrill_test_single(prefix_doubling1_aaa3 "THRILL_LOCAL=3"
  suffix_sorting -c -s 10000 -a fl unary)
thrill_test_single(prefix_doubling1_aaa4 "THRILL_LOCAL=4"
  suffix_sorting -c -s 10000 -a fl unary)

thrill_test_single(prefix_doubling1_random3 "THRILL_LOCAL=3"
  suffix_sorting -c -s 100000 -a fl random)
thrill_test_single(prefix_doubling1_random4 "THRILL_LOCAL=4"
  suffix_sorting -c -s 100000 -a fl random)

thrill_test_single(prefix_doubling2_aaa3 "THRILL_LOCAL=3"
  suffix_sorting -c -s 10000 -a de unary)
thrill_test_single(prefix_doubling2_aaa4 "THRILL_LOCAL=4"
  suffix_sorting -c -s 10000 -a de unary)

thrill_test_single(prefix_doubling2_random3 "THRILL_LOCAL=3"
  suffix_sorting -c -s 100000 -a de random)
thrill_test_single(prefix_doubling2_random4 "THRILL_LOCAL=4"
  suffix_sorting -c -s 100000 -a de random)

if(MSVC)
  # requires /bigobj flag to build
  set_target_properties(suffix_sorting PROPERTIES COMPILE_FLAGS /bigobj)
endif()

if(THRILL_FULL_BUILD)

  if(MSVC)
    # requires /bigobj flag to build
    #set_target_properties(dc3 PROPERTIES COMPILE_FLAGS /bigobj)
    #set_target_properties(dc7 PROPERTIES COMPILE_FLAGS /bigobj)
  else()
    #set_target_properties(dc3 PROPERTIES COMPILE_FLAGS "-g0 -O1")
    #set_target_properties(dc7 PROPERTIES COMPILE_FLAGS "-g0 -O1")
  endif()

  # thrill_test_single(dc3_aaa3 "THRILL_LOCAL=3" dc3 -c -s 10000 unary)
  # thrill_test_single(dc3_aaa4 "THRILL_LOCAL=4" dc3 -c -s 10000 unary)

  # thrill_test_single(dc3_random3 "THRILL_LOCAL=3" dc3 -c -s 100000 random)
  # thrill_test_single(dc3_random4 "THRILL_LOCAL=4" dc3 -c -s 100000 random)

  # thrill_test_single(dc7_aaa3 "THRILL_LOCAL=3" dc7 -c -s 10000 unary)
  # thrill_test_single(dc7_aaa4 "THRILL_LOCAL=4" dc7 -c -s 10000 unary)

  # thrill_test_single(dc7_random3 "THRILL_LOCAL=3" dc7 -c -s 100000 random)
  # thrill_test_single(dc7_random4 "THRILL_LOCAL=4" dc7 -c -s 100000 random)
endif()

################################################################################
