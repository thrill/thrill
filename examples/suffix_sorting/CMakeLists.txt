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
  prefix_doubling.cpp prefix_quadrupling.cpp dc3.cpp dc7.cpp)

# disable debug symbols on large files
set_source_files_properties(
  prefix_doubling.cpp dc3.cpp dc7.cpp PROPERTIES COMPILE_FLAGS "-g0")

thrill_build_prog(wavelet_tree)
thrill_build_prog(wavelet_tree2)
thrill_build_prog(wavelet_tree3)
thrill_build_prog(rl_bwt)
thrill_build_prog(just_sort)

thrill_test_multiple(prefix_doubling1_aaa
  suffix_sorting -c -s 10000 -a pdw unary)

thrill_test_multiple(prefix_doubling1_random
  suffix_sorting -c -s 100000 -a pdw random)

thrill_test_multiple(prefix_doubling2_aaa
  suffix_sorting -c -s 10000 -a pds unary)

thrill_test_multiple(prefix_doubling2_random
  suffix_sorting -c -s 100000 -a pds random)

thrill_test_multiple(prefix_doubling3_aaa
  suffix_sorting -c -s 10000 -a dis unary)

thrill_test_multiple(prefix_doubling3_random
  suffix_sorting -c -s 100000 -a dis random)

thrill_test_multiple(prefix_quadrupling1_aaa
  suffix_sorting -c -s 10000 -a q unary)

thrill_test_multiple(prefix_quadrupling1_random
  suffix_sorting -c -s 100000 -a q random)

thrill_test_multiple(prefix_quadrupling2_aaa
  suffix_sorting -c -s 10000 -a qd unary)

thrill_test_multiple(prefix_quadrupling2_random
  suffix_sorting -c -s 100000 -a qd random)

thrill_test_multiple(dc3_aaa
  suffix_sorting -a dc3 -c -s 10000 unary)

thrill_test_multiple(dc3_random
  suffix_sorting -a dc3 -c -s 100000 random)

thrill_test_multiple(dc7_aaa
  suffix_sorting -a dc7 -c -s 10000 unary)

thrill_test_multiple(dc7_random
  suffix_sorting -a dc7 -c -s 100000 random)

if(MSVC)
  # requires /bigobj flag to build
  set_target_properties(suffix_sorting PROPERTIES COMPILE_FLAGS /bigobj)
endif()

################################################################################
