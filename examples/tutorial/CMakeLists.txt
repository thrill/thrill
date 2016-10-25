################################################################################
# examples/tutorial/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

thrill_build_prog(k-means_step1)
thrill_build_prog(k-means_step2)
thrill_build_prog(k-means_step3)
thrill_build_prog(k-means_step4)
thrill_build_prog(k-means_step5)
if(Boost_FOUND)
  thrill_build_prog(k-means_step6)
endif()

################################################################################
