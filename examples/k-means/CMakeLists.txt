################################################################################
# examples/k-means/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

thrill_build_prog(k-means_run)

if(MSVC)
  # requires /bigobj flag to build
  set_target_properties(k-means_run PROPERTIES COMPILE_FLAGS /bigobj)
endif()

################################################################################
