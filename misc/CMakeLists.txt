################################################################################
# misc/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  thrill_build_prog(standalone_profiler)
endif()

if(THRILL_USE_AWS)
  thrill_build_prog(aws/config_writer)
endif()
thrill_build_prog(json2profile)
thrill_build_prog(memprofile2stats)

################################################################################
