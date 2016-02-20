################################################################################
# frontends/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

### Check for SWIG

option(THRILL_USE_SWIG "Use (optional) swig wrapper generator." ON)
option(THRILL_REQUIRE_SWIG "Make swig wrapper generator mandatory." OFF)

if(THRILL_USE_SWIG)
  find_package(SWIG)
  if(SWIG_FOUND)
    include(${SWIG_USE_FILE})
    add_subdirectory(swig_python)
  endif()
  if(NOT SWIG_FOUND AND THRILL_REQUIRE_SWIG)
    message(FATAL "Swig was not found but is mandatory.")
  endif()
endif()

################################################################################
