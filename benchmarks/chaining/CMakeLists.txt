################################################################################
# benchmarks/chaining/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

thrill_build_prog(cache_count)
thrill_build_prog(collapse_count)
thrill_build_prog(chain_count)

file(COPY "${CMAKE_SOURCE_DIR}/tests/inputs/wordcount.in"
  DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

file(COPY "${CMAKE_SOURCE_DIR}/tests/inputs/headwords"
  DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

################################################################################
