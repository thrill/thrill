################################################################################
# benchmarks/word_count/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

file(COPY "${CMAKE_SOURCE_DIR}/tests/inputs/wordcount.in"
  DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

file(COPY "${CMAKE_SOURCE_DIR}/tests/inputs/headwords"
  DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

################################################################################
