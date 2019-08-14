################################################################################
# examples/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
# Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

add_subdirectory(bfs)
add_subdirectory(k-means)
add_subdirectory(logistic_regression)
add_subdirectory(page_rank)
add_subdirectory(percentiles)
add_subdirectory(select)
add_subdirectory(sleep)
add_subdirectory(stochastic_gradient_descent)
add_subdirectory(terasort)
add_subdirectory(tpch)
add_subdirectory(triangles)
add_subdirectory(tutorial)
add_subdirectory(vfs_tool)
add_subdirectory(word_count)

if(THRILL_BUILD_EXAMPLES_MORE)
  add_subdirectory(suffix_sorting)
endif()

################################################################################
