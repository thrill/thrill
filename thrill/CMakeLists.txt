################################################################################
# thrill/CMakeLists.txt
#
# CMake file for Thrill library
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

# glob general sources
file(GLOB SRCS
  RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/common/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/mem/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/io/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/core/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/data/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/net/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/api/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/net/mock/*.[ch]pp
  )

# add net/tcp on all platforms except Windows
if(NOT MSVC)
  file(GLOB NET_TCP_SRCS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/net/tcp/*.[ch]pp
    )
  list(APPEND SRCS ${NET_TCP_SRCS})
endif()

# add net/mpi if MPI is wanted
if(MPI_FOUND)
  file(GLOB NET_MPI_SRCS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/net/mpi/*.[ch]pp
    )
  list(APPEND SRCS ${NET_MPI_SRCS})
endif()

# add net/ib if IB/MPI is wanted
if(THRILL_USE_IB)
  file(GLOB NET_IB_SRCS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/net/ib/*.[ch]pp
    )
  list(APPEND SRCS ${NET_IB_SRCS})
endif()

add_library(thrill ${SRCS})
target_link_libraries(thrill ${THRILL_DEP_LIBRARIES})

################################################################################
