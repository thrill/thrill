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
file(GLOB THRILL_SRCS
  RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/api/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/common/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/core/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/data/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/mem/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/net/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/net/mock/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/vfs/*.[ch]pp
  )

# add net/tcp on all platforms except Windows
if(NOT MSVC)
  file(GLOB THRILL_NET_TCP_SRCS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/net/tcp/*.[ch]pp)

  list(APPEND THRILL_SRCS ${THRILL_NET_TCP_SRCS})
endif()

# add net/mpi if MPI is wanted
if(MPI_FOUND)
  file(GLOB THRILL_NET_MPI_SRCS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/net/mpi/*.[ch]pp)

  list(APPEND THRILL_SRCS ${THRILL_NET_MPI_SRCS})
endif()

add_library(thrill STATIC ${THRILL_SRCS})
target_compile_definitions(thrill PUBLIC ${THRILL_DEFINITIONS})
target_include_directories(thrill PUBLIC ${PROJECT_SOURCE_DIR})
target_include_directories(thrill SYSTEM PUBLIC ${THRILL_INCLUDE_DIRS})
target_link_libraries(thrill ${THRILL_LINK_LIBRARIES})

################################################################################
