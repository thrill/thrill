################################################################################
# thrill/CMakeLists.txt
#
# CMake file for Thrill library
################################################################################

# glob general sources
file(GLOB SRCS
  RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/*/*.[ch]pp
  ${CMAKE_CURRENT_SOURCE_DIR}/*/mock/*.[ch]pp
  )

# add net/tcp on all platforms except Windows
if(NOT MSVC)
  file(GLOB NET_TCP_SRCS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/net/tcp/*.[ch]pp
    )
  list(APPEND SRCS ${NET_TCP_SRCS})
endif()

add_library(thrill ${SRCS})

################################################################################
