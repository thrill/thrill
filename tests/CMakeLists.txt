################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

file(GLOB_RECURSE SRCS */*.cpp)

add_executable(c7atest ${SRCS})

target_link_libraries(c7atest
  c7a
  libgtest
  libgmock
  pthread
)

add_test(c7atest c7atest)

install(TARGETS c7atest DESTINATION build)

################################################################################
