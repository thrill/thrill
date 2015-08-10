################################################################################
# swig/perl5/CMakeLists.txt
#
# CMake file for swig perl5 frontend
################################################################################

find_package(Perl REQUIRED)
find_package(PerlLibs REQUIRED)
include_directories(${PERL_INCLUDE_PATH})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(SWIG_MODULE_c7a_perl5_EXTRA_DEPS
  c7a_perl5.hpp
  )

set_source_files_properties(c7a_perl5.i PROPERTIES
  CPLUSPLUS ON
  SWIG_MODULE_NAME c7a
  SWIG_FLAGS "-Wall;-O;")

swig_add_module(c7a_perl5 perl5 c7a_perl5.i)
swig_link_libraries(c7a_perl5 c7a ${ALL_LIBRARIES} ${PERL_LIBRARY})
# fixup target shared object name, such that multiple swig target can be made
set_target_properties(c7a_perl5 PROPERTIES OUTPUT_NAME "c7a")

add_test(
  NAME swig_perl5_test
  COMMAND ${PERL_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/perl5_test.pl)
set_tests_properties(swig_perl5_test PROPERTIES
  ENVIRONMENT PERL5LIB=${CMAKE_CURRENT_BINARY_DIR})

################################################################################
