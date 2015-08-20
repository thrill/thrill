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

set(SWIG_MODULE_thrill_perl5_EXTRA_DEPS
  thrill_perl5.hpp
  )

set_source_files_properties(thrill_perl5.i PROPERTIES
  CPLUSPLUS ON
  SWIG_MODULE_NAME thrill
  SWIG_FLAGS "-Wall;-O;")

swig_add_module(thrill_perl5 perl5 thrill_perl5.i)
swig_link_libraries(thrill_perl5 thrill ${ALL_LIBRARIES} ${PERL_LIBRARY})
# fixup target shared object name, such that multiple swig target can be made
set_target_properties(thrill_perl5 PROPERTIES OUTPUT_NAME "thrill")

add_test(
  NAME swig_perl5_test
  COMMAND ${PERL_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/perl5_test.pl)
set_tests_properties(swig_perl5_test PROPERTIES
  ENVIRONMENT PERL5LIB=${CMAKE_CURRENT_BINARY_DIR})

################################################################################
