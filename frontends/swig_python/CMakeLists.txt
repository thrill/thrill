################################################################################
# frontends/swig_python/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

find_package(PythonInterp REQUIRED)
find_package(PythonLibs REQUIRED)
include_directories(${PYTHON_INCLUDE_PATH})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(SWIG_MODULE_thrill_python_EXTRA_DEPS
  thrill_python.hpp)

set_source_files_properties(thrill_python.i PROPERTIES
  CPLUSPLUS ON
  SWIG_MODULE_NAME thrill
  SWIG_FLAGS "-Wall;-O;-threads")

swig_add_module(thrill_python python thrill_python.i)
swig_link_libraries(thrill_python thrill ${PYTHON_LIBRARIES})

# fixup target shared object name, such that multiple swig target can be made
set_target_properties(_thrill_python PROPERTIES OUTPUT_NAME "_thrill")

# silence warnings in swig generated code
set_join(NO_WARN
  "-Wno-cast-qual -Wno-missing-field-initializers"
  "-Wno-deprecated-register -Wno-self-assign -Wno-sometimes-uninitialized"
  "-Wno-conversion -Wno-deprecated")
set_source_files_properties(thrill_pythonPYTHON_wrap.cxx
  PROPERTIES COMPILE_FLAGS "${NO_WARN}")

# run python test
add_test(
  NAME swig_python_test
  COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/python_test.py)

set_tests_properties(swig_python_test PROPERTIES
  ENVIRONMENT PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR})

################################################################################
