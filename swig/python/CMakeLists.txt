################################################################################
# swig/python/CMakeLists.txt
#
# CMake file for swig python frontend
################################################################################

find_package(PythonInterp REQUIRED)
find_package(PythonLibs REQUIRED)
include_directories(${PYTHON_INCLUDE_PATH})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(SWIG_MODULE_c7a_EXTRA_DEPS
  c7a_python.hpp
  )

set_source_files_properties(c7a.i PROPERTIES CPLUSPLUS ON)
set_source_files_properties(c7a.i PROPERTIES SWIG_FLAGS "-Wall;-O;-threads")

swig_add_module(c7a python c7a.i)
swig_link_libraries(c7a c7a ${ALL_LIBRARIES} ${PYTHON_LIBRARIES})

add_test(
  NAME python_test
  COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test.py ${ARGN})
set_tests_properties(python_test PROPERTIES
  ENVIRONMENT PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR})

################################################################################
