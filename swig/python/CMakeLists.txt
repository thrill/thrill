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

set(SWIG_MODULE_c7a_python_EXTRA_DEPS
  c7a_python.hpp
  )

set_source_files_properties(c7a_python.i PROPERTIES
  CPLUSPLUS ON
  SWIG_MODULE_NAME c7a
  SWIG_FLAGS "-Wall;-O;-threads")

swig_add_module(c7a_python python c7a_python.i)
swig_link_libraries(c7a_python c7a ${ALL_LIBRARIES} ${PYTHON_LIBRARIES})
# fixup target shared object name, such that multiple swig target can be made
set_target_properties(_c7a_python PROPERTIES OUTPUT_NAME "_c7a")

add_test(
  NAME swig_python_test
  COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/python_test.py)
set_tests_properties(swig_python_test PROPERTIES
  ENVIRONMENT PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR})

################################################################################
