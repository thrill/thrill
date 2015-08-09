################################################################################
# swig/java/CMakeLists.txt
#
# CMake file for swig java frontend
################################################################################

find_package(Java REQUIRED COMPONENTS Runtime Development)
include(UseJava)

find_package(JNI REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(SWIG_MODULE_c7a_java_EXTRA_DEPS
  c7a_java.hpp
  )

# unclear if this conflicts with other swig runs? -tb
set(CMAKE_SWIG_OUTDIR "${CMAKE_CURRENT_BINARY_DIR}/java")

set_source_files_properties(c7a_java.i PROPERTIES
  CPLUSPLUS ON
  SWIG_MODULE_NAME c7a
  SWIG_FLAGS "-Wall;-O;-package;c7a")

swig_add_module(c7a_java java c7a_java.i)
swig_link_libraries(c7a_java c7a ${ALL_LIBRARIES})
set_target_properties(c7a_java PROPERTIES OUTPUT_NAME "c7a")

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/classes")

add_custom_command(OUTPUT c7a.jar
  DEPENDS c7a_java
  COMMAND "${Java_JAVAC_EXECUTABLE}" -d classes java/*.java
  COMMAND "${Java_JAR_EXECUTABLE}" -cf c7a.jar -C classes .
  )
set(c7a_JAR "${CMAKE_CURRENT_BINARY_DIR}/c7a.jar")

add_jar(JavaTest
  SOURCES "JavaTest.java"
  INCLUDE_JARS ${c7a_JAR}
  ENTRY_POINT JavaTest)

add_test(
  NAME swig_java_test
  COMMAND ${Java_JAVA_EXECUTABLE} -cp c7a.jar:JavaTest.jar JavaTest
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
set_tests_properties(swig_java_test PROPERTIES
  ENVIRONMENT LD_LIBRARY_PATH=.)

################################################################################
