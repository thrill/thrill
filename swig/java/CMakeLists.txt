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

set(SWIG_MODULE_thrill_java_EXTRA_DEPS
  thrill_java.hpp
  )

# unclear if this conflicts with other swig runs? -tb
set(CMAKE_SWIG_OUTDIR "${CMAKE_CURRENT_BINARY_DIR}/source")

set_source_files_properties(thrill_java.i PROPERTIES
  CPLUSPLUS ON
  SWIG_MODULE_NAME thrill
  SWIG_FLAGS "-Wall;-O;-package;thrill")

swig_add_module(thrill_java java thrill_java.i)
swig_link_libraries(thrill_java thrill ${ALL_LIBRARIES})
set_target_properties(thrill_java PROPERTIES OUTPUT_NAME "thrill")

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/classes")

add_custom_command(OUTPUT thrill.jar
  DEPENDS thrill_java
  COMMAND "${Java_JAVAC_EXECUTABLE}" -d classes "source/*.java" "${CMAKE_CURRENT_SOURCE_DIR}/thrill/*.java"
  COMMAND "${Java_JAR_EXECUTABLE}" -cf thrill.jar -C classes .
  )
set(thrill_JAR "${CMAKE_CURRENT_BINARY_DIR}/thrill.jar")

add_jar(JavaTest
  SOURCES "JavaTest.java"
  INCLUDE_JARS ${thrill_JAR}
  ENTRY_POINT JavaTest)

add_test(
  NAME swig_java_test
  COMMAND ${Java_JAVA_EXECUTABLE} -cp thrill.jar:JavaTest.jar JavaTest
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
set_tests_properties(swig_java_test PROPERTIES
  ENVIRONMENT LD_LIBRARY_PATH=.)

################################################################################
