################################################################################
# examples/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

add_executable(table hashtable/bench_table.cpp)

target_link_libraries(table c7a ${ALL_LIBRARIES})

################################################################################
