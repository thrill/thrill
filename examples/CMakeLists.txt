################################################################################
# examples/CMakeLists.txt
#
# CMake file for c7a example programs.
################################################################################

# real apps
add_executable(word_count word_count.cpp)
add_executable(wc word_count_simple.cpp)

target_link_libraries(word_count c7a ${ALL_LIBRARIES} pthread)
target_link_libraries(wc c7a ${ALL_LIBRARIES} pthread)

################################################################################
