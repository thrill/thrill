################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

include_directories(${gtest_SOURCE_DIR}/include)
set(test_sources
    data/test_data_manager.cpp
)
add_executable(unit_tests ${test_sources})
target_link_libraries(unit_tests gtest_main)

################################################################################
