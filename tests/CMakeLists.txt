################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

include_directories(${gtest_SOURCE_DIR}/include)
# ADD NEW TESTS HERE!
set(unit_test_sources
    data/test_data_manager.cpp
    data/test_block_iterator.cpp
    data/test_serializer.cpp
)
add_executable(unit_tests ${unit_test_sources})
target_link_libraries(unit_tests gtest_main)

################################################################################
