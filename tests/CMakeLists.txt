################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

include_directories(${gtest_SOURCE_DIR}/include)
# ADD NEW TESTS HERE! (alphabetical order)
set(unit_test_sources
    api/test_simple_api.cpp
    communication/test_net_dispatcher.cpp
    data/test_data_manager.cpp
    data/test_block_iterator.cpp
    data/test_serializer.cpp
    engine/mock-network.cpp
)
add_executable(unit_tests ${unit_test_sources})
target_link_libraries(unit_tests gtest_main)

################################################################################
