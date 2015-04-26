################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

set(SRCS
    main.cpp
    api/test_simple_api.cpp
    # data/test_data_manager.cpp
    # data/test_block_iterator.cpp
    # data/test_serializer.cpp
    # net/test-net-group.cpp
    # engine/mock-network.cpp
    # engine/test_stage_builder.cpp
    # api/test_wordcount.cpp
    # data/test_dia_allocate
)

add_executable(c7a-tests ${SRCS})

target_link_libraries(c7a-tests
  c7a gtest gmock pthread
)

add_test(c7a-tests c7a-tests ${CMAKE_CURRENT_SOURCE_DIR})

################################################################################
