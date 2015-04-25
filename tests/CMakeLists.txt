################################################################################
# tests/CMakeLists.txt
#
# CMake file for c7a testsuite
################################################################################

set(SRCS
    main.cpp
    api/test_simple_api.cpp
    data/test_data_manager.cpp
    data/test_block_iterator.cpp
    data/test_serializer.cpp
    communication/test_net_dispatcher.cpp
    engine/mock-network.cpp
    engine/test_stage_builder.cpp

)

add_executable(c7atest ${SRCS})

target_link_libraries(c7atest
  c7a
  libgtest
  libgmock
  pthread
)

install(TARGETS c7atest DESTINATION build)

################################################################################
