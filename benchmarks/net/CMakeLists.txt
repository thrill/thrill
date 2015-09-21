################################################################################
# benchmarks/net/CMakeLists.txt
#
# CMake file for Thrill benchmark programs.
################################################################################

thrill_build_prog(broadcast)
thrill_build_prog(prefixsum)
thrill_build_prog(net_benchmark)

add_test(net_benchmark_ping_pong_local3 net_benchmark ping_pong 10)

set_tests_properties(net_benchmark_ping_pong_local3
  PROPERTIES
  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=3;THRILL_WORKERS_PER_HOST=1")

add_test(net_benchmark_ping_pong_local4 net_benchmark ping_pong 10)

set_tests_properties(net_benchmark_ping_pong_local4
  PROPERTIES
  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1")

add_test(net_benchmark_bandwidth_local3 net_benchmark bandwidth 10)

set_tests_properties(net_benchmark_bandwidth_local3
  PROPERTIES
  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=3;THRILL_WORKERS_PER_HOST=1")

add_test(net_benchmark_bandwidth_local4 net_benchmark bandwidth 10)

set_tests_properties(net_benchmark_bandwidth_local4
  PROPERTIES
  ENVIRONMENT "THRILL_NET=mock;THRILL_LOCAL=4;THRILL_WORKERS_PER_HOST=1")

################################################################################
