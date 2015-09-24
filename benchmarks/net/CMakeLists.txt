################################################################################
# benchmarks/net/CMakeLists.txt
#
# Part of Project Thrill.
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

thrill_build_prog(net_benchmark)

thrill_test_single(net_benchmark_ping_pong_local3 "THRILL_LOCAL=3"
  net_benchmark ping_pong 10)

thrill_test_single(net_benchmark_ping_pong_local4 "THRILL_LOCAL=4"
  net_benchmark ping_pong 10)

thrill_test_single(net_benchmark_bandwidth_local3 "THRILL_LOCAL=3"
  net_benchmark bandwidth 10)

thrill_test_single(net_benchmark_bandwidth_local4 "THRILL_LOCAL=4"
  net_benchmark bandwidth 10)

################################################################################
