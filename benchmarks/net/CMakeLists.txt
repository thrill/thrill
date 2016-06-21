################################################################################
# benchmarks/net/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

thrill_build_prog(net_benchmark)

thrill_test_multiple(net_benchmark_ping_pong_local
  net_benchmark ping_pong 10)

thrill_test_multiple(net_benchmark_bandwidth_local
  net_benchmark bandwidth 10)

thrill_test_multiple(net_benchmark_prefixsum_local
  net_benchmark prefixsum -r 10)

################################################################################
