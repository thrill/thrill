################################################################################
# benchmarks/data/CMakeLists.txt
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

thrill_build_prog(data_benchmark)

thrill_test_single(data_benchmark_file_consome ""
  data_benchmark file -b 64mi size_t consume)
thrill_test_single(data_benchmark_file_keep ""
  data_benchmark file -b 64mi size_t keep)

thrill_test_single(data_benchmark_blockqueue_consome ""
  data_benchmark blockqueue -b 64mi size_t consume)
thrill_test_single(data_benchmark_blockqueue_keep ""
  data_benchmark blockqueue -b 64mi size_t keep)

thrill_test_single(data_benchmark_cat_stream_1factor_consome "THRILL_LOCAL=3"
  data_benchmark cat_stream_1factor -b 16mib size_t consume)
thrill_test_single(data_benchmark_cat_stream_1factor_keep "THRILL_LOCAL=4"
  data_benchmark cat_stream_1factor -b 16mib size_t keep)

thrill_test_single(data_benchmark_mix_stream_1factor_consome "THRILL_LOCAL=3"
  data_benchmark mix_stream_1factor -b 16mib size_t consume)
thrill_test_single(data_benchmark_mix_stream_1factor_keep "THRILL_LOCAL=4"
  data_benchmark mix_stream_1factor -b 16mib size_t keep)

thrill_test_single(data_benchmark_cat_stream_all2all_consome "THRILL_LOCAL=3"
  data_benchmark cat_stream_all2all -b 32mib size_t consume)
thrill_test_single(data_benchmark_cat_stream_all2all_keep "THRILL_LOCAL=4"
  data_benchmark cat_stream_all2all -b 32mib size_t keep)

thrill_test_single(data_benchmark_mix_stream_all2all_consome "THRILL_LOCAL=3"
  data_benchmark mix_stream_all2all -b 16mib size_t consume)
thrill_test_single(data_benchmark_mix_stream_all2all_keep "THRILL_LOCAL=4"
  data_benchmark mix_stream_all2all -b 16mib size_t keep)

thrill_test_single(data_benchmark_scatter_consome ""
  data_benchmark scatter -b 64mi size_t consume)

################################################################################
