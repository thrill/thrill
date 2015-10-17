################################################################################
# benchmarks/data/run_data_benchmark.sh
#
# runs the bench_data_file_read_write benchmark with different types and
# different data types
#
# Part of Project Thrill - http://project-thrill.org
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

githash=$(eval "git rev-parse --short HEAD")
timestamp=$(date)
echo ${timestamp}
for benchmark in block_queue file
do
for type in size_t string
do
  for reader in consume
  do
    for size in 1M 1G 10G
    do
      for block_size in 8K 16K 32K 64K 128K 256K 512K 1024K 2048K 4096K
      do
          eval ./data_io_benches -n 10 -l 1 -u 1K -s ${block_size} -b ${size} ${benchmark} ${type} ${reader} | grep "RESULT" |  sed -e "s/$/ version=${githash} timestamp=${timestamp} /"
      done
    done
  done
done
done
################################################################################
