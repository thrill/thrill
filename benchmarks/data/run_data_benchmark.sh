################################################################################
# benchmarks/data/run_data_benchmark.sh
#
# runs the bench_data_file_read_write benchmark with different types and
# different data types
#
# Part of Project Thrill.
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

githash=$(eval "git rev-parse --short HEAD")
timestamp=$(date)
echo ${timestamp}
for benchmark in block_queue
do
for type in size_t string pair triple
do
  for reader in consume keep
  do
    for size in 100K 1M 100M 2G
    do
      for block_size in 1K 1024K 2048K 4096K
      do
        for threads in 1 2 4
        do
          eval ./data_io_benches -t ${threads} -l 1 -u 100 -s ${block_size} -b ${size} ${benchmark} ${type} ${reader} | grep "RESULT" |  sed -e "s/$/ version=${githash} timestamp=${timestamp} /"
          eval ./data_io_benches -t ${threads} -l 1K -u 10K -s ${block_size} -b ${size} ${benchmark} ${type} ${reader} | grep "RESULT" |  sed -e "s/$/ version=${githash} timestamp=${timestamp} /"
        done
      done
    done
  done
done
done

for benchmark in file
for type in size_t string pair triple
do
  for reader in consume keep
  do
    for size in 100K 1M 100M 2G
    do
      for block_size in 1K 1024K 2048K 4096K
      do
        eval ./data_io_benches -l 1 -u 100 -s ${block_size} -b ${size} ${benchmark} ${type} ${reader} | grep "RESULT" |  sed -e "s/$/ version=${githash} timestamp=${timestamp} /"
        eval ./data_io_benches -l 1K -u 10K -s ${block_size} -b ${size} ${benchmark} ${type} ${reader} | grep "RESULT" |  sed -e "s/$/ version=${githash} timestamp=${timestamp} /"
      done
    done
  done
done

################################################################################
