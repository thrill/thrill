# runs the bench_data_file_read_write benchmark with different types and
# different data types

githash=$(eval "git rev-parse --short HEAD")
timestamp=$(date)
echo ${timestamp}
for benchmark in file block_queue
do
  for type in size_t string pair triple
  do
    for reader in consume non-consume
    do
      for size in 100K 1M 10M 100M
      do
        for block_size in 64K 265K 512K 2048K 4096K
        do
          eval ./data_io_benches -l 1 -u 100 -s ${block_size} -b ${size} ${benchmark} ${type} ${reader} | grep "RESULT" |  sed -e "s/$/ version=${githash} timestamp=${timestamp} /"
          eval ./data_io_benches -l 1K -u 10K -s ${block_size} -b ${size} ${benchmark} ${type} ${reader} | grep "RESULT" |  sed -e "s/$/ version=${githash} timestamp=${timestamp} /"
        done
      done
    done
  done
done
