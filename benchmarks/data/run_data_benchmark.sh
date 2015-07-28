# runs the bench_data_file_read_write benchmark with different types and
# different data types

githash=$(eval "git rev-parse --short HEAD")
for type in int string pair triple
do
  for size in 1K 100K 1M
  do
    ./bench_data_file_read_write -b ${size} $1 int | grep 'single run; ti' | sed -e "s/^/${githash}; ${size}; ${type}; /"
 done
done
