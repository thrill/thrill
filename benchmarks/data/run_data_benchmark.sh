# Script for running data benchmarks
# first parameter is input file size, second parameter is #runs
# see 'man head' for the size parameter specs


if [ $# -eq 0 ]
  then
    echo "No arguments supplied - use <amout> <runs>"
fi
./bench_data_disk $2 out_file $2
rm out_file
