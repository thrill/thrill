#!/bin/sh

this="${BASH_SOURCE-$0}"
build=$(cd -P -- "$(dirname -- "$this")" && pwd -P)/../../build/benchmarks/chaining
bench=$(cd -P -- "$(dirname -- "$this")" && pwd -P)

set -e

if [ -f ${build}/bench.log ]; then
    > ${build}/bench.log
fi

setup="1in10.out"
#setup="10in1.out"

for i in `seq 1 100`;
    do
        echo "CHAIN" >> ${build}/bench.log
        ${build}/chain_count 10000 >> ${build}/bench.log
        echo "COLLAPSE" >> ${build}/bench.log
        ${build}/collapse_count 10000 >> ${build}/bench.log
        echo "CACHE" >> ${build}/bench.log
        ${build}/cache_count 10000 >> ${build}/bench.log
    done

python ${bench}/evaluate.py ${build}/bench.log ${build}/${setup}
