#!/bin/sh

this="${BASH_SOURCE-$0}"
build=$(cd -P -- "$(dirname -- "$this")" && pwd -P)/../../build/benchmarks/chaining
bench=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
spark=$(cd -P -- "$(dirname -- "$this")" && pwd -P)/../../../../../Projects/Spark
flink=$(cd -P -- "$(dirname -- "$this")" && pwd -P)/../../../../../Projects/Flink

set -e

if [ -f ${build}/bench.log ]; then
    > ${build}/bench.log
fi

export THRILL_LOCAL="1"

setup="1in10"

for i in `seq 1 10`;
    do
        echo "CACHE" >> ${build}/bench.log
        ${build}/cache_count_${setup} 10000 >> ${build}/bench.log
        echo "COLLAPSE" >> ${build}/bench.log
        ${build}/collapse_count_${setup} 10000 >> ${build}/bench.log
        echo "CHAIN" >> ${build}/bench.log
        ${build}/chain_count_${setup} 10000 >> ${build}/bench.log
        echo "SPARK" >> ${build}/bench.log
        ${spark}/bin/spark-submit --class org.apache.spark.examples.LocalCount --master local[1] ${spark}/apps/LocalCounter/local-count-${setup}.jar 10000 >> ${build}/bench.log
        echo "FLINK" >> ${build}/bench.log
        ${flink}/bin/start-local.sh
        ${flink}/bin/flink run ${flink}/apps/LocalCounter/local-count-${setup}.jar 10000 >> ${build}/bench.log
        ${flink}/bin/stop-local.sh
    done

python ${bench}/evaluate.py ${build}/bench.log ${build}/${setup}.out

setup="10in1"

for i in `seq 1 10`;
    do
        echo "CACHE" >> ${build}/bench.log
        ${build}/cache_count_${setup} 10000 >> ${build}/bench.log
        echo "COLLAPSE" >> ${build}/bench.log
        ${build}/collapse_count_${setup} 10000 >> ${build}/bench.log
        echo "CHAIN" >> ${build}/bench.log
        ${build}/chain_count_${setup} 10000 >> ${build}/bench.log
        echo "SPARK" >> ${build}/bench.log
        ${spark}/bin/spark-submit --class org.apache.spark.examples.LocalCount --master local[1] ${spark}/apps/LocalCounter/local-count-${setup}.jar 10000 >> ${build}/bench.log
        echo "FLINK" >> ${build}/bench.log
        ${flink}/bin/start-local.sh
        ${flink}/bin/flink run ${flink}/apps/LocalCounter/local-count-${setup}.jar 10000 >> ${build}/bench.log
        ${flink}/bin/stop-local.sh
    done

python ${bench}/evaluate.py ${build}/bench.log ${build}/${setup}.out
