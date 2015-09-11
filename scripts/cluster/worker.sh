#!/bin/bash

cluster="`dirname "$0"`"
cluster="`cd "$cluster"; pwd`"
build=${cluster}/../../build

${build}/benchmarks/data_channel -b 1000mi AllPairs size_t
${build}/benchmarks/data_channel -b 1000mi Full size_t
