#!/bin/sh

this="${BASH_SOURCE-$0}"
build=$(cd -P -- "$(dirname -- "$this")" && pwd -P)/../../build/benchmarks/chaining

set -e

time ${build}/cache_count ${build}/headwords ${build}/output
time ${build}/collapse_count ${build}/headwords ${build}/output
time ${build}/chain_count ${build}/headwords ${build}/output
