#!/bin/bash -x

set -e

./slurmCheck.sh

ip addr list

# set and export environment for c7a
C7A_HOSTLIST=$(./getSlurmHostlist.sh | awk -f map_ib0.awk)
C7A_RANK=$(./getSlurmRank.sh)

export C7A_HOSTLIST C7A_RANK

date
echo "C7A_HOSTLIST: $C7A_HOSTLIST"
echo "C7A_RANK:     $C7A_RANK"

# note: for running a command with gdb:
# gdb -q -x ./backtrace --args $COMMAND

# this enables continuation if one of the commands fails
set +e

# --- Commands to run ----------------------------------------------------------

../../build/benchmarks/data_channel -b 1000mi AllPairs size_t
../../build/benchmarks/data_channel -b 1000mi Full size_t
