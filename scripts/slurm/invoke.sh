#!/bin/bash -x

set -e

./slurmCheck.sh

ip addr list

# set and export environment for Thrill
THRILL_HOSTLIST=$(./getSlurmHostlist.sh | awk -f map_ib0.awk)
THRILL_RANK=$(./getSlurmRank.sh)

export THRILL_HOSTLIST THRILL_RANK

date
echo "THRILL_HOSTLIST: $THRILL_HOSTLIST"
echo "THRILL_RANK:     $THRILL_RANK"

# note: for running a command with gdb:
# gdb -q -x ./backtrace --args $COMMAND

# this enables continuation if one of the commands fails
set +e

# --- Commands to run ----------------------------------------------------------

../../build/benchmarks/data_channel -b 1000mi AllPairs size_t
../../build/benchmarks/data_channel -b 1000mi Full size_t
