#!/bin/bash -x

set -e

slurm="`dirname "$0"`"
slurm="`cd "$slurm"; pwd`"
cluster=${slurm}/../cluster

${slurm}/slurmCheck.sh

ip addr list

# set and export environment for Thrill
THRILL_HOSTLIST=$(${slurm}/getSlurmHostlist.sh | awk -v workers_per_node="$SLURM_JOB_CPUS_PER_NODE" -f map_ib0.awk)
THRILL_RANK=$(${slurm}/getSlurmRank.sh)

export THRILL_HOSTLIST THRILL_RANK

date
echo "THRILL_HOSTLIST: $THRILL_HOSTLIST"
echo "THRILL_RANK:     $THRILL_RANK"

# note: for running a command with gdb:
# gdb -q -x ./backtrace --args $COMMAND

# this enables continuation if one of the commands fails
set +e


. ${cluster}/thrill-env.sh

#sleep $timer
. ${cluster}/${THRILL_TASK}
