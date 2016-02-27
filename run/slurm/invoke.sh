#!/bin/bash -x
################################################################################
# run/slurm/invoke.sh
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
# Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

set -e

slurm=$(dirname "$0")
slurm=$(cd "$slurm"; pwd)

echo "Here follows lots of info about the host $(hostname)"
set -x
date
pwd
${slurm}/slurm_check.sh
ip addr list
df
set +x

# set and export environment for Thrill
THRILL_HOSTLIST=$(${slurm}/slurm_hostlist.sh | awk -f ${slurm}/map_ib0.awk)
THRILL_NUM_HOSTS=$(echo $THRILL_HOSTLIST | wc -w)

# fixup SLURM_NNODES on uc1 cluster
[[ $(hostname) == uc1* ]] && export SLURM_NNODES=$THRILL_NUM_HOSTS

if [ -n "$THRILL_WORKERS_PER_HOST" ]; then
    : # let user override via the environment
elif [ -n "$SLURM_NTASKS_PER_NODE" ]; then
    THRILL_WORKERS_PER_HOST=$SLURM_NTASKS_PER_NODE
elif [[ $SLURM_TASKS_PER_NODE =~ ^([0-9]+)\(x([0-9]+)\)$ ]]; then
    THRILL_WORKERS_PER_HOST=${BASH_REMATCH[1]}
else
    THRILL_WORKERS_PER_HOST=$SLURM_TASKS_PER_NODE
fi

export THRILL_HOSTLIST THRILL_WORKERS_PER_HOST

# use the following settings for ssh invocation
echo "THRILL_NUM_HOSTS:        $THRILL_NUM_HOSTS"
echo "THRILL_HOSTLIST:         $THRILL_HOSTLIST"
echo "THRILL_WORKERS_PER_HOST: $THRILL_WORKERS_PER_HOST"

srun -v \
     --exclusive \
     --ntasks="$THRILL_NUM_HOSTS" \
     --ntasks-per-node="1" \
     --kill-on-bad-exit \
     "$@"

exit 0

################################################################################
