#!/usr/bin/env bash

#do not Load current dir
#cluster="`dirname "$0"`"
#cluster="`cd "$cluster"; pwd`"

# Cluster Options
export CLUSTER_NODES="2"
export CLUSTER_PPN="8"
export CLUSTER_WALLTIME="1:00:00"
export WORKER_TIMER="1h"
export THRILL_TASK="worker.sh"

module load compiler/gnu/5.2