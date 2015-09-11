#!/usr/bin/env bash

# Load current dir
cluster="`dirname "$0"`"
cluster="`cd "$cluster"; pwd`"

# Cluster Options
export CLUSTER_NODES="1"
export CLUSTER_PPN="1"
export CLUSTER_WALLTIME="2:00:00"
export THRILL_TASK="${cluster}/worker.sh"
