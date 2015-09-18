#!/usr/bin/env bash

# Cluster Options
export CLUSTER_NODES="1"
export CLUSTER_PPN="1"
export CLUSTER_WALLTIME="2:00:00"
export WORKER_TIMER="2h"
export THRILL_WORKERS_PER_HOST="8"
export THRILL_TASK="worker.sh"
