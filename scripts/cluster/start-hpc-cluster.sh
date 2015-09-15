#!/usr/bin/env bash

cluster="`dirname "$0"`"
cluster="`cd "$cluster"; pwd`"
slurm=${cluster}/../slurm

. ${cluster}/thrill-env.sh

msub -v slurm=${slurm},cluster=${cluster} -N thrill -l nodes=${CLUSTER_NODES}:ppn=${CLUSTER_PPN},walltime=${CLUSTER_WALLTIME},naccesspolicy=singlejob ${slurm}/invokeWrapper.sh

