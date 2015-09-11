#!/usr/bin/env bash

cluster="`dirname "$0"`"
cluster="`cd "$cluster"; pwd`"
slurm=${cluster}/../slurm

msub -v slurm=${slurm} cluster=${cluster} -N thrill -l nodes=${CLUSTER_NODES}:ppn=${CLUSTER_PPN},walltime=${CLUSTER_WALLTIME},acesspolicy=singlejob ${slurm}/invokeWrapper.sh
