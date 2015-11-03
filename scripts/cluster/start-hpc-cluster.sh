#!/usr/bin/env bash
################################################################################
# scripts/cluster/start-hpc-cluster.sh
#
# Part of Project Thrill - http://project-thrill.org
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

cluster="`dirname "$0"`"
cluster="`cd "$cluster"; pwd`"
slurm=${cluster}/../slurm

. ${cluster}/thrill-env.sh

msub -v slurm=${slurm},cluster=${cluster} -N thrill -l nodes=${CLUSTER_NODES}:ppn=${CLUSTER_PPN},walltime=${CLUSTER_WALLTIME},naccesspolicy=singlejob ${slurm}/invokeWrapper.sh


################################################################################
