#!/bin/bash
################################################################################
# run_scripts/slurm/slurmCheck.sh
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

set -e

function printVars {
  echo "SLURM_NNODES: $SLURM_NNODES"
  echo "SLURM_NTASKS: $SLURM_NTASKS"
  echo "SLURM_JOB_HOSTLIST: $SLURM_JOB_NODELIST"
  echo "HOSTNAME: $(hostname)"
}

echo "Checking Environment:"
printVars

if [ -z $SLURM_NNODES ]; then
  echo "Error: SLURM environment not found."
  exit -1
fi

#if [ $SLURM_NNODES -ne $SLURM_NTASKS ]; then 
#  echo "Error: Multiple Thrill instances running on a single node."
#  exit -1
#fi

echo "More SLURM Environment:"
set | grep ^SLURM
echo "END SLURM Environment:"

################################################################################
