#! /bin/bash

function printVars {
  echo "SLURM_NNODES: $SLURM_NNODES"
  echo "SLURM_NTASKS: $SLURM_NTASKS"
  echo "SLURM_JOB_HOSTLIST: $SLURM_JOB_NODELIST"
}

echo "Checking Environment:"
printVars


if [ -z $SLURM_NNODES ]; then
  echo "Error: SLURM environment not found."
  exit -1
fi

if [ $SLURM_NNODES -ne $SLURM_NTASKS ]; then 
  echo "Error: Multiple c7a instances running on a single node."
  exit -1
fi

