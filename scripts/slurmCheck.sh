#! /bin/bash

function printVars {
  echo "SLURM_STEP_NUM_TASKS: $SLURM_STEP_NUM_TASKS"
  echo "SLURM_NUM_NODES: $SLURM_NUM_NODES"
  echo "SLURM_JOB_HOSTLIST: $SLURM_JOB_NODELIST"
  echo "SLURM_PROCID: $SLURM_PROCID"
}


if [ -z $SLURM_STEP_NUM_TASKS ]; then
  echo "Error: SLURM environment not found."
  printVars
  exit -1
fi

if [ $SLURM_STEP_NUM_TASKS -gt $SLURM_NUM_NODES ]; then 
  echo "Error: Multiple c7a instances running on a single node."
  printVars
  exit -1
fi

