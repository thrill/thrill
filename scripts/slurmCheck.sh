#! /bin/bash

if [ -z $SLURM_STEP_NUM_TASKS ]; then
  echo "Error: SLURM environment not found."
  exit -1
fi

if [ $SLURM_STEP_NUM_TASKS -gt $SLURM_NUM_NODES ]; then 
  echo "Error: Multiple c7a instances running on a single node."
  exit -1
fi

