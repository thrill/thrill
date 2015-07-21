#! /bin/bash
JOBS=$SLURM_JOB_NODELIST
BASE=$(echo $JOBS | awk 'BEGIN { FS="[" } { print $1 }')
REST=$(echo $JOBS | awk 'BEGIN { FS="(\\[|\\])" } { print $2 }')

if [ -z "$REST" ]; then
  #1 item in list
  echo $JOBS
else
  #n items in list
  echo $REST | awk 'BEGIN { FS=","} { for(i = 1; i <= NF; i++) { printf "%s%s ", "'$BASE'", $i } }'
fi
