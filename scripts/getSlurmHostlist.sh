#! /bin/bash
JOBS=$SLURM_JOB_NODELIST
BASE=$(echo $JOBS | awk 'BEGIN { FS="[" } { print $1 }')
REST=$(echo $JOBS | awk 'BEGIN { FS="(\[|\])" } { print $2 }')
echo $REST | awk 'BEGIN { FS=","} { for(i = 1; i <= NF; i++) { printf "%s%s ", "'$BASE'", $i } }'
