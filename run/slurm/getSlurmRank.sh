#! /bin/bash
################################################################################
# run_scripts/slurm/getSlurmRank.sh
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

HOSTS=$(${slurm}/getSlurmHostlist.sh)
HOST="$HOSTNAME"
ID=$(echo $HOSTS | awk -v workers_per_node="$SLURM_CPU_CORES_PER_NODE" -v procid="$SLURM_PROCID" 'BEGIN { FS=" " } { for(i = 1; i <= NF; i++) { if($i == "'$HOST'") { printf "%i", (((i - i) * workers_per_node) + procid) } } }')

if [ -z $ID ]; then
  echo "0"
else 
  echo "$ID"
fi


################################################################################
