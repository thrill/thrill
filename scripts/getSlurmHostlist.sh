#! /bin/bash

#example hostlist:  uc1n[068,074,080-082,134,139,141,152,156]
JOBS=$SLURM_JOB_NODELIST

#gets the first part of a hostlist, for instance uc1n
BASE=$(echo $JOBS | awk 'BEGIN { FS="[" } { print $1 }')

#gets the enumeration of a hostlist, e.g. 068, 071, 080-082, ...
REST=$(echo $JOBS | awk 'BEGIN { FS="(\\[|\\])" } { print $2 }')

if [ -z "$REST" ]; then
  #1 item in list
  echo $JOBS
else
  #n items in list

  #converts the parts of the hostlist to an enumeration
  #of all hostnames, seperated by blanks. Also takes care 
  # of concatenating the pre- and posfixes of hostnames and also 
  #handles and expands ranges. 
  echo $REST | awk '
BEGIN { FS=","} 
{ 
  for(i = 1; i <= NF; i++) {
    if($i ~ /-/) {
      n = split($i, borders, "-")
      if(n == 2) {
        for(j = borders[1]; j <= borders[2]; j++) {
          printf "%s%s ", "'$BASE'", j 
        } 
      } else {
        exit -1
      }
    } else { 
      printf "%s%s ", "'$BASE'", $i 
    }
  } 
}'
fi
