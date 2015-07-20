#! /bin/bash
PORT=${1-64999}
awk 'BEGIN { FS=" "} { for(i = 1; i < NF; i++) { printf "%s:%s ", $i, "'$PORT'"  } }'
