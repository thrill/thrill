#! /bin/bash
HOSTS=$(./getSlurmHostlist.sh)
HOST=$(hostname)
echo $HOSTS | awk 'BEGIN { FS=" " } { for(i = 1; i < NF; i++) { if($i == "'$HOST'") { printf "%s", (i - 1) } } }'

