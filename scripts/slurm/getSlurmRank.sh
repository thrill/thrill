#! /bin/bash
HOSTS=$(./getSlurmHostlist.sh)
HOST=$(hostname)
ID=$(echo $HOSTS | awk 'BEGIN { FS=" " } { for(i = 1; i <= NF; i++) { if($i == "'$HOST'") { printf "%i", (i - 1) } } }')

if [ -z $ID ]; then
  echo "0"
else 
  echo "$ID"
fi

