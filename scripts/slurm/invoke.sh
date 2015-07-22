#! /bin/bash

./slurmCheck.sh

if [ $? -ne 0 ]; then
  #Check failed. 
  exit -1
fi 

PORT=10090
COMMAND="../build/examples/word_count"
HOSTLIST=$(./getSlurmHostlist.sh | ./formatPort.sh $PORT)
MY_RANK=$(./getSlurmRank.sh)
EX="$COMMAND -r $MY_RANK $HOSTLIST >> $MY_RANK.stdout 2>> $MY_RANK.stderr"
echo "Invoking: $EX"
eval $EX
