#! /bin/bash

./slurmCheck.sh

if [ $? -ne 0 ]; then
  #Check failed. 
  exit -1
fi 

PORT=64999
COMMAND="../build/examples/word_count"
HOSTLIST=$(./getSlurmHostlist.sh | ./formatPort.sh $PORT)
MY_RANK=$(./getSlurmRank.sh)
EX="$COMMAND -r $MY_RANK $HOSTLIST"
echo "Invoking: $COMMAND"
eval $EX
