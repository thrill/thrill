#! /bin/bash

DEBUG=1
PORT=10090
COMMAND="../../build/examples/word_count"

./slurmCheck.sh

if [ $? -ne 0 ]; then
  #Check failed. 
  exit -1
fi 

HOSTLIST=$(./getSlurmHostlist.sh | ./formatPort.sh $PORT)
MY_RANK=$(./getSlurmRank.sh)
EX="$COMMAND -r $MY_RANK $HOSTLIST >> $MY_RANK.stdout 2>> $MY_RANK.stderr"
if [ $DEBUG -eq 1 ]; then
  EX="gdb -q -x ./backtrace --args $EX"
fi
echo "Invoking: $EX"
eval $EX
