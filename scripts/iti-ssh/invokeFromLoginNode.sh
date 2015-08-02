#! /bin/bash

# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

function getDefaultHostlist {
  hosts=$(css list unused)
  echo $hosts | awk 'BEGIN { RS=" " } { printf "%s:%i ", $1, 59999}'
}

# Initialize default vals
hostlist=""
cmd=""
verbose=0
user=$(whoami)

while getopts "u:h:c:v" opt; do
    case "$opt" in
    h)
        hostlist=$OPTARG
        ;;
    \?)  echo "help"
        ;;
    c)  cmd=$OPTARG
        ;;
    v)  verbose=1
        ;;
    u)  user=$OPTARG
        ;;
    :)
        echo "Option -$OPTARG requires an argument." >&2
        exit 1
        ;;
    esac
done

if [ -z $cmd ]; then 
  echo "Command option -c has to be specified" >&2
  exit 1
fi

if [ -z "$hostlist" ]; then
  
  if [ -z $(which css) ]; then 
    echo "Host list not specified. Unable to query default hosts using css." >&2
    exit 1 
  fi 

  hostlist=$(getDefaultHostlist)
fi 

dir=$(dirname $cmd)


if [ $verbose -ne 0 ]; then
    echo "Hosts: $hostlist"
    echo "Command: $cmd" 
fi

rank=0

for hostport in $hostlist 
do
  uuid=$(cat /proc/sys/kernel/random/uuid)
  host=$(echo $hostport | awk 'BEGIN { FS=":" } { printf "%s", $1 }')
  ex="$cmd -r $rank $hostlist"
  echo "Connecting to $host to invoke $ex"
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no $host "$ex"
  ((rank++))
done
