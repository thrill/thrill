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

while getopts "u:h:e:d:v" opt; do
    case "$opt" in
    h)
        hostlist=$OPTARG
        ;;
    \?)  echo "TODO: Help"
        ;;
    e)  cmd=$OPTARG
        ;;
    d)  dir=$OPTARG
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
  echo "Executable option -e has to be specified" >&2
  exit 1
fi

if [ -z $dir ]; then 
  dir=$(pwd) 
fi

if [ -z "$hostlist" ]; then
  
  if [ -z $(which css) ]; then 
    echo "Host list not specified. Unable to query default hosts using css." >&2
    exit 1 
  fi 

  hostlist=$(getDefaultHostlist)
fi 


if [ $verbose -ne 0 ]; then
    echo "Hosts: $hostlist"
    echo "Command: $cmd" 
    echo "Directory: $dir" 
fi

rank=0

for hostport in $hostlist 
do
  uuid=$(cat /proc/sys/kernel/random/uuid)
  host=$(echo $hostport | awk 'BEGIN { FS=":" } { printf "%s", $1 }')
  ex="$cmd -r $rank $hostlist"
  if [ $verbose -ne 0 ]; then
    echo "Connecting to $host to invoke $ex"
  fi
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no $host "cd $dir; ./$ex" &
  ((rank++))
done

echo "Waiting for execution to finish."
wait
echo "Done."
