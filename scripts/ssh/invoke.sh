#!/bin/bash

set -e

ssh_dir="`dirname "$0"`"
ssh_dir="`cd "$ssh_dir"; pwd`"
cluster=${ssh_dir}/../cluster

. ${cluster}/thrill-env.sh

# Reset in case getopts has been used previously in the shell.
OPTIND=1

# Initialize default vals
copy=0
verbose=1
dir=
user=$(whoami)

while getopts "u:h:cvC:" opt; do
    case "$opt" in
    h)
        # this overrides the user environment variable
        THRILL_HOSTLIST=$OPTARG
        ;;
    \?)  echo "TODO: Help"
        ;;
    v)  verbose=1
        set -x
        ;;
    u)  user=$OPTARG
        ;;
    c)  copy=1
        ;;
    C)  dir=$OPTARG
        ;;
    :)
        echo "Option -$OPTARG requires an argument." >&2
        exit 1
        ;;
    esac
done

# remove those arguments that we were able to parse
shift $((OPTIND - 1))

# get executable
cmd=$1
shift || true

if [ -z "$cmd" ]; then
    echo "Usage: $0 [-h hostlist] thrill_executable [args...]"
    echo "More Options:"
    echo "  -c         copy program to hosts and execute"
    echo "  -C <path>  remove directory to change into"
    echo "  -h <list>  list of nodes with port numbers"
    echo "  -u <name>  ssh user name"
    echo "  -v         verbose output"
    exit 1
fi

if [ ! -e "$cmd" ]; then
  echo "Thrill executable \"$cmd\" does not exist?" >&2
  exit 1
fi

# get absolute path
cmd=`readlink -f "$cmd"`

if [ -z "$THRILL_HOSTLIST" ]; then
    echo "No host list specified and THRILL_HOSTLIST variable is empty." >&2
    exit 1
fi

if [ -z "$dir" ]; then
    dir=`dirname "$cmd"`
fi

if [ $verbose -ne 0 ]; then
    echo "Hosts: $THRILL_HOSTLIST"
    echo "Command: $cmd"
fi

rank=0
uuid=$(cat /proc/sys/kernel/random/uuid)

# check THRILL_HOSTLIST for hosts without port numbers: add 10000+rank
hostlist=()
for hostport in $THRILL_HOSTLIST; do
  port=$(echo $hostport | awk 'BEGIN { FS=":" } { printf "%s", $2 }')
  if [ -z "$port" ]; then
      hostport="$hostport:$((10000+rank))"
  fi
  hostlist+=($hostport)
  rank=$((rank+1))
done

rank=0
THRILL_HOSTLIST="${hostlist[@]}"

for hostport in $THRILL_HOSTLIST; do
  host=$(echo $hostport | awk 'BEGIN { FS=":" } { printf "%s", $1 }')
  if [ $verbose -ne 0 ]; then
    echo "Connecting to $host to invoke $cmd"
  fi
  if [ "$copy" == "1" ]; then
      cmdbase=`basename "$cmd"`
      REMOTENAME="/tmp/$cmdbase.$hostport"
      # pipe the program though the ssh pipe, save and execute it at the remote end.
      ( cat $cmd | \
              ssh -o BatchMode=yes -o StrictHostKeyChecking=no \
                  $host \
                  "module load compiler/gnu/5.2 && export THRILL_HOSTLIST=\"$THRILL_HOSTLIST\" THRILL_RANK=\"$rank\" && cat - > \"$REMOTENAME\" && chmod +x \"$REMOTENAME\" && cd $dir && \"$REMOTENAME\" $* && rm \"$REMOTENAME\""
      ) &
  else
      ssh \
          -o BatchMode=yes -o StrictHostKeyChecking=no \
          $host \
          "module load compiler/gnu/5.2 && export THRILL_HOSTLIST=\"$THRILL_HOSTLIST\" THRILL_RANK=\"$rank\" && cd $dir && $cmd $*" &
  fi
  rank=$((rank+1))
done

echo "Waiting for execution to finish."
for hostport in $THRILL_HOSTLIST; do
    wait
done
echo "Done."
