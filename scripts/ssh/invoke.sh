#!/bin/bash

ssh_dir="`dirname "$0"`"
ssh_dir="`cd "$ssh_dir"; pwd`"
cluster=${ssh_dir}/../cluster

set -e

# Reset in case getopts has been used previously in the shell.
OPTIND=1

# Initialize default vals
copy=0
verbose=1
dir=
user=$(whoami)

. ${cluster}/thrill-env.sh

while getopts "u:h:H:cvC" opt; do
    case "$opt" in
    h)
        # this overrides the user environment variable
        THRILL_SSHLIST=$OPTARG
        ;;
    H)
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
        dir=/tmp/
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
    echo "  -C <path>  remote directory to change into (else: exe's dir)"
    echo "  -h <list>  list of nodes with port numbers"
    echo "  -H <list>  list of internal IPs passed to thrill exe (else: -h list)"
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
    if [ -z "$THRILL_SSHLIST" ]; then
        echo "No host list specified and THRILL_SSHLIST/HOSTLIST variable is empty." >&2
        exit 1
    fi
    THRILL_HOSTLIST="$THRILL_SSHLIST"
fi

if [ -z "$dir" ]; then
    dir=`dirname "$cmd"`
fi

if [ $verbose -ne 0 ]; then
    echo "Hosts: $THRILL_HOSTLIST"
    if [ "$THRILL_HOSTLIST" != "$THRILL_SSHLIST" ]; then
        echo "ssh Hosts: $THRILL_SSHLIST"
    fi
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

cmdbase=`basename "$cmd"`
rank=0
THRILL_HOSTLIST="${hostlist[@]}"

for hostport in $THRILL_SSHLIST; do
  host=$(echo $hostport | awk 'BEGIN { FS=":" } { printf "%s", $1 }')
  if [ $verbose -ne 0 ]; then
    echo "Connecting to $host to invoke $cmd"
  fi
  REMOTEPID="/tmp/$cmdbase.$hostport.$$.pid"
  if [ "$copy" == "1" ]; then
      REMOTENAME="/tmp/$cmdbase.$hostport.$$"
      # copy the program to the remote, and execute it at the remote end.
      ( scp -o BatchMode=yes -o StrictHostKeyChecking=no \
            "$cmd" "$host:$REMOTENAME" &&
        ssh -o BatchMode=yes -o StrictHostKeyChecking=no \
            $host \
            "export THRILL_WORKERS_PER_HOST=\"THRILL_WORKERS_PER_HOST\" THRILL_HOSTLIST=\"$THRILL_HOSTLIST\" THRILL_RANK=\"$rank\" && chmod +x \"$REMOTENAME\" && cd $dir && \"$REMOTENAME\" $* && rm \"$REMOTENAME\""
      ) &
  else
      ssh \
          -o BatchMode=yes -o StrictHostKeyChecking=no \
          $host \
          "export THRILL_WORKERS_PER_HOST=\"THRILL_WORKERS_PER_HOST\" THRILL_HOSTLIST=\"$THRILL_HOSTLIST\" THRILL_RANK=\"$rank\" && cd $dir && $cmd $*" &
  fi
  rank=$((rank+1))
done

# on Ctrl+C kill remote programs
function killcommand() {
    echo "Killing remote programs, please wait."
    for hostport in $THRILL_SSHLIST; do
        host=$(echo $hostport | awk 'BEGIN { FS=":" } { printf "%s", $1 }')
        REMOTENAME="$cmdbase.$hostport.$$"

        ssh -o BatchMode=yes -o StrictHostKeyChecking=no \
            $host "killall \"$REMOTENAME\"" || true
    done
}

trap "killcommand" SIGINT

echo "Waiting for execution to finish."
for hostport in $THRILL_HOSTLIST; do
    wait
done
echo "Done."
