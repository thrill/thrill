#!/bin/bash
################################################################################
# run/ssh/invoke.sh
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
# Copyright (C) 2017 Tim Zeitz <dev.tim.zeitz@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

set -e

# Reset in case getopts has been used previously in the shell.
OPTIND=1

# Initialize default vals
copy=0
verbose=1
dir=
user=$(whoami)
with_perf=0
with_perf_graph=0
timeout=
more_env=

while getopts "u:h:H:cvC:w:pPx:T:" opt; do
    case "$opt" in
    h)  # this overrides the user environment variable
        THRILL_SSHLIST=$OPTARG
        ;;
    H)  # this overrides the user environment variable
        THRILL_HOSTLIST=$OPTARG
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
    p)  with_perf=1
        ;;
    P)  with_perf_graph=1
        ;;
    T)  timeout=$OPTARG
        ;;
    w)  # this overrides the user environment variable
        export THRILL_WORKERS_PER_HOST=$OPTARG
        ;;
    x)  # more environment variables
        more_env="$more_env \"$OPTARG\""
        ;;
    :)  echo "Option -$OPTARG requires an argument." >&2
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
    echo "  -p         run with perf"
    echo "  -P         run with perf -g (profile callgraph)"
    echo "  -h <list>  space-delimited list of nodes"
    echo "  -H <list>  list of internal IPs passed to Thrill exe (else: -h list)"
    echo "  -u <name>  ssh user name"
    echo "  -T <num>   kill Thrill job after <num> seconds"
    echo "  -w <num>   set thrill workers per host variable"
    echo "  -v         verbose output"
    echo "  -x v=arg   set environment variable, all THRILL_* are automatically set."
    exit 1
fi

#if [ ! -e "$cmd" ]; then
 # echo "Thrill executable \"$cmd\" does not exist?" >&2
  #exit 1
#fi

# get absolute path
if [[ "$(uname)" == "Darwin" ]]; then
  # note for OSX users: readlink will fail on mac.
  # install coreutils (brew install coreutils) and use greadlink instead
  cmd=`greadlink -f "$cmd"` # requires package coreutils
else
  cmd=`readlink -f "$cmd"`
fi

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

portbase=$(( RANDOM % 20000 + 30000 ))

# check THRILL_HOSTLIST for hosts without port numbers: add 30000+random+rank
hostlist=()
for hostport in $THRILL_HOSTLIST; do
  port=$(echo $hostport | awk 'BEGIN { FS=":" } { printf "%s", $2 }')
  if [ -z "$port" ]; then
      hostport="$hostport:$((portbase+rank))"
  fi
  hostlist+=($hostport)
  rank=$((rank+1))
done

cmdbase=`basename "$cmd"`
rank=0
export THRILL_HOSTLIST="${hostlist[@]}"
SSH_PIDS=()

trap '[ $(jobs -p | wc -l) != 0 ] && kill $(jobs -p)' SIGINT SIGTERM EXIT

for hostport in $THRILL_SSHLIST; do
  host=$(echo $hostport | awk 'BEGIN { FS=":" } { printf "%s", $1 }')
  if [ $verbose -ne 0 ]; then
    echo "Connecting to $user@$host to invoke $dir/$cmdbase"
  fi
  THRILL_EXPORTS=$(env | awk -F= '/^THRILL_/ { printf("%s", $1 "=\"" $2 "\" ") }')
  THRILL_EXPORTS="${THRILL_EXPORTS}THRILL_RANK=\"$rank\" THRILL_DIE_WITH_PARENT=1"
  RUN_PREFIX="exec"
  if [ "$with_perf" == "1" ]; then
      # run with perf
      RUN_PREFIX="exec perf record -o perf-$rank.data"
  fi
  if [ "$with_perf_graph" == "1" ]; then
      # run with perf
      RUN_PREFIX="exec perf record -g -o perf-$rank.data"
  fi
  if [ "$timeout" != "" ]; then
      RUN_PREFIX="$RUN_PREFIX timeout ${timeout}"
  fi

  if [ "$copy" == "1" ]; then
      REMOTENAME="/tmp/$cmdbase.$hostport.$$"
      echo "HOSTPORT: $hostport"
      echo "REMOTENAME: $REMOTENAME"
      THRILL_EXPORTS="$THRILL_EXPORTS THRILL_UNLINK_BINARY=\"$REMOTENAME\""
      # copy the program to the remote, and execute it at the remote end.
      (
        scp -o BatchMode=yes -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o Compression=yes \
            "$cmd" "$user@$host:$REMOTENAME" &&
        ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o TCPKeepAlive=yes \
            $user@$host \
            "export $THRILL_EXPORTS $more_env && chmod +x \"$REMOTENAME\" && cd $dir && $RUN_PREFIX \"$REMOTENAME\" $*" &&
        if [ -n "$THRILL_LOG" ]; then
            scp -o BatchMode=yes -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o Compression=yes \
                "$user@$host:/tmp/$THRILL_LOG-*" "."
        fi
      ) &
  else
      command="$dir/$cmdbase"
      ssh \
          -o BatchMode=yes -o StrictHostKeyChecking=no \
          $user@$host \
          "export $THRILL_EXPORTS $more_env && cd $dir && $RUN_PREFIX $command $*" &
  fi
  # save PID of ssh child for later
  SSHPIDS[$rank]=$!
  rank=$((rank+1))
done

echo "Waiting for execution to finish."
result=0
rank=0
for hostport in $THRILL_HOSTLIST; do
    set +e
    wait ${SSHPIDS[$rank]}
    retcode=$?
    set -e
    if [ $retcode != 0 ]; then
        echo "Thrill program on host $hostport returned code $retcode"
        result=$retcode
    fi
    rank=$((rank+1))
done

echo "Done. Result $result"

exit $result

################################################################################
