#!/bin/bash
################################################################################
# run/ssh/invoke.sh
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
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

while getopts "u:h:H:cvCw:" opt; do
    case "$opt" in
    h)
        # this overrides the user environment variable
        THRILL_SSHLIST=$OPTARG
        ;;
    H)
        # this overrides the user environment variable
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
    w)
        # this overrides the user environment variable
        THRILL_WORKERS_PER_HOST=$OPTARG
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
    echo "  -w <num>   set thrill workers per host variable"
    echo "  -v         verbose output"
    exit 1
fi

if [ ! -e "$cmd" ]; then
  echo "Thrill executable \"$cmd\" does not exist?" >&2
  exit 1
fi

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
export THRILL_HOSTLIST="${hostlist[@]}"
SSH_PIDS=()

for hostport in $THRILL_SSHLIST; do
  host=$(echo $hostport | awk 'BEGIN { FS=":" } { printf "%s", $1 }')
  if [ $verbose -ne 0 ]; then
    echo "Connecting to $user@$host to invoke $cmd"
  fi
  THRILL_EXPORTS=$(env | awk -F= '/^THRILL_/ { printf("%s", $1 "=\"" $2 "\" ") }')
  THRILL_EXPORTS="${THRILL_EXPORTS}THRILL_RANK=\"$rank\" THRILL_DIE_WITH_PARENT=1"
  REMOTEPID="/tmp/$cmdbase.$hostport.$$.pid"
  echo $*
  if [ "$copy" == "1" ]; then
      REMOTENAME="/tmp/$cmdbase.$hostport.$$"
      THRILL_EXPORTS="$THRILL_EXPORTS THRILL_UNLINK_BINARY=\"$REMOTENAME\""
      # copy the program to the remote, and execute it at the remote end.
      ( scp -o BatchMode=yes -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o Compression=yes \
            "$cmd" "$host:$REMOTENAME" &&
        ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o TCPKeepAlive=yes \
            $host \
            "export $THRILL_EXPORTS && chmod +x \"$REMOTENAME\" && cd $dir && exec \"$REMOTENAME\" $*" &&
        if [ -n "$THRILL_LOG" ]; then
            scp -o BatchMode=yes -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o Compression=yes \
                "$host:/tmp/$THRILL_LOG-*" "."
        fi
      ) &
  else
      ssh \
          -o BatchMode=yes -o StrictHostKeyChecking=no \
          $host \
          "export $THRILL_EXPORTS && cd $dir && exec $cmd $*" &
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
