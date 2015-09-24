#!/bin/bash

set -e

scriptdir="`dirname "$0"`"

ENV=$($scriptdir/make_env.py)
eval $ENV

$scriptdir/../ssh/invoke.sh "$@"
