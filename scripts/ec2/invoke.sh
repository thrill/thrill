#!/bin/bash

set -e

ENV=$(./make_env.py)

eval $ENV

../ssh/invoke.sh "$@"
