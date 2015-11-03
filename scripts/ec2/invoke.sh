#!/bin/bash
################################################################################
# scripts/ec2/invoke.sh
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

set -e

scriptdir="`dirname "$0"`"

ENV=$($scriptdir/make_env.py)
eval $ENV

$scriptdir/../ssh/invoke.sh "$@"

################################################################################
