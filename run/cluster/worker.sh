#!/bin/bash
################################################################################
# run/cluster/worker.sh
#
# Part of Project Thrill - http://project-thrill.org
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

#cluster="`dirname "$0"`"
#cluster="`cd "$cluster"; pwd`"
build=${cluster}/../../build

time ${build}/benchmarks/word_count/word_count '/home/kit/stud/uagtc/common/inputs/RC_2015-01.body' '/home/kit/stud/uagtc/outputs/reddit-$-##'

################################################################################
