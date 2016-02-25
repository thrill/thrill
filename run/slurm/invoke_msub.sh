#!/bin/bash
################################################################################
# run/slurm/invoke_msub.sh
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################
#MSUB -q multinode
#MSUB -l nodes=2:ppn=4
#MSUB -l walltime=0:20:00
#MSUB -l pmem=4000mb
#MSUB -v

slurm=$(dirname "$0")
slurm=$(cd "$slurm"; pwd)

srun ${slurm}/invoke.sh \
     ~/thrill/build/examples/page_rank/page_rank_run -g 100000 output.txt 5

################################################################################
