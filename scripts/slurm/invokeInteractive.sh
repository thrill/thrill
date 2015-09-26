#!/bin/bash
################################################################################
# scripts/slurm/invokeInteractive.sh
#
# Part of Project Thrill.
#
# Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

msub -I -V -N test -l nodes=1:ppn=1,walltime=0:01:00 ./invoke.sh

################################################################################
