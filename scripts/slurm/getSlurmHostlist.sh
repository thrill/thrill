#!/bin/bash
################################################################################
# scripts/slurm/getSlurmHostlist.sh
#
# Part of Project Thrill.
#
# Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

# Simply use (undocumented) expandnodes tool to expand the hostlist. 
expandnodes "$SLURM_JOB_NODELIST"

################################################################################
