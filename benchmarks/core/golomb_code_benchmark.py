#!/usr/bin/env python2
##########################################################################
# benchmarks/core/golomb_code_benchmark.py
#
# Part of Project Thrill - http://project-thrill.org
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import os
import sys
import subprocess
import random

num_runs = 5

##### bench golomb coder

param = [5,6,7,8,9,10]
distance = [10,20,40,80]
amounts= [14,16,18,20,22]

for g in param:
    for d in distance:
        for amount in amounts:
            times = []
            for _ in range(num_runs):
                a = ['../../build/benchmarks/core_golomb_code_benchmark',
                         '-n', str(pow(2,amount)), '-g', str(g), '-d', str(d)]
                print(a)
                process = subprocess.call(a)

##########################################################################
