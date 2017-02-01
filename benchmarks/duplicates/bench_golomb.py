#!/usr/bin/env python2
##########################################################################
# benchmarks/duplicates/bench_golomb.py
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
#import numpy

num_runs = 5

##### bench golomb coder

result_dir = "./bench_golomb"

param = [4,8,16,32]
bs = [5,6,7,8,9,10]
distance = [10,20,40,80]
amounts= [14,16,18,20,22]

if not os.path.exists(result_dir): os.makedirs(result_dir)

# Insert pow(2,n) random integer elements into hashtable with specific number of workers and keyspace. Perform 5 times and print median to file.
for p in param:
    for b in bs:
        for d in distance:
            for amount in amounts:
                times = []
                for _ in range(num_runs):
                    a = ['../../build/benchmarks/duplicates_bench_golomb',
                             '-n', str(pow(2,amount)), '-b', str(b),
                             '-d', str(d), '-f', str(p)]
                    print(a)
                    process = subprocess.Popen(a, stdout=subprocess.PIPE)
                    process.wait()
                    result = process.communicate()[0].split()
                    print(result)
                    assert result[0] == "RESULT"
                    times.append(int(result[2].split("=")[1]))

                time_avg = "time_avg=" + str(sum(times) / num_runs)
                print(result[0] + " " + result[1] + " " +
                    time_avg + " " + result[3] + " " + result[4] + " " +
                    result[5] + " " + result[6] + " " + result[7])

##########################################################################
