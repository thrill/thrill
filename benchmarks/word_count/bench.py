#!/usr/bin/env python
##########################################################################
# benchmarks/word_count/bench.py
#
# Test speedup for local parallelization with 1 to 4 workers on a single node.
#
# Part of Project Thrill.
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import os
import sys
import subprocess
import random
import re

amounts=[14,16,18,20,22,24,26,28]

workers = [4,3,2,1]

def median(x):
    if len(x)%2 != 0:
        return sorted(x)[len(x)/2]
    else:
        midavg = (sorted(x)[len(x)/2] + sorted(x)[len(x)/2-1])/2.0
        return midavg

# Generate pow(2,n) random integer elements and perform wordcount on them.

for amount in amounts:
    for worker in workers:
        print "Testing with " + str(worker) + " workers"
        with open(str(worker) + "_workers", "a+") as file1:
            results = []
            for _ in range(9):
                process = subprocess.Popen(["../../build/examples/local_word_count", "-n", str(worker), "-s", str(pow(2, amount))], stdout=subprocess.PIPE)
                process.wait()
                output = process.communicate()[0]
                #print output
                timers = [m.start() for m in re.finditer("job::overall", output)]
                times = []
                for timer in timers:
                    afterTimer = output[timer+24:]
                    spacePos = afterTimer.find('u')
                    times.append(int(afterTimer[:spacePos]))
                results.append(max(times))
                print max(times)
            med = median(results)
            print str(amount) + " " + str(med * 1000 / pow(2,amount))
            file1.write(str(amount) + " " + str(med * 1000 / pow(2,amount)) + "\n")
            #file1.flush()
            file1.close()



##########################################################################
