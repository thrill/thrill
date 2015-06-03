#!/usr/bin/env python

import os
import sys
import subprocess
import random
import numpy
import re

amounts=[10,12,14,16,18,20]

workers = [3,4]


# Generate pow(2,n) random integer elements and perform wordcount on them.

for worker in workers:
    print "Testing with " + str(worker) + " workers"
    with open(str(worker) + "_workers", "w+") as file1:
        for amount in amounts:
            results = []
            for _ in range(5):
                process = subprocess.Popen(["../../build/examples/wc", "-n", str(worker), "-s", str(pow(2, amount))], stdout=subprocess.PIPE)
                process.wait()
                output = process.communicate()[0]
                #print output
                timers = [m.start() for m in re.finditer('timer', output)]
                times = []
                for timer in timers:
                    afterTimer = output[timer+7:]
                    spacePos = afterTimer.find('\n')
                    times.append(int(afterTimer[:spacePos]))
                results.append(max(times))
                print max(times)
            median = numpy.median(results)
            print str(amount) + " " + str(median * 1000 / pow(2,amount))
            file1.write(str(amount) + " " + str(median * 1000 / pow(2,amount)) + "\n")
        file1.close()

