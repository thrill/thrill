#!/usr/bin/env python

import os
import sys
import subprocess
import random

amounts=[10,12,14,16,18,20,22,24,26,28]

workers = sys.argv[1]
modulo = sys.argv[2]


# Generate files using generator script and execute the sorter
with open(workers + "_" + modulo + "_true", "w+") as file1:
    for amount in amounts:
    	sum = 0
    	for _ in range(5):
       	    process = subprocess.Popen(["./../build/examples/bench", str(pow(2,amount))], stdout=subprocess.PIPE)
            process.wait()
	    time = process.communicate()[0]
            sum += float(time)
	    print time
            avg = sum / 5
    	print str(amount) + " " + str(avg / pow(2,amount))
    	file1.write(str(amount) + " " + str(avg / pow(2,amount)))
    file1.close()
