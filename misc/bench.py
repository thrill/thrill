#!/usr/bin/env python

import os
import sys
import subprocess
import random

amounts=[10,12,14,16,18,20]

workers = sys.argv[1]
modulo = sys.argv[2]


# Generate files using generator script and execute the sorter
with open(workers + "_" + modulo + "_true", "w+") as file1:
    for amount in amounts:
    	sum = 0
    	for _ in range(5):
       	    process = subprocess.Popen(["./../build/examples/bench", str(pow(2,amount)), workers, modulo], stdout=subprocess.PIPE)
            process.wait()
	    time = process.communicate()[0]
            sum += float(time)
	    print time
            avg = sum / 5
    	print str(amount) + " " + str(avg / pow(2,amount)) + "\n"
    	file1.write(str(amount) + " " + str(avg / pow(2,amount)) + "\n")
    file1.close()

with open(workers + "_" + modulo + "_false", "w+") as file2:
    for amount in amounts:
    	sum = 0
    	for _ in range(5):
       	    process = subprocess.Popen(["./../build/examples/bench_ref", str(pow(2,amount)), workers, modulo], stdout=subprocess.PIPE)
            process.wait()
	    time = process.communicate()[0]
            sum += float(time)
	    print time
            avg = sum / 5
    	print str(amount) + " " + str(avg / pow(2,amount)) + "\n"
    	file2.write(str(amount) + " " + str(avg / pow(2,amount)) + "\n")
    file2.close()

os.system("python graph_gen.py " + workers + "_" + modulo + " " + sys.argv[3])
os.remove(workers + "_" + modulo + "_true")
os.remove(workers + "_" + modulo + "_false")
