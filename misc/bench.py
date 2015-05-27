#!/usr/bin/env python

import os
import sys
import subprocess
import random

amounts=[10,12,14,16,18,20,22,24,26,28]

workers = [1,10,100,1000]
modulae = [100,1000,10000,100000]


# Generate files using generator script and execute the sorter
for worker in workers:
    for modulo in modulae:
        print "Testing with " + str(worker) + " workers and integers modulo " + str(modulo)
        with open(str(worker) + "_" + str(modulo) + "_new", "w+") as file1:
            for amount in amounts:
                sum = 0
                for _ in range(3):
                    process = subprocess.Popen(["./../build/examples/bench", str(pow(2,amount)), str(worker), str(modulo)], stdout=subprocess.PIPE)
                    process.wait()
                    time = process.communicate()[0]
                    sum += float(time)
                    print time
                avg = sum / 3
                print str(amount) + " " + str(avg / pow(2,amount))
                file1.write(str(amount) + " " + str(avg / pow(2,amount)) + "\n")
            file1.close()

#        with open(str(worker) + "_" + str(modulo) + "_false", "w+") as file2:
#            for amount in amounts:
#                sum = 0
#                for _ in range(3):
#                    process = subprocess.Popen(["./../build/examples/bench_ref", str(pow(2,amount)), str(worker), str(modulo)], stdout=subprocess.PIPE)
#                    process.wait()
#                    time = process.communicate()[0]
#                    sum += float(time)
#                    print time
#                avg = sum / 3
#                print str(amount) + " " + str(avg / pow(2,amount))
#                file2.write(str(amount) + " " + str(avg / pow(2,amount)) + "\n")
#            file2.close()
