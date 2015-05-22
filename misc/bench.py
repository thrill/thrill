#!/usr/bin/env python

import os
import sys
import subprocess
import random

amounts=[10,12,14,16,18,20,22,24,26,28]
#args = [[10,2,2,256,32,20],
#        [10,2,4,256,32,20],
#        [10,2,8,256,32,20],
#        [10,2,16,256,32,20],
#        [10,2,2,64,32,20],
#        [10,2,2,128,32,20],
#        [10,2,2,256,32,20],
#        [10,2,2,512,32,20],
#        [10,2,2,256,64,20],
#        [10,2,2,256,128,20],
#        [10,2,2,256,256,20],
#        [10,2,2,256,512,20]]

# Generate files using generator script and execute the sorter
#for idx in range(len(args)):
for amount in amounts:
    sum = 0
    for _ in range(5):
        process = subprocess.Popen(["./../build/examples/bench", str(pow(2,amount))], stdout=subprocess.PIPE)
        process.wait()
        sum += float(process.communicate()[0])
        avg = sum / 5
    print str(amount) + " " + str(avg * 1000 / pow(2,amount))
