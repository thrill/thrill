#!/usr/bin/env python

import os
import sys
import subprocess
import random

amounts=[10,12,14,16,18,20,22,24,26,28]

# Generate files using generator script and execute the sorter
for amount in amounts:
    sum = 0
    for _ in range(5):
        process = subprocess.Popen(["./../build/examples/bench_ref", str(pow(2,amount))], stdout=subprocess.PIPE)
        process.wait()
        sum += float(process.communicate()[0])
        avg = sum / 5
    print str(amount) + " " + str(avg * 1000 / pow(2,amount))
