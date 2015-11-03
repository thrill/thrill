#!/usr/bin/env python
##########################################################################
# benchmarks/word_count/graph_gen.py
#
# Part of Project Thrill - http://project-thrill.org
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

from pylab import *
import sys, os, re, operator, numpy, itertools

def generate_graph(path, table):

    if not os.path.isfile(path):
        print "File not found!"
        sys.exit(0)

    #list of time-size tuples
    sizes = []
    times = []
    with open(path) as file:
        for line in file:
            args = line.strip().split()
            if len(args) == 2:
                sizes.append(int(args[0]))
                times.append(float(args[1]))
            else:
                print "Bad line!"
    if(table == 1):
        plot(sizes, times, color="green", label="1 Workers")
    if(table == 2):
        plot(sizes, times, color="red", label="2 Workers")
    if(table == 3):
        plot(sizes, times, color="blue", label="3 Workers")
    if(table == 4):
        plot(sizes, times, color="black", label="4 Workers")
    if(table == 8):
        plot(sizes, times, color="orange", label="8 Workers (2*4)")
    if(table == 16):
        plot(sizes, times, color="brown", label="16 Workers (4*4)")

plottitle = "WordCount on 1-16 Workers"
for i in range(1,5):
    generate_graph(str(i) + "_workers", i)
generate_graph("8_workers", 8)
generate_graph("16_workers", 16)
title(plottitle)
grid(True)
minorticks_on()

legend(loc="upper right")
xlabel("2^x elements")
ylabel("ns per element")
print "generating plot"
savefig("wordcount.pdf")

##########################################################################
