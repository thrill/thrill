#!/usr/bin/env python
##########################################################################
# benchmarks/chaining/evaluate.py
#
# Part of Project Thrill - http://project-thrill.org
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

from pylab import *
import sys, os, re, operator, numpy, itertools, json

filename = sys.argv[1]
output = sys.argv[2]

cache = []
collapse = []
chain = []
spark = []
flink = []

with open(filename) as f:
    selection = ""
    for line in f:
        dic = {}
        args = line.strip().split()
        if args[0].startswith("{"):
            dic = json.loads(line)
            if u'took' in dic:
                if selection == "CACHE":
                    cache.append(dic[u'took'])
                elif selection == "COLLAPSE":
                    collapse.append(dic[u'took'])
                elif selection == "CHAIN":
                    chain.append(dic[u'took'])
                elif selection == "SPARK":
                    spark.append(dic[u'took'])
                elif selection == "FLINK":
                    flink.append(dic[u'took'])
        if len(args) == 1 and args[0].startswith("CHAIN"):
            selection = "CHAIN"
        elif len(args) == 1 and args[0].startswith("COLLAPSE"):
            selection = "COLLAPSE"
        elif len(args) == 1 and args[0].startswith("CACHE"):
            selection = "CACHE"
        elif len(args) == 1 and args[0].startswith("SPARK"):
            selection = "SPARK"
        elif len(args) == 1 and args[0].startswith("FLINK"):
            selection = "FLINK"

f = open(output, 'w+')
f.write("CACHE:\t\t")
f.write(str((sum(cache))/len(cache) if len(cache) > 0 else float('nan')) + " microseconds\n")
f.write("COLLAPSE:\t")
f.write(str((sum(collapse))/len(collapse) if len(collapse) > 0 else float('nan')) + " microseconds\n")
f.write("CHAIN:\t\t")
f.write(str((sum(chain))/len(chain) if len(chain) > 0 else float('nan')) + " microseconds\n")
f.write("SPARK:\t\t")
f.write(str((sum(spark))/len(spark) if len(spark) > 0 else float('nan')) + " microseconds\n")
f.write("FLINK:\t\t")
f.write(str((sum(flink))/len(flink) if len(flink) > 0 else float('nan')) + " microseconds\n")
f.close()

##########################################################################
