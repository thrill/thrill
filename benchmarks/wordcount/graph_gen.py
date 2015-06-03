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

plottitle = "WordCount on 1-4 Workers"
for i in range(1,5):
    generate_graph(str(i) + "_workers", i)
title(plottitle)
grid(True)
minorticks_on()

legend(loc="upper right")
xlabel("2^x elements")
ylabel("micros per element")
print "generating plot"
savefig("wordcount.pdf")
