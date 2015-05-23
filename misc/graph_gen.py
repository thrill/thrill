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
    if(table == 0):
        plot(sizes, times, color="green", label="64")
    if(table == 1):
        plot(sizes, times, color="red", label="128")
    if(table == 2):
        plot(sizes, times, color="blue", label="256")
    if(table == 3):
        plot(sizes, times, color="yellow", label="512")


#paths = sys.argv[1]
#plottitle = sys.argv[2]
generate_graph("2", 0)
generate_graph("4", 1)
generate_graph("8", 2)
generate_graph("16",3)
title("Maximum number of items per block")
grid(True)
minorticks_on()

legend(loc="lower right")
xlabel("2^x elements")
ylabel("micros per element")
print "generating plot"
savefig("blocksize.pdf")
