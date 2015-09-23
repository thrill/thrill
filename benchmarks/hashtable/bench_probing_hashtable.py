#!/usr/bin/env python

import os
import sys
import subprocess
import random
import numpy

result_dir = "./bench_probing_hashtable"
if not os.path.exists(result_dir): os.makedirs(result_dir)

def bench(workers, sizes, max_partition_fill_rates, byte_sizes, num_runs):
    for worker in workers:
        for size in sizes:
            for max_partition_fill_rate in max_partition_fill_rates:
                for byte_size in byte_sizes:
                    with open(result_dir + "/" + str(worker) + "_" + str(size) + "_" + str(max_partition_fill_rate) + "_" + str(byte_size), "w+") as file1:
                        times = []
                        flushes = []
                        collisions = []
                        for _ in range(num_runs):
                            process = subprocess.Popen(['../../build/benchmarks/hashtable_bench_probing_hashtable', '-s', str(size), '-w', str(worker), '-f', str(max_partition_fill_rate), '-t', str(byte_size)], stdout=subprocess.PIPE)
                            process.wait()
                            out = process.communicate()[0]
                            out_s = out.split()
                            times.append(float(out_s[0]))
                            flushes.append(float(out_s[1]))
                            collisions.append(float(out_s[2]))
                        time = numpy.median(times)
                        flush = numpy.median(flushes)
                        collision = numpy.median(collisions)
                        print str(worker) + "_" + str(size) + "_" + "_" + str(max_partition_fill_rate) + "_" + str(byte_size) + ": " + str(time) + " " + str(flush) + " " + str(collision)
                        file1.write(str(time) + " " + str(flush) + " " + str(collision) + "\n")
                    file1.close()

num_runs = 3
bench([100], [1000000000, 2000000000, 4000000000], [0.5, 0.6, 0.7, 0.8], [1000000000], num_runs)
