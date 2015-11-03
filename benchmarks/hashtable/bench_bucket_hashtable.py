#!/usr/bin/env python
##########################################################################
# benchmarks/hashtable/bench_bucket_hashtable.py
#
# Part of Project Thrill - http://project-thrill.org
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import os
import sys
import subprocess
import random
import numpy

result_dir = "./bench_bucket_hashtable"
if not os.path.exists(result_dir): os.makedirs(result_dir)

def bench(workers, sizes, bucket_rates, max_partition_fill_rates, byte_sizes, num_runs):
    for worker in workers:
        for size in sizes:
            for bucket_rate in bucket_rates:
                for max_partition_fill_rate in max_partition_fill_rates:
                    for byte_size in byte_sizes:
                        with open(result_dir + "/" + str(worker) + "_" + str(size) + "_" + str(bucket_rate) + "_" + str(max_partition_fill_rate) + "_" + str(byte_size) + "_32_S", "w+") as file1:
                            times = []
                            #flushes = []
                            #collisions = []
                            spills = []
                            for _ in range(num_runs):
                                process = subprocess.Popen(['../../build/benchmarks/hashtable_bench_bucket_hashtable', '-s', str(size), '-w', str(worker), '-b', str(bucket_rate), '-f', str(max_partition_fill_rate), '-t', str(byte_size)], stdout=subprocess.PIPE)
                                process.wait()
                                out = process.communicate()[0]
                                out_s = out.split()
                                times.append(float(out_s[0]))
                                #flushes.append(float(out_s[1]))
                                #collisions.append(float(out_s[2]))
                                spills.append(float(out_s[1]))
                            time = numpy.median(times)
                            #flush = numpy.median(flushes)
                            #collision = numpy.median(collisions)
                            spill = numpy.median(spills)
                            #print str(worker) + "_" + str(size) + "_" + str(bucket_rate) + "_" + str(max_partition_fill_rate) + "_" + str(byte_size) + ": " + str(time) + " " + str(flush) + " " + str(collision)
                            print str(worker) + "_" + str(size) + "_" + str(bucket_rate) + "_" + str(max_partition_fill_rate) + "_" + str(byte_size) + ": " + str(time) + " " + str(spill)
                            file1.write(str(time) + " " + str(spill) + " " + "\n")
                        file1.close()


num_runs = 1
bench([100], [1000000000, 2000000000, 4000000000], [1.0], [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], [1000000000], num_runs)
