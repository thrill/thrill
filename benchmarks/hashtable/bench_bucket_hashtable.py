#!/usr/bin/env python
##########################################################################
# benchmarks/hashtable/bench_bucket_hashtable.py
#
# Part of Project Thrill.
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

def bench(workers, sizes, intervals, num_buckets_per_partitions, max_partition_fill_rates, max_num_blocks_tables, table_sizes, num_runs):
    for worker in workers:
        for size in sizes:
            for interval in intervals:
                for num_buckets_per_partition in num_buckets_per_partitions:
                    for max_partition_fill_rate in max_partition_fill_rates:
                        for max_num_blocks_table in max_num_blocks_tables:
                            for table_size in table_sizes:
                                with open(result_dir + "/" + "4" + "_" + str(worker) + "_" + str(size) + "_" + str(interval[0]) + "-" + str(interval[1]) + "_" + str(num_buckets_per_partition) + "_" + str(max_partition_fill_rate) + "_" + str(max_num_blocks_table) + "_" + str(table_size), "w+") as file1:
                                    times = []
                                    flushes = []
                                    items = []
                                    for _ in range(num_runs):
                                        process = subprocess.Popen(['../../build/benchmarks/hashtable_bench_bucket_hashtable', '-s', str(size), '-w', str(worker), '-l', str(interval[0]), '-u', str(interval[1]), '-b', str(num_buckets_per_partition), '-f', str(max_partition_fill_rate), '-n', str(max_num_blocks_table), '-t', str(table_size)], stdout=subprocess.PIPE)
                                        process.wait()
                                        out = process.communicate()[0]
                                        out_s = out.split()
                                        times.append(float(out_s[0]))
                                        flushes.append(float(out_s[1]))
                                        items.append(float(out_s[2]))
                                    time = numpy.median(times)
                                    flush = numpy.median(flushes)
                                    item = numpy.median(items)
                                    print str(worker) + "_" + str(interval[0]) + "-" + str(interval[1]) + "_" + str(num_buckets_per_partition) + "_" + str(max_partition_fill_rate) + "_" + str(max_num_blocks_table) + "_" + str(table_size) + ": " + str(time) + " " + str(flush) + " " + str(item) + " " + str(item * (pow(10,6)/time)) + " " + str(flush * (pow(10,6)/time))
                                    file1.write(str(interval[0]) + "-" + str(interval[1]) + " " + str(time) + " " + str(flush) + " " + str(item) + " " + str(item * (pow(10,6)/time)) + " " + str(flush * (pow(10,6)/time)) + "\n")
                                file1.close()

num_runs = 5
bench([100], [2000000000], [[5, 15]], [1], [0.5], [1], [500000000], num_runs);
#4 not applied
#6 not applied

##########################################################################
