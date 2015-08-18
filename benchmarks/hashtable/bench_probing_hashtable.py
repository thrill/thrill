#!/usr/bin/env python

import os
import sys
import subprocess
import random
import numpy

result_dir = "./bench_probing_hashtable"
if not os.path.exists(result_dir): os.makedirs(result_dir)

def bench(workers, num_unique_items, num_buckets_init_scales, num_buckets_resize_scales, max_partition_fill_ratios, max_num_items_tables, amounts, num_runs):
    for worker in workers:
        for modulo in modulae:
            for a in num_buckets_init_scales:
                for b in num_buckets_resize_scales:
                    for c in max_partition_fill_ratios:
                        for d in max_num_items_tables:
                            print "Testing with " + str(worker) + " workers and integers modulo " + str(modulo)
                            with open(result_dir + "/" + str(worker) + "_" + str(modulo) + "_" + str(a) + "_" + str(b) + "_" + str(c) + "_" + str(d), "w+") as file1:
                                for amount in amounts:
                                    results = []
                                    for _ in range(num_runs):
                                        process = subprocess.Popen(['../../build/benchmarks/hashtable_bench_probing_hashtable', '-s', str(pow(2,amount)), '-w', str(worker), '-m', str(modulo), '-i', str(a), '-r', str(b), '-f', str(c), '-t', str(d)], stdout=subprocess.PIPE)
                                        process.wait()
                                        time = process.communicate()[0]
                                        results.append(float(time))
                                        print time
                                    median = numpy.median(results)
                                    print str(amount) + " " + str(median * 1000 / pow(2,amount))
                                    file1.write(str(amount) + " " + str(median * 1000 / pow(2,amount)) + "\n")
                                file1.close()

num_runs = 2
amounts = [10,12,14,16,18,20,22,24,26,28]
bench([1000], [1000000], [1000], [2], [1.0], [1000000], amounts, num_runs);