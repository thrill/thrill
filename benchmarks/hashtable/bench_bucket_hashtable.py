#!/usr/bin/env python

import os
import sys
import subprocess
import random
import numpy

result_dir = "./bench_bucket_hashtable"
if not os.path.exists(result_dir): os.makedirs(result_dir)

def bench(workers, num_unique_items, num_buckets_init_scales, num_buckets_resize_scales, max_num_items_per_buckets, max_num_items_tables, amounts, num_runs):
    for worker in workers:
        for modulo in num_unique_items:
            for a in num_buckets_init_scales:
                for b in num_buckets_resize_scales:
                    for c in max_num_items_per_buckets:
                        for d in max_num_items_tables:
                            print "Testing with " + str(worker) + " workers and integers modulo " + str(modulo)
                            with open(result_dir + "/" + str(worker) + "_" + str(num_unique_items) + "_" + str(a) + "_" + str(b) + "_" + str(c) + "_" + str(d), "w+") as file1:
                                for amount in amounts:
                                    times = []
                                    means = []
                                    medians = []
                                    stdevs = []
                                    for _ in range(num_runs):
                                        process = subprocess.Popen(['../../build/benchmarks/hashtable_bench_bucket_hashtable', '-s', str(pow(2,amount)), '-w', str(worker), '-m', str(modulo), '-i', str(a), '-r', str(b), '-b', str(c), '-t', str(d)], stdout=subprocess.PIPE)
                                        process.wait()
                                        out = process.communicate()[0]
                                        out_s = out.split();
                                        times.append(float(out_s[0]))
                                        means.append(float(out_s[1]))
                                        medians.append(float(out_s[2]))
                                        stdevs.append(float(out_s[3]))
                                    time = numpy.median(times)
                                    mean = numpy.median(means)
                                    median = numpy.median(medians)
                                    stdev = numpy.median(stdevs)
                                    print str(amount) + " " + str(time * 1000 / pow(2,amount)) + " " + str(mean / (modulo / worker)) + " " + str(median / (modulo / worker)) + " " + str(stdev / (modulo / worker))
                                    file1.write(str(amount) + " " + str(time * 1000 / pow(2,amount)) + " " + str(mean / modulo) + " " + str(median / modulo) + " " + str(stdev / modulo) + "\n")
                                file1.close()

num_runs = 2
amounts = [10,12,14,16,18,20,22,24,26,28]
bench([1000], [1000000], [1000], [2], [1000000], [1000000], amounts, num_runs);