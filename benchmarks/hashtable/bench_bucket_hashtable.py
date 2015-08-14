#!/usr/bin/env python

import os
import sys
import subprocess
import random
import numpy

result_dir = "./bench_bucket_hashtable"
if not os.path.exists(result_dir): os.makedirs(result_dir)

num_runs = 5

workers = [1000]
num_buckets_resize_scales = [2]
amounts= [10,12,14,16,18,20,22,24,26,28]

num_buckets_init_scales = [1000]
modulae = [10000000]
max_num_items_per_buckets = modulae
max_num_items_tables = modulae

# Insert pow(2,n) random integer elements into hashtable with specific number of workers and keyspace. Perform 5 times and print median to file.
for worker in workers:
    for modulo in modulae:
        for a in num_buckets_init_scales:
            for b in num_buckets_resize_scales:
                for c in max_num_items_per_buckets:
                    for d in max_num_items_tables:
                        print "Testing with " + str(worker) + " workers and integers modulo " + str(modulo)
                        with open(result_dir + "/" + str(worker) + "_" + str(modulo) + "_" + str(a) + "_" + str(b) + "_" + str(c) + "_" + str(d), "w+") as file1:
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
                                print str(amount) + " " + str(time * 1000 / pow(2,amount)) + " " + str(mean) + " " + str(median) + " " + str(stdev)
                                file1.write(str(amount) + " " + str(time * 1000 / pow(2,amount)) + " " + str(mean * 1000 / pow(2,amount)) + " " + str(median * 1000 / pow(2,amount)) + " " + str(stdev * 1000 / pow(2,amount)) + "\n")
                            file1.close()
