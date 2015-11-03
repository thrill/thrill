#!/usr/bin/env python
##########################################################################
# benchmarks/hashtable/bench_hashtable.py
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

num_runs = 5

##### bucket-based hashtable

result_dir = "./bench_bucket_hashtable"

workers = [10,100,1000]
num_buckets_init_scales = [1000,10000,100000]
num_buckets_resize_scales = [2,5,10]
max_num_items_per_buckets = [128,256,512]
max_num_items_tables = [1048576]

modulae = [1000,10000,100000]
amounts= [10,12,14,16,18,20,22,24,26,28]

if not os.path.exists(result_dir): os.makedirs(result_dir)

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
                                results = []
                                for _ in range(num_runs):
                                    process = subprocess.Popen(['../../build/benchmarks/bench_bucket_hashtable', '-s', str(pow(2,amount)), '-w', str(worker), '-m', str(modulo), '-i', str(a), '-r', str(b), '-b', str(c), '-t', str(d)], stdout=subprocess.PIPE)
                                    process.wait()
                                    time = process.communicate()[0]
                                    results.append(float(time))
                                    print time
                                median = numpy.median(results)
                                print str(amount) + " " + str(median * 1000 / pow(2,amount))
                                file1.write(str(amount) + " " + str(median * 1000 / pow(2,amount)) + "\n")
                            file1.close()


##### probing-based hashtable

result_dir = "./bench_probing_hashtable"

workers = [10,100,1000]
num_buckets_init_scales = [1000,10000,100000]
num_buckets_resize_scales = [2,5,10]
max_partition_fill_ratios = [1.0]
max_num_items_tables = [1048576]

modulae = [1000,10000,100000]
amounts= [10,12,14,16,18,20,22,24,26,28]

if not os.path.exists(result_dir): os.makedirs(result_dir)

# Insert pow(2,n) random integer elements into hashtable with specific number of workers and keyspace. Perform 5 times and print median to file.
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
                                    process = subprocess.Popen(['../../build/benchmarks/bench_probing_hashtable', '-s', str(pow(2,amount)), '-w', str(worker), '-m', str(modulo), '-i', str(a), '-r', str(b), '-f', str(c), '-t', str(d)], stdout=subprocess.PIPE)
                                    process.wait()
                                    time = process.communicate()[0]
                                    results.append(float(time))
                                    print time
                                median = numpy.median(results)
                                print str(amount) + " " + str(median * 1000 / pow(2,amount))
                                file1.write(str(amount) + " " + str(median * 1000 / pow(2,amount)) + "\n")
                            file1.close()

##########################################################################
