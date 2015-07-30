#!/usr/bin/env python

import os
import sys
import subprocess
import random
import numpy

result_dir = "./bench_bucket_hashtable"

amounts= [10,12,14] #,16,18,20,22,24,26,28]

workers = [10,100,1000]
modulae = [1000,100000]

num_buckets_init_scale = 10
num_buckets_resize_scale = 2
max_num_items_per_bucket = 256
max_num_items_table = 1048576

if not os.path.exists(result_dir): os.makedirs(result_dir)

# Insert pow(2,n) random integer elements into hashtable with specific number of workers and keyspace. Perform 5 times and print median to file.
for worker in workers:
    for modulo in modulae:
        print "Testing with " + str(worker) + " workers and integers modulo " + str(modulo)
        with open(result_dir + "/" + str(worker) + "_" + str(modulo) + "_true", "w+") as file1:
            for amount in amounts:
                results = []
                for _ in range(5):
                    process = subprocess.Popen(['../../build/benchmarks/bench_bucket_hashtable', '-s', str(pow(2,amount)), '-w', str(worker), '-m', str(modulo), '-i', str(num_buckets_init_scale), '-r', str(num_buckets_resize_scale), '-b', str(max_num_items_per_bucket), '-t', str(max_num_items_table)], stdout=subprocess.PIPE)
                    process.wait()
                    time = process.communicate()[0]
                    results.append(float(time))
                    print time
                median = numpy.median(results)
                print str(amount) + " " + str(median * 1000 / pow(2,amount))
                file1.write(str(amount) + " " + str(median * 1000 / pow(2,amount)) + "\n")
            file1.close()