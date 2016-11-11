#!/usr/bin/env python
##########################################################################
# run/ec2/benchmark_wordcount.py
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import boto3
import json
import os
import sys
import subprocess


def getString(integer):
    return "s3://commoncrawl/" + lines[integer] + " "

iterations = 3

log_min_size = 2
log_max_size = 6

# check num of hosts:
ec2 = boto3.resource('ec2')

with open('config.json') as data_file:
    data = json.load(data_file)

filters = [{'Name': 'instance-state-name', 'Values': ['running']},
           {'Name':'key-name', 'Values': ['ec2_anoe']}]
if 'filters' in data:
    filters += data['filters']

instances = ec2.instances.filter(Filters=filters)

lines = [line.rstrip('\n') for line in open('../../../cc-paths')]

outputfile = open('wordcountresults', 'w+')

username = "ubuntu"
num_instances = 0
for instance in instances:
    num_instances = num_instances + 1
    target = username + "@" + instance.public_ip_address + ":/home/" + username + "/word_count_run_on"
    process = subprocess.Popen(["scp", "../../build/examples/word_count/word_count_run_on", target])
    process.wait()
    target = username + "@" + instance.public_ip_address + ":/home/" + username + "/word_count_run_off"
    process = subprocess.Popen(["scp", "../../build/examples/word_count/word_count_run_off", target])
    process.wait()

for size in range(log_min_size, log_max_size + 1):

    size_pow = pow(2, size)
    path = ""
    for i in range(0, size_pow):
        path += getString(i)

    for i in range(0, iterations):
        print "size: " + str(size) + " i: " + str(i) + " instances: " + str(num_instances)
        process = subprocess.Popen(["./invoke.sh", "-u", username, "-Q", "word_count_run_on", path], stdout=subprocess.PIPE)
        process.wait()
        output = process.stdout.readlines()

        for x in output:
            if x.find('RESULT') > 0 :
                print "Printing " + x
                outputfile.write(x[x.find('RESULT'):])
                outputfile.flush()


    for i in range(0, iterations):
        print "size: " + str(size) + " i: " + str(i) + " instances: " + str(num_instances)
        process = subprocess.Popen(["./invoke.sh", "-u", username, "-Q", "word_count_run_off", path], stdout=subprocess.PIPE)
        process.wait()
        output = process.stdout.readlines()

        for x in output:
            if x.find('RESULT') > 0 :
                print "Printing " + x
                outputfile.write(x[x.find('RESULT'):])
                outputfile.flush()






##########################################################################
