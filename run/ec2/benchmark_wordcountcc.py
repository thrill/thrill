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

iterations = 1

log_min_size = 1
log_max_size = 1

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

username = "ubuntu"
num_instances = 0
for instance in instances:
    num_instances = num_instances + 1
    target = username + "@" + instance.public_ip_address + ":/home/" + username + "/word_count_run"
    process = subprocess.Popen(["scp", "../../build/examples/word_count/word_count_run", target])
    process.wait()

for size in range(4, 5):
    size_pow = pow(2, size)
    path = ""
    for i in range(0, size_pow):
        path += getString(i)
    print path
    for i in range(0, iterations):
        process = subprocess.Popen(["./invoke.sh", "-u", username, "-Q", "word_count_run", path])

        process.wait()

        print "size: " + str(size) + " i: " + str(i) + " instances: " + str(num_instances)




##########################################################################
