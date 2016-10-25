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

iterations = 5

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

username = "ubuntu"
num_instances = 0
for instance in instances:
    num_instances = num_instances + 1
    target = username + "@" + instance.public_ip_address + ":/home/" + username + "/wordcount_run"
    process = subprocess.Popen(["scp", "../../build/examples/wordcount/wordcount_run", target])
    process.wait()

for size in range(0, 8):
    size_pow = pow(2, size)
    path = "COMMONCRAWLPATH"
    for i in range(0, iterations):
        process = subprocess.Popen(["./invoke.sh", "-u", username, "-Q",
                                    "wordcount_run", path])

        process.wait()

        print "size: " + str(size) + " i: " + str(i) + " instances: " + str(num_instances)

##########################################################################
