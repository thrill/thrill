1#!/usr/bin/env python

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
    target = username + "@" + instance.public_ip_address + ":/home/" + username + "/page_rank_run"
    process = subprocess.Popen(["scp", "../../build/examples/page_rank/page_rank_run", target])
    process.wait()

for size in range(10, 20):
    size_pow = pow(2, size)
    for i in range(0, iterations):
        process = subprocess.Popen(["./invoke.sh", "-u", username, "-Q",
                                    "page_rank_run", "-g", "-j", str(size_pow)])

        process.wait()

        print "size: " + str(size) + " i: " + str(i) + " instances: " + str(num_instances)
