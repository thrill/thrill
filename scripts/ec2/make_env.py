#!/usr/bin/env python
##########################################################################
# scripts/ec2/make_env.py
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import boto3
import sys
import os
import json
import sys

from subprocess import call

#if len(sys.argv) != 2:
#    print "Usage: make_env.py 123"
#    sys.exit();
#job_id = str(sys.argv[1])

ec2 = boto3.resource('ec2')

with open('config.json') as data_file:
    data = json.load(data_file)

instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                                          {'Name': 'key-name', 'Values': [data["EC2_KEY_HANDLE"]]},
                                          {'Name': 'tag:JobId', 'Values':[str(data["JOB_ID"])]}])

for instance in instances:
    sys.stderr.write("%s pub %s priv %s\n" % (instance.id, instance.public_ip_address, instance.private_ip_address))

pub_ips = [instance.public_ip_address for instance in instances]
priv_ips = [instance.private_ip_address for instance in instances]

sys.stderr.write("%d instances\n" % (len(pub_ips)))

print("export THRILL_HOSTLIST=\"" + (" ".join(priv_ips)) + "\"")
print("export THRILL_SSHLIST=\"" + (" ".join(pub_ips)) + "\"")
if data["EC2_ATTACH_VOLUME"]:
    print("export EC2_ATTACH_VOLUME=\"" + data["EC2_ATTACH_VOLUME"] + "\"")
print("# cluster ssh: cssh " + (" ".join(pub_ips)))

##########################################################################
